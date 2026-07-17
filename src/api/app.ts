import express, { Express } from 'express';
import path from 'path';
import { Pool } from 'pg';
import { RateLimiter } from '../ratelimiter/limiter';
import { ClientConfigStore } from '../config/clientStore';
import { LogQueue } from '../logging/queue';

export function createApp(
  limiter: RateLimiter,
  clientStore: ClientConfigStore,
  logQueue: LogQueue,
  pool: Pool
): Express {
  const app = express();
  app.use(express.json());
  app.use('/dashboard', express.static(path.join(__dirname, '..', '..', 'web', 'dashboard')));

  // --- health -------------------------------------------------------
  app.get('/healthz', (_req, res) => {
    res.json({
      status: 'ok',
      breaker: limiter.getBreakerState(),
      logQueue: logQueue.stats(),
    });
  });

  // --- the hot path ---------------------------------------------------
  // POST /v1/check { clientId, cost? }
  app.post('/v1/check', async (req, res) => {
    const start = process.hrtime.bigint();
    const { clientId, cost } = req.body ?? {};

    if (!clientId || typeof clientId !== 'string') {
      return res.status(400).json({ error: 'clientId is required' });
    }

    const cfg = clientStore.get(clientId);
    if (!cfg) {
      return res.status(404).json({ error: `unknown clientId: ${clientId}` });
    }

    const result = await limiter.check(cfg, cost ?? 1);
    const latencyMs = Number(process.hrtime.bigint() - start) / 1e6;

    // Fire-and-forget: never await the log write on the response path.
    logQueue.enqueue({
      clientId,
      allowed: result.allowed,
      latencyMs,
      source: result.source,
      occurredAt: new Date(),
    });

    res.status(result.allowed ? 200 : 429).json({
      allowed: result.allowed,
      remaining: result.remaining,
      retryAfterMs: result.retryAfterMs,
      source: result.source,
      checkLatencyMs: Number(latencyMs.toFixed(3)),
    });
  });

  // --- client admin -----------------------------------------------------
  app.get('/v1/clients', (_req, res) => {
    res.json(clientStore.all());
  });

  app.put('/v1/clients/:clientId', async (req, res) => {
    const { clientId } = req.params;
    const { name, capacity, refillRatePerSec } = req.body ?? {};
    if (!name || !capacity || !refillRatePerSec) {
      return res.status(400).json({ error: 'name, capacity, refillRatePerSec are required' });
    }
    await clientStore.upsert({ clientId, capacity, refillRatePerSec }, name);
    res.status(204).send();
  });

  // --- dashboard: usage with filters -------------------------------
  // GET /v1/usage/:clientId?days=10|15|30
  app.get('/v1/usage/:clientId', async (req, res) => {
    const { clientId } = req.params;
    const days = [10, 15, 30].includes(Number(req.query.days)) ? Number(req.query.days) : 10;

    const { rows } = await pool.query(
      `SELECT day, total_requests, allowed_count, denied_count, avg_latency_ms
       FROM usage_daily
       WHERE client_id = $1 AND day >= (now() - ($2 || ' days')::interval)::date
       ORDER BY day ASC`,
      [clientId, days]
    );

    const totals = rows.reduce(
      (acc, r) => {
        acc.totalRequests += Number(r.total_requests);
        acc.allowed += Number(r.allowed_count);
        acc.denied += Number(r.denied_count);
        return acc;
      },
      { totalRequests: 0, allowed: 0, denied: 0 }
    );

    res.json({
      clientId,
      rangeDays: days,
      totals,
      dailyTrend: rows.map((r) => ({
        day: r.day,
        totalRequests: Number(r.total_requests),
        allowed: Number(r.allowed_count),
        denied: Number(r.denied_count),
        avgLatencyMs: Number(r.avg_latency_ms),
      })),
    });
  });

  return app;
}
