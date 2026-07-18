import express, { Express } from 'express';
import path from 'path';
import { Pool } from 'pg';
import { RateLimiter } from '../ratelimiter/limiter';
import { ClientConfigStore } from '../config/clientStore';
import { LogStreamProducer } from '../logging/streamProducer';

export function createApp(
  limiter: RateLimiter,
  clientStore: ClientConfigStore,
  logStream: LogStreamProducer,
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
      logStream: logStream.stats(),
    });
  });

  // --- auth middleware ----------------------------------------------
  const authMiddleware = (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const key = process.env.INTERNAL_API_KEY;
    if (!key) return next(); // fail open if not configured
    if (req.headers.authorization !== `Bearer ${key}`) {
      return res.status(401).json({ error: 'unauthorized' });
    }
    next();
  };

  // --- the hot path ---------------------------------------------------
  // POST /v1/check { clientId, cost? }
  app.post('/v1/check', authMiddleware, async (req, res) => {
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
    logStream.publish({
      clientId,
      allowed: result.allowed,
      latencyMs,
      source: result.source,
      occurredAt: new Date(),
    });

    // Standard rate-limit headers (the convention GitHub/Stripe/AWS all
    // follow) so callers -- and any gateway/CDN in front of them -- can
    // act automatically instead of parsing the JSON body. Token bucket
    // has no single fixed "reset time" the way fixed-window does, so
    // Retry-After expresses "seconds until enough tokens exist for the
    // next request" rather than a window boundary.
    res.set('RateLimit-Limit', String(cfg.capacity));
    res.set('RateLimit-Remaining', String(Math.max(0, Math.floor(result.remaining))));
    if (!result.allowed) {
      res.set('Retry-After', String(Math.ceil(result.retryAfterMs / 1000)));
    }

    res.status(result.allowed ? 200 : 429).json({
      allowed: result.allowed,
      remaining: result.remaining,
      retryAfterMs: result.retryAfterMs,
      source: result.source,
      checkLatencyMs: Number(latencyMs.toFixed(3)),
    });
  });

  // --- client admin -----------------------------------------------------
  app.get('/v1/clients', authMiddleware, (_req, res) => {
    res.json(clientStore.all());
  });

  // Tiers let many clients share one limit definition instead of every
  // client needing its own hand-set capacity/rate -- e.g. "free" vs
  // "premium" -- while a client can still override with bespoke numbers.
  app.get('/v1/tiers', authMiddleware, async (_req, res) => {
    res.json(await clientStore.listTiers());
  });

  app.put('/v1/clients/:clientId', authMiddleware, async (req, res) => {
    const { clientId } = req.params;
    const { name, tierId, capacity, refillRatePerSec } = req.body ?? {};
    if (!name) {
      return res.status(400).json({ error: 'name is required' });
    }
    if (!tierId && (!capacity || !refillRatePerSec)) {
      return res.status(400).json({
        error: 'provide either tierId, or both capacity and refillRatePerSec',
      });
    }
    try {
      await clientStore.upsert({ 
        clientId: clientId as string, 
        name: name as string, 
        tierId: tierId as string | null | undefined, 
        capacity: capacity as number | null | undefined, 
        refillRatePerSec: refillRatePerSec as number | null | undefined 
      });
    } catch (err: any) {
      return res.status(400).json({ error: err.message });
    }
    res.status(204).send();
  });

  // --- admin: trigger chaos ----------------------------------------
  app.post('/v1/admin/trip-breaker', authMiddleware, (_req, res) => {
    // Force the breaker open for 10 seconds to test fallback path
    limiter.tripBreaker(10000);
    res.json({ status: 'breaker tripped for 10s' });
  });

  // --- dashboard: usage with filters -------------------------------
  // GET /v1/usage/:clientId?days=3|10|15|30
  app.get('/v1/usage/:clientId', authMiddleware, async (req, res) => {
    const { clientId } = req.params;
    const days = [3, 10, 15, 30].includes(Number(req.query.days)) ? Number(req.query.days) : 10;

    let rows;
    if (clientId === 'all') {
      const result = await pool.query(
        `SELECT day, 
                SUM(total_requests) as total_requests, 
                SUM(allowed_count) as allowed_count, 
                SUM(denied_count) as denied_count, 
                AVG(avg_latency_ms) as avg_latency_ms
         FROM usage_daily
         WHERE day >= (now() - ($1 || ' days')::interval)::date
         GROUP BY day
         ORDER BY day ASC`,
        [days]
      );
      rows = result.rows;
    } else {
      const result = await pool.query(
        `SELECT day, total_requests, allowed_count, denied_count, avg_latency_ms
         FROM usage_daily
         WHERE client_id = $1 AND day >= (now() - ($2 || ' days')::interval)::date
         ORDER BY day ASC`,
        [clientId, days]
      );
      rows = result.rows;
    }

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
