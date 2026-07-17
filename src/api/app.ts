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
  pool: Pool,
  redisPublisher: import('ioredis').Redis
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
  app.get('/v1/clients', authMiddleware, (_req, res) => {
    res.json(clientStore.all());
  });

  app.put('/v1/clients/:clientId', authMiddleware, async (req, res) => {
    const clientId = req.params.clientId as string;
    const { name, capacity, refillRatePerSec } = req.body ?? {};
    if (!name || !capacity || !refillRatePerSec) {
      return res.status(400).json({ error: 'name, capacity, refillRatePerSec are required' });
    }
    await clientStore.upsert({ clientId, capacity, refillRatePerSec }, name);
    redisPublisher.publish('config:refresh', clientId).catch(err => console.error('[api] config publish failed', err));
    res.status(204).send();
  });

  app.post('/v1/admin/trip-breaker', authMiddleware, (req, res) => {
    limiter.simulateOutage(10000);
    res.json({ status: 'breaker_tripped', durationMs: 10000 });
  });

  app.get('/v1/logs', authMiddleware, async (req, res) => {
    try {
      const page = Math.max(1, Number(req.query.page) || 1);
      const limit = Math.max(1, Math.min(100, Number(req.query.limit) || 10));
      const offset = (page - 1) * limit;

      const clientIdParam = req.query.clientId as string;
      const statusParam = req.query.status as string;

      let whereClauses = [];
      let params = [];
      
      if (clientIdParam && clientIdParam !== 'all') {
        params.push(clientIdParam);
        whereClauses.push(`client_id = $${params.length}`);
      }
      if (statusParam === 'allowed') {
        whereClauses.push(`allowed = true`);
      } else if (statusParam === 'denied') {
        whereClauses.push(`allowed = false`);
      }

      const whereSql = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';

      const countResult = await pool.query(`SELECT COUNT(*) FROM request_log ${whereSql}`, params);
      const total = parseInt(countResult.rows[0].count, 10);

      const logsParams = [...params, limit, offset];
      const limitParamIdx = params.length + 1;
      const offsetParamIdx = params.length + 2;

      const { rows } = await pool.query(
        `SELECT id, client_id, allowed, latency_ms, source, occurred_at 
         FROM request_log 
         ${whereSql}
         ORDER BY occurred_at DESC 
         LIMIT $${limitParamIdx} OFFSET $${offsetParamIdx}`,
         logsParams
      );
      
      res.json({
        data: rows,
        meta: {
          total,
          page,
          limit,
          totalPages: Math.ceil(total / limit)
        }
      });
    } catch (err) {
      console.error('[api] GET /v1/logs failed', err);
      res.status(500).json({ error: 'internal server error' });
    }
  });

  // --- dashboard: usage with filters -------------------------------
  app.get('/v1/usage/top-denied', async (req, res) => {
    const range = req.query.range as string || '60m';
    let intervalStr = '60 minutes';
    if (range === '24h') intervalStr = '24 hours';
    else if (range === '7d') intervalStr = '7 days';

    try {
      const { rows } = await pool.query(
        `SELECT client_id, COUNT(*) as denied_count
         FROM request_log
         WHERE allowed = false AND occurred_at >= (now() - $1::interval)
         GROUP BY client_id
         ORDER BY denied_count DESC
         LIMIT 5`,
        [intervalStr]
      );
      res.json(rows.map(r => ({ clientId: r.client_id, deniedCount: Number(r.denied_count) })));
    } catch (err) {
      console.error('[api] GET /v1/usage/top-denied failed', err);
      res.status(500).json({ error: 'internal server error' });
    }
  });

  // GET /v1/usage/:clientId?range=60m|24h|7d
  app.get('/v1/usage/:clientId', async (req, res) => {
    const clientId = req.params.clientId as string;
    const range = req.query.range as string || '60m';
    
    let intervalStr = '60 minutes';
    let truncUnit = 'minute';
    if (range === '24h') {
      intervalStr = '24 hours';
      truncUnit = 'hour';
    } else if (range === '7d') {
      intervalStr = '7 days';
      truncUnit = 'day';
    }

    try {
      let rows;
      if (clientId === 'all') {
        const result = await pool.query(
          `SELECT 
             date_trunc($1, occurred_at) as period, 
             COUNT(*) as total_requests,
             COUNT(CASE WHEN allowed = true THEN 1 END) as allowed_count,
             COUNT(CASE WHEN allowed = false THEN 1 END) as denied_count,
             PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95_latency_ms
           FROM request_log
           WHERE occurred_at >= (now() - $2::interval)
           GROUP BY date_trunc($1, occurred_at)
           ORDER BY date_trunc($1, occurred_at) ASC`,
          [truncUnit, intervalStr]
        );
        rows = result.rows;
      } else {
        const result = await pool.query(
          `SELECT 
             date_trunc($1, occurred_at) as period, 
             COUNT(*) as total_requests,
             COUNT(CASE WHEN allowed = true THEN 1 END) as allowed_count,
             COUNT(CASE WHEN allowed = false THEN 1 END) as denied_count,
             PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95_latency_ms
           FROM request_log
           WHERE client_id = $3 AND occurred_at >= (now() - $2::interval)
           GROUP BY date_trunc($1, occurred_at)
           ORDER BY date_trunc($1, occurred_at) ASC`,
          [truncUnit, intervalStr, clientId]
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
        range,
        totals,
        timeline: rows.map(r => ({
          period: r.period,
          totalRequests: Number(r.total_requests),
          allowed: Number(r.allowed_count),
          denied: Number(r.denied_count),
          p95LatencyMs: r.p95_latency_ms ? Number(r.p95_latency_ms).toFixed(2) : 0
        }))
      });
    } catch (err) {
      console.error('[api] GET /v1/usage failed', err);
      res.status(500).json({ error: 'internal server error' });
    }
  });

  // Daily cleanup of request logs older than 30 days
  setInterval(async () => {
    try {
      await pool.query(`DELETE FROM request_log WHERE occurred_at < NOW() - INTERVAL '30 days'`);
    } catch (err) {
      console.error('[api] cleanup job failed', err);
    }
  }, 1000 * 60 * 60 * 24);

  return app;
}
