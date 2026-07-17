import Redis from 'ioredis';
import { createApp } from './api/app';
import { RateLimiter } from './ratelimiter/limiter';
import { ClientConfigStore } from './config/clientStore';
import { LogQueue } from './logging/queue';
import { createPool, runMigrations } from './storage/postgres';
import { startRollupTimer } from './logging/rollup';

async function main() {
  const pool = createPool();
  await runMigrations(pool);

  const redis = new Redis({
    host: process.env.REDIS_HOST ?? 'localhost',
    port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
    maxRetriesPerRequest: 1, // fail fast -> let the circuit breaker take over
    retryStrategy: (times) => Math.min(times * 200, 2000),
  });
  redis.on('error', (err) => console.error('[redis] connection error', err.message));

  const limiter = new RateLimiter(redis, {
    redisTimeoutMs: 20,
    breakerFailureThreshold: 3,
    breakerCooldownMs: 2000,
  });
  limiter.startFallbackSweeper();

  const clientStore = new ClientConfigStore(pool);
  await clientStore.refresh();
  clientStore.startAutoRefresh(5000);

  const logQueue = new LogQueue(pool, { batchSize: 500, flushIntervalMs: 1000 });
  logQueue.startFlusher();

  startRollupTimer(pool, 30_000);

  const app = createApp(limiter, clientStore, logQueue, pool);
  const port = parseInt(process.env.PORT ?? '3000', 10);
  app.listen(port, () => console.log(`rate-limiter listening on :${port}`));

  process.on('SIGTERM', async () => {
    await logQueue.flush();
    await pool.end();
    redis.disconnect();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('fatal startup error', err);
  process.exit(1);
});
