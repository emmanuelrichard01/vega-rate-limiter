import Redis from 'ioredis';
import { createApp } from './api/app';
import { RateLimiter } from './ratelimiter/limiter';
import { ClientConfigStore } from './config/clientStore';
import { LogStreamProducer } from './logging/streamProducer';
import { createPool, runMigrations } from './storage/postgres';

async function main() {
  const pool = createPool();
  await runMigrations(pool);

  if (!process.env.SERVICE_API_KEY || !process.env.ADMIN_API_KEY) {
    console.warn('⚠️  SERVICE_API_KEY or ADMIN_API_KEY not set — auth is effectively disabled or degraded');
  }

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

  const clientStore = new ClientConfigStore(pool, redis);

  // Publishes onto stream:request_log; a separate `worker` process (see
  // src/worker.ts) consumes it into Postgres. The API replicas don't
  // touch Postgres for logging at all anymore -- they only ever talk to
  // Redis on the hot path, and the durable hand-off to Postgres is
  // fully decoupled into its own service/failure domain.
  const logStream = new LogStreamProducer(redis);

  const app = createApp(limiter, clientStore, logStream, pool);
  const port = parseInt(process.env.PORT ?? '3000', 10);
  app.listen(port, () => console.log(`rate-limiter listening on :${port}`));

  process.on('SIGTERM', async () => {
    clientStore.close();
    await pool.end();
    redis.disconnect();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('fatal startup error', err);
  process.exit(1);
});
