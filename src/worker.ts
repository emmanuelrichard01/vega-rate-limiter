import Redis from 'ioredis';
import { StreamConsumer } from './logging/streamConsumer';
import { createPool, runMigrations } from './storage/postgres';
import { startRollupTimer } from './logging/rollup';

async function main() {
  const pool = createPool();
  await runMigrations(pool); // idempotent; safe even if the API replicas already ran it

  const redis = new Redis({
    host: process.env.REDIS_HOST ?? 'localhost',
    port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
    maxRetriesPerRequest: null, // the worker should keep retrying rather than give up on a blocking read
    retryStrategy: (times) => Math.min(times * 200, 2000),
  });
  redis.on('error', (err) => console.error('[worker/redis] connection error', err.message));

  const consumer = new StreamConsumer(redis, pool, {
    consumerName: process.env.WORKER_ID ?? undefined,
  });

  // The rollup that keeps usage_daily fresh for the dashboard doesn't
  // need to live in every API replica; running it once here avoids
  // redundant work (still safe if it also runs elsewhere -- it's an
  // idempotent upsert).
  startRollupTimer(pool, 30_000);

  console.log(`[worker] starting as consumer "${consumer.getConsumerName()}"`);
  await consumer.start();
}

process.on('SIGTERM', () => {
  console.log('[worker] received SIGTERM, exiting');
  process.exit(0);
});

main().catch((err) => {
  console.error('[worker] fatal error', err);
  process.exit(1);
});
