import Redis from 'ioredis';
import { Pool } from 'pg';
import { LogStreamProducer } from '../src/logging/streamProducer';
import { StreamConsumer } from '../src/logging/streamConsumer';

const REDIS_HOST = process.env.REDIS_HOST ?? 'localhost';
const REDIS_PORT = parseInt(process.env.REDIS_PORT ?? '6379', 10);

function testPool(): Pool {
  return new Pool({
    host: process.env.PGHOST ?? 'localhost',
    port: parseInt(process.env.PGPORT ?? '5432', 10),
    user: process.env.PGUSER ?? 'ratelimiter',
    password: process.env.PGPASSWORD ?? 'ratelimiter',
    database: process.env.PGDATABASE ?? 'ratelimiter',
  });
}

describe('Redis Streams logging pipeline', () => {
  let redis: Redis;
  let pool: Pool;
  const streamKey = 'stream:test_request_log';
  const groupName = 'test_log_consumers';

  beforeAll(async () => {
    redis = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    pool = testPool();
    await pool.query(
      `INSERT INTO clients (client_id, name, capacity, refill_rate_per_sec)
       VALUES ('stream-test-client', 'Stream Test Client', 100, 1.667)
       ON CONFLICT (client_id) DO NOTHING`
    );
  });

  afterAll(async () => {
    await pool.query(`DELETE FROM request_log WHERE client_id = 'stream-test-client'`);
    await pool.query(`DELETE FROM clients WHERE client_id = 'stream-test-client'`);
    await redis.del(streamKey);
    await redis.quit();
    await pool.end();
  });

  beforeEach(async () => {
    await redis.del(streamKey);
    await pool.query(`DELETE FROM request_log WHERE client_id = 'stream-test-client'`);
  });

  it('publishes survive being read by a consumer group and land correctly in Postgres', async () => {
    const producer = new LogStreamProducer(redis, streamKey);
    const consumer = new StreamConsumer(redis, pool, { streamKey, groupName, consumerName: 'consumer-1' });
    await consumer.ensureGroup();

    for (let i = 0; i < 10; i++) {
      producer.publish({
        clientId: 'stream-test-client',
        allowed: i < 8,
        latencyMs: 1.5 + i,
        source: 'redis',
        occurredAt: new Date(),
      });
    }

    // publish() is fire-and-forget; give the XADDs a moment to land
    await new Promise((r) => setTimeout(r, 200));

    const processed = await consumer.runOnce();
    expect(processed).toBe(10);

    const { rows } = await pool.query(
      `SELECT allowed, latency_ms FROM request_log WHERE client_id = 'stream-test-client' ORDER BY latency_ms ASC`
    );
    expect(rows).toHaveLength(10);
    expect(rows.filter((r) => r.allowed).length).toBe(8);

    // and the group has nothing left pending -- everything was acked
    const pending = await (redis as any).xpending(streamKey, groupName);
    expect(pending[0]).toBe(0);
  });

  it('recovers entries from a consumer that read but never acked (simulated crash)', async () => {
    const producer = new LogStreamProducer(redis, streamKey);
    const crashedConsumer = new StreamConsumer(redis, pool, {
      streamKey,
      groupName,
      consumerName: 'crashed-consumer',
      claimIdleMs: 50, // short, so the test doesn't have to wait long
    });
    await crashedConsumer.ensureGroup();

    producer.publish({
      clientId: 'stream-test-client',
      allowed: true,
      latencyMs: 2.1,
      source: 'redis',
      occurredAt: new Date(),
    });
    await new Promise((r) => setTimeout(r, 150));

    // Simulate a crash: read via XREADGROUP directly (claiming the
    // entry for "crashed-consumer") but never insert-and-ack it -- this
    // is exactly the failure mode the in-memory queue couldn't survive.
    await (redis as any).xreadgroup(
      'GROUP', groupName, 'crashed-consumer',
      'COUNT', 10, 'STREAMS', streamKey, '>'
    );

    // confirm it's genuinely stuck pending, assigned to the dead consumer
    const pendingBefore = await (redis as any).xpending(streamKey, groupName);
    expect(pendingBefore[0]).toBe(1);

    // wait past claimIdleMs so a healthy consumer is allowed to reclaim it
    await new Promise((r) => setTimeout(r, 100));

    const healthyConsumer = new StreamConsumer(redis, pool, {
      streamKey,
      groupName,
      consumerName: 'healthy-consumer',
      claimIdleMs: 50,
    });
    const reclaimed = await healthyConsumer.reclaimStale();
    expect(reclaimed).toBe(1);

    const { rows } = await pool.query(
      `SELECT allowed FROM request_log WHERE client_id = 'stream-test-client'`
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].allowed).toBe(true);

    const pendingAfter = await (redis as any).xpending(streamKey, groupName);
    expect(pendingAfter[0]).toBe(0);
  });

  it('does not ack entries if the Postgres insert fails, so they remain retryable', async () => {
    const producer = new LogStreamProducer(redis, streamKey);
    const consumer = new StreamConsumer(redis, pool, { streamKey, groupName, consumerName: 'consumer-fail-test' });
    await consumer.ensureGroup();

    // an unknown clientId violates the FK constraint on request_log,
    // so the batch insert will throw
    producer.publish({
      clientId: 'nonexistent-client-xyz',
      allowed: true,
      latencyMs: 1,
      source: 'redis',
      occurredAt: new Date(),
    });
    await new Promise((r) => setTimeout(r, 150));

    await expect(consumer.runOnce()).rejects.toThrow();

    // it must still be pending -- not silently dropped
    const pending = await (redis as any).xpending(streamKey, groupName);
    expect(pending[0]).toBe(1);
  });
});
