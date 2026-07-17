import Redis from 'ioredis';
import { RateLimiter } from '../src/ratelimiter/limiter';
import { LimitConfig } from '../src/ratelimiter/types';

const REDIS_HOST = process.env.REDIS_HOST ?? 'localhost';
const REDIS_PORT = parseInt(process.env.REDIS_PORT ?? '6379', 10);

describe('Race conditions across a simulated cluster', () => {
  let redis: Redis;

  beforeAll(async () => {
    redis = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    await redis.flushdb();
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('never admits more than `capacity` requests when N nodes fire concurrently', async () => {
    // Requirement: "Rate limit checks must be accurate regardless of
    // which service instance receives the request." We simulate a
    // cluster of 8 independent rate-limiter instances (each its own
    // ioredis connection, its own circuit breaker) all racing to
    // consume from the SAME client's bucket at the same instant.
    const NODE_COUNT = 8;
    const CONCURRENT_REQUESTS = 300;
    const CAPACITY = 50;

    const cfg: LimitConfig = { clientId: 'race-client', capacity: CAPACITY, refillRatePerSec: 0.0001 };
    await redis.flushdb();

    const nodes: RateLimiter[] = [];
    const connections: Redis[] = [];
    for (let i = 0; i < NODE_COUNT; i++) {
      const conn = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
      connections.push(conn);
      // Generous timeout on purpose: this test asks "does the bucket
      // stay correct under concurrent access", not "how fast is a
      // single check" -- that's covered separately in
      // limiter.redis.test.ts's p50/p99 assertions. A resource-shared
      // sandbox running 300 simultaneous EVALSHA calls plus Node's own
      // event loop can have real tail latency that has nothing to do
      // with the atomicity guarantee we're verifying here.
      nodes.push(new RateLimiter(conn, { redisTimeoutMs: 2000 }));
    }

    // Warm up: establish each connection and force the Lua script to be
    // loaded before the timed burst, the same way a real instance
    // finishes connecting to Redis before it's added to the load
    // balancer. Without this, cold TCP handshakes + first-time SCRIPT
    // LOAD race against the 20ms per-call budget and legitimately trip
    // the breaker into fallback mode -- correct fail-safe behavior, but
    // not what this test is trying to isolate.
    await Promise.all(
      nodes.map((n, i) =>
        n.check({ clientId: `warmup-${i}`, capacity: 1, refillRatePerSec: 1 })
      )
    );
    await redis.flushdb(); // clear the warmup keys, keep race-client's bucket untouched (it doesn't exist yet)

    // Fire all requests essentially simultaneously, round-robining
    // across the simulated cluster nodes.
    const promises = Array.from({ length: CONCURRENT_REQUESTS }, (_, i) => {
      const node = nodes[i % NODE_COUNT];
      return node.check(cfg);
    });

    const results = await Promise.all(promises);
    const allowedCount = results.filter((r) => r.allowed).length;

    await Promise.all(connections.map((c) => c.quit()));

    // With near-zero refill during the test window, exactly CAPACITY
    // requests should be admitted -- no more, regardless of how many
    // nodes raced for the same bucket. Redis's atomic Lua execution is
    // what makes this deterministic instead of a race.
    expect(allowedCount).toBe(CAPACITY);
    expect(results.length - allowedCount).toBe(CONCURRENT_REQUESTS - CAPACITY);
  }, 20000);

  it('local fallback buckets are also race-safe within a single process', async () => {
    // The fallback limiter is single-threaded JS (no real parallel
    // mutation), but this guards against accidental async interleaving
    // bugs (e.g. an await between read-modify-write of the bucket).
    const { FallbackLimiter } = await import('../src/ratelimiter/fallback');
    const fb = new FallbackLimiter();
    const cfg: LimitConfig = { clientId: 'fb-race', capacity: 20, refillRatePerSec: 0.0001 };

    const results = await Promise.all(
      Array.from({ length: 100 }, () => Promise.resolve(fb.check('fb-race', cfg, 1)))
    );
    const allowed = results.filter((r) => r.allowed).length;
    expect(allowed).toBe(20);
  });
});
