import Redis from 'ioredis';
import { RateLimiter } from '../src/ratelimiter/limiter';
import { LimitConfig } from '../src/ratelimiter/types';

const REDIS_HOST = process.env.REDIS_HOST ?? 'localhost';
const REDIS_PORT = parseInt(process.env.REDIS_PORT ?? '6379', 10);

describe('RateLimiter against real Redis', () => {
  let redis: Redis;

  beforeAll(async () => {
    redis = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    await redis.flushdb();
  });

  afterAll(async () => {
    await redis.quit();
  });

  afterEach(async () => {
    await redis.flushdb();
  });

  it('allows a burst up to capacity, then denies', async () => {
    const limiter = new RateLimiter(redis);
    const cfg: LimitConfig = { clientId: 'burst-test', capacity: 5, refillRatePerSec: 1 };

    for (let i = 0; i < 5; i++) {
      const r = await limiter.check(cfg);
      expect(r.allowed).toBe(true);
      expect(r.source).toBe('redis');
    }
    const denied = await limiter.check(cfg);
    expect(denied.allowed).toBe(false);
    expect(denied.retryAfterMs).toBeGreaterThan(0);
  });

  it('refills at the configured rate', async () => {
    const limiter = new RateLimiter(redis);
    const cfg: LimitConfig = { clientId: 'refill-test', capacity: 2, refillRatePerSec: 10 };

    await limiter.check(cfg);
    await limiter.check(cfg);
    expect((await limiter.check(cfg)).allowed).toBe(false);

    await new Promise((r) => setTimeout(r, 150)); // 1.5 tokens at 10/s
    expect((await limiter.check(cfg)).allowed).toBe(true);
  });

  it('keeps client buckets fully independent', async () => {
    const limiter = new RateLimiter(redis);
    const cfgA: LimitConfig = { clientId: 'client-a-iso', capacity: 2, refillRatePerSec: 1 };
    const cfgB: LimitConfig = { clientId: 'client-b-iso', capacity: 2, refillRatePerSec: 1 };

    await limiter.check(cfgA);
    await limiter.check(cfgA);
    expect((await limiter.check(cfgA)).allowed).toBe(false);
    // client B untouched
    expect((await limiter.check(cfgB)).allowed).toBe(true);
  });

  it('honors a fractional per-request cost', async () => {
    const limiter = new RateLimiter(redis);
    const cfg: LimitConfig = { clientId: 'cost-test', capacity: 10, refillRatePerSec: 1 };
    const r1 = await limiter.check(cfg, 4);
    const r2 = await limiter.check(cfg, 4);
    expect(r1.allowed).toBe(true);
    expect(r2.allowed).toBe(true);
    const r3 = await limiter.check(cfg, 4); // only 2 tokens left
    expect(r3.allowed).toBe(false);
  });

  it('checks complete in a few milliseconds against local Redis', async () => {
    const limiter = new RateLimiter(redis);
    const cfg: LimitConfig = { clientId: 'perf-test', capacity: 100000, refillRatePerSec: 100000 };

    const samples: number[] = [];
    for (let i = 0; i < 200; i++) {
      const start = process.hrtime.bigint();
      await limiter.check(cfg);
      samples.push(Number(process.hrtime.bigint() - start) / 1e6);
    }
    samples.sort((a, b) => a - b);
    const p50 = samples[Math.floor(samples.length * 0.5)];
    const p99 = samples[Math.floor(samples.length * 0.99)];
    // generous bound for a shared CI/sandbox box; production same-AZ
    // Redis is typically sub-millisecond
    expect(p50).toBeLessThan(5);
    expect(p99).toBeLessThan(15);
  });
});
