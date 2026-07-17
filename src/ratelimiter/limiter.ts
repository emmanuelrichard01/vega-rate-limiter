import Redis from 'ioredis';
import fs from 'fs';
import path from 'path';
import { CircuitBreaker } from './circuitBreaker';
import { FallbackLimiter } from './fallback';
import { LimitConfig, LimitResult } from './types';

const LUA_SCRIPT = fs.readFileSync(
  path.join(__dirname, 'tokenbucket.lua'),
  'utf-8'
);

export interface RateLimiterOptions {
  redisTimeoutMs?: number;      // per-call budget before we treat Redis as failed
  breakerFailureThreshold?: number;
  breakerCooldownMs?: number;
  bucketTtlSeconds?: number;
}

// Augment ioredis's Redis type with the command we register via
// defineCommand below (ioredis auto-generates the JS method from the
// name we give it, but doesn't know about it at compile time).
declare module 'ioredis' {
  interface Redis {
    checkTokenBucket(
      key: string,
      capacity: number | string,
      refillRate: number | string,
      cost: number | string,
      ttlSeconds: number | string
    ): Promise<[number, string, number]>;
  }
}

export class RateLimiter {
  private redis: Redis;
  private breaker: CircuitBreaker;
  private fallback = new FallbackLimiter();
  private opts: Required<RateLimiterOptions>;

  constructor(redis: Redis, opts: RateLimiterOptions = {}) {
    this.redis = redis;
    this.opts = {
      redisTimeoutMs: opts.redisTimeoutMs ?? 20,
      breakerFailureThreshold: opts.breakerFailureThreshold ?? 3,
      breakerCooldownMs: opts.breakerCooldownMs ?? 2000,
      bucketTtlSeconds: opts.bucketTtlSeconds ?? 3600,
    };
    this.breaker = new CircuitBreaker({
      failureThreshold: this.opts.breakerFailureThreshold,
      cooldownMs: this.opts.breakerCooldownMs,
    });

    // Registers a `checkTokenBucket` command on this client. ioredis
    // caches the script's SHA and automatically retries with a full
    // EVAL if Redis ever responds NOSCRIPT (e.g. after a restart) --
    // we don't have to hand-roll that fallback ourselves.
    if (typeof (this.redis as any).checkTokenBucket !== 'function') {
      this.redis.defineCommand('checkTokenBucket', {
        numberOfKeys: 1,
        lua: LUA_SCRIPT,
      });
    }
  }

  private async withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
    return Promise.race([
      p,
      new Promise<T>((_, reject) =>
        setTimeout(() => reject(new Error('redis_timeout')), ms)
      ),
    ]);
  }

  /**
   * Check-and-consume one request for `clientId`. Never throws: on any
   * Redis failure (down, timeout, NOSCRIPT storm) it transparently falls
   * back to the local approximate limiter so the caller is never blocked
   * by an infrastructure problem downstream of the rate limiter itself.
   */
  async check(cfg: LimitConfig, cost = 1): Promise<LimitResult> {
    if (this.breaker.canAttempt()) {
      try {
        const key = `rl:{${cfg.clientId}}`;
        const raw = await this.withTimeout(
          this.redis.checkTokenBucket(
            key,
            cfg.capacity,
            cfg.refillRatePerSec,
            cost,
            this.opts.bucketTtlSeconds
          ),
          this.opts.redisTimeoutMs
        );

        this.breaker.onSuccess();
        const [allowed, remaining, retryAfterMs] = raw;
        return {
          allowed: allowed === 1,
          remaining: parseFloat(remaining),
          retryAfterMs,
          source: 'redis',
        };
      } catch (err) {
        console.error('[RateLimiter] redis check failed:', err);
        this.breaker.onFailure();
        // fall through to fallback below
      }
    }

    return this.fallback.check(cfg.clientId, cfg, cost);
  }

  getBreakerState() {
    return this.breaker.getState();
  }

  simulateOutage(durationMs = 10000) {
    this.breaker.trip(durationMs);
  }

  startFallbackSweeper(intervalMs = 60_000, maxIdleMs = 10 * 60_000) {
    return setInterval(() => this.fallback.sweep(maxIdleMs), intervalMs);
  }
}
