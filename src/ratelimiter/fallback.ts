import { LimitConfig, LimitResult } from './types';

interface LocalBucket {
  tokens: number;
  ts: number; // ms
}

/**
 * Per-instance, in-memory token bucket. Used only while the circuit
 * breaker has judged Redis unreachable.
 *
 * This is intentionally a fail-OPEN degradation: each node enforces the
 * client's full limit independently, so a client can burst up to
 * (limit * number_of_healthy_nodes) for the duration of the outage.
 * That is the correct trade-off for this system: the brief, unlikely to be exact, over-admission
 * during a Redis outage is far cheaper than dropping legitimate traffic
 * (which is the whole point of requirement #4: never block all traffic).
 */
export class FallbackLimiter {
  private buckets = new Map<string, LocalBucket>();

  check(clientId: string, cfg: LimitConfig, cost: number): LimitResult {
    const now = Date.now();
    let b = this.buckets.get(clientId);
    if (!b) {
      b = { tokens: cfg.capacity, ts: now };
    }

    const elapsedSec = Math.max(0, now - b.ts) / 1000;
    let tokens = Math.min(cfg.capacity, b.tokens + elapsedSec * cfg.refillRatePerSec);

    let allowed: boolean;
    let retryAfterMs = 0;

    if (tokens >= cost) {
      tokens -= cost;
      allowed = true;
    } else {
      allowed = false;
      const deficit = cost - tokens;
      retryAfterMs = Math.ceil((deficit / cfg.refillRatePerSec) * 1000);
    }

    this.buckets.set(clientId, { tokens, ts: now });

    return {
      allowed,
      remaining: tokens,
      retryAfterMs,
      source: 'fallback',
    };
  }

  /** Periodic cleanup so long-idle clients don't accumulate forever. */
  sweep(maxIdleMs: number): void {
    const now = Date.now();
    for (const [id, b] of this.buckets) {
      if (now - b.ts > maxIdleMs) this.buckets.delete(id);
    }
  }

  size(): number {
    return this.buckets.size;
  }
}
