import { Pool } from 'pg';
import { LimitConfig } from '../ratelimiter/types';

/**
 * Per-client limits live in Postgres (source of truth) but are cached
 * in-process so the hot path (`RateLimiter.check`) never does a DB
 * round-trip. A background refresh keeps the cache eventually
 * consistent (default: every 5s) across all rate-limiter instances.
 */
export class ClientConfigStore {
  private cache = new Map<string, LimitConfig>();
  private timer?: ReturnType<typeof setInterval>;

  constructor(private pool: Pool) {}

  async refresh(): Promise<void> {
    const { rows } = await this.pool.query(
      'SELECT client_id, capacity, refill_rate_per_sec FROM clients'
    );
    const next = new Map<string, LimitConfig>();
    for (const r of rows) {
      next.set(r.client_id, {
        clientId: r.client_id,
        capacity: Number(r.capacity),
        refillRatePerSec: Number(r.refill_rate_per_sec),
      });
    }
    this.cache = next;
  }

  startAutoRefresh(intervalMs = 5000) {
    this.timer = setInterval(() => {
      this.refresh().catch((err) =>
        console.error('[clientStore] refresh failed', err)
      );
    }, intervalMs);
    return this.timer;
  }

  get(clientId: string): LimitConfig | undefined {
    return this.cache.get(clientId);
  }

  async upsert(cfg: LimitConfig, name: string): Promise<void> {
    await this.pool.query(
      `INSERT INTO clients (client_id, name, capacity, refill_rate_per_sec, updated_at)
       VALUES ($1, $2, $3, $4, now())
       ON CONFLICT (client_id)
       DO UPDATE SET name = $2, capacity = $3, refill_rate_per_sec = $4, updated_at = now()`,
      [cfg.clientId, name, cfg.capacity, cfg.refillRatePerSec]
    );
    this.cache.set(cfg.clientId, cfg);
  }

  all(): LimitConfig[] {
    return [...this.cache.values()];
  }
}
