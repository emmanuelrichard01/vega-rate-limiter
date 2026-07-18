import { Pool } from 'pg';
import { LimitConfig } from '../ratelimiter/types';

export interface Tier {
  tierId: string;
  name: string;
  capacity: number;
  refillRatePerSec: number;
}

export interface UpsertClientInput {
  clientId: string;
  name: string;
  tierId?: string | null;
  capacity?: number | null;
  refillRatePerSec?: number | null;
}

/**
 * Per-client limits live in Postgres (source of truth) but are cached
 * in-process so the hot path (`RateLimiter.check`) never does a DB
 * round-trip. A background refresh keeps the cache eventually
 * consistent (default: every 5s) across all rate-limiter instances.
 *
 * A client's effective limit is resolved as: its own capacity/rate if
 * set, otherwise its tier's. This lets thousands of "free" clients
 * share one definition (bump the tier once, every client on it moves
 * together) while any individual client can still get bespoke numbers
 * -- both mechanisms coexist, neither is special-cased in the
 * RateLimiter itself, which only ever sees the resolved LimitConfig.
 */
export class ClientConfigStore {
  private cache = new Map<string, LimitConfig & { tierId: string | null; name: string }>();
  private timer?: ReturnType<typeof setInterval>;

  constructor(private pool: Pool) {}

  async refresh(): Promise<void> {
    const { rows } = await this.pool.query(
      `SELECT
         c.client_id,
         c.name,
         c.tier_id,
         COALESCE(c.capacity, t.capacity) AS capacity,
         COALESCE(c.refill_rate_per_sec, t.refill_rate_per_sec) AS refill_rate_per_sec
       FROM clients c
       LEFT JOIN tiers t ON c.tier_id = t.tier_id`
    );
    const next = new Map<string, LimitConfig & { tierId: string | null; name: string }>();
    for (const r of rows) {
      next.set(r.client_id, {
        clientId: r.client_id,
        name: r.name,
        capacity: Number(r.capacity),
        refillRatePerSec: Number(r.refill_rate_per_sec),
        tierId: r.tier_id,
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

  /**
   * `tierId: null` explicitly clears a client's tier (falls back to its
   * own capacity/rate, which must then be provided). `capacity`/
   * `refillRatePerSec` left `undefined` means "don't touch this field";
   * `null` means "clear it, inherit from the tier instead."
   */
  async upsert(input: UpsertClientInput): Promise<void> {
    const capacity = input.capacity ?? null;
    const refillRatePerSec = input.refillRatePerSec ?? null;
    const tierId = input.tierId ?? null;

    if (tierId === null && (capacity === null || refillRatePerSec === null)) {
      throw new Error(
        'a client needs either a tierId or both capacity and refillRatePerSec'
      );
    }

    await this.pool.query(
      `INSERT INTO clients (client_id, name, tier_id, capacity, refill_rate_per_sec, updated_at)
       VALUES ($1, $2, $3, $4, $5, now())
       ON CONFLICT (client_id)
       DO UPDATE SET name = $2, tier_id = $3, capacity = $4, refill_rate_per_sec = $5, updated_at = now()`,
      [input.clientId, input.name, tierId, capacity, refillRatePerSec]
    );
    // Re-resolve from the DB rather than compute the effective values
    // in JS, so tier inheritance stays correct with a single code path.
    await this.refresh();
  }

  async listTiers(): Promise<Tier[]> {
    const { rows } = await this.pool.query(
      'SELECT tier_id, name, capacity, refill_rate_per_sec FROM tiers ORDER BY capacity ASC'
    );
    return rows.map((r) => ({
      tierId: r.tier_id,
      name: r.name,
      capacity: Number(r.capacity),
      refillRatePerSec: Number(r.refill_rate_per_sec),
    }));
  }

  all(): (LimitConfig & { tierId: string | null; name: string })[] {
    return [...this.cache.values()];
  }
}
