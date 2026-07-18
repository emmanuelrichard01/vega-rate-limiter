import { Pool } from 'pg';
import Redis from 'ioredis';
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
 * Enterprise Config Store using a Read-Through LRU Cache + Redis Pub/Sub.
 * 
 * Instead of periodically polling the entire `clients` table (which OOMs at scale),
 * this store holds up to MAX_CACHE_SIZE active clients in memory.
 * Cache misses query Postgres directly. When any node calls `upsert()`, it
 * publishes an invalidation message via Redis to instantly evict stale
 * configs across the entire API cluster.
 */
export class ClientConfigStore {
  private cache = new Map<string, LimitConfig & { tierId: string | null; name: string }>();
  private subRedis?: Redis;
  private readonly MAX_CACHE_SIZE = 10000;

  constructor(private pool: Pool, private redis?: Redis) {
    if (this.redis) {
      this.subRedis = this.redis.duplicate();
      this.subRedis.subscribe('config_invalidation').catch(err => 
        console.error('[clientStore] subscribe failed', err)
      );
      this.subRedis.on('message', (channel, message) => {
        if (channel === 'config_invalidation') {
          if (message.startsWith('tier:')) {
            const tierId = message.split(':')[1];
            for (const [clientId, cfg] of this.cache.entries()) {
              if (cfg.tierId === tierId) {
                this.cache.delete(clientId);
              }
            }
          } else if (message.startsWith('client:')) {
            const clientId = message.split(':')[1];
            this.cache.delete(clientId);
          }
        }
      });
    }
  }

  close() {
    if (this.subRedis) {
      this.subRedis.disconnect();
    }
  }

  async resolve(clientId: string): Promise<(LimitConfig & { tierId: string | null; name: string }) | undefined> {
    const existing = this.cache.get(clientId);
    if (existing) {
      // LRU logic: move recently accessed to the end of the Map
      this.cache.delete(clientId);
      this.cache.set(clientId, existing);
      return existing;
    }

    const { rows } = await this.pool.query(
      `SELECT
         c.client_id,
         c.name,
         c.tier_id,
         COALESCE(c.capacity, t.capacity) AS capacity,
         COALESCE(c.refill_rate_per_sec, t.refill_rate_per_sec) AS refill_rate_per_sec
       FROM clients c
       LEFT JOIN tiers t ON c.tier_id = t.tier_id
       WHERE c.client_id = $1`,
      [clientId]
    );

    if (rows.length === 0) return undefined;

    const r = rows[0];
    const cfg = {
      clientId: r.client_id,
      name: r.name,
      capacity: Number(r.capacity),
      refillRatePerSec: Number(r.refill_rate_per_sec),
      tierId: r.tier_id,
    };

    this.cache.set(clientId, cfg);
    
    // LRU Eviction: remove the oldest entry (first in Map iteration)
    if (this.cache.size > this.MAX_CACHE_SIZE) {
      const firstKey = this.cache.keys().next().value;
      if (firstKey) this.cache.delete(firstKey);
    }

    return cfg;
  }

  async upsert(input: UpsertClientInput): Promise<void> {
    const capacity = input.capacity ?? null;
    const refillRatePerSec = input.refillRatePerSec ?? null;
    const tierId = input.tierId ?? null;

    if (tierId === null && (capacity === null || refillRatePerSec === null)) {
      throw new Error('a client needs either a tierId or both capacity and refillRatePerSec');
    }

    await this.pool.query(
      `INSERT INTO clients (client_id, name, tier_id, capacity, refill_rate_per_sec, updated_at)
       VALUES ($1, $2, $3, $4, $5, now())
       ON CONFLICT (client_id)
       DO UPDATE SET name = $2, tier_id = $3, capacity = $4, refill_rate_per_sec = $5, updated_at = now()`,
      [input.clientId, input.name, tierId, capacity, refillRatePerSec]
    );
    
    if (this.redis) {
      await this.redis.publish('config_invalidation', `client:${input.clientId}`);
    } else {
      this.cache.delete(input.clientId);
    }
  }
  
  // Method to manually broadcast a tier update (used when tiers are changed)
  async broadcastTierUpdate(tierId: string): Promise<void> {
    if (this.redis) {
      await this.redis.publish('config_invalidation', `tier:${tierId}`);
    } else {
      for (const [clientId, cfg] of this.cache.entries()) {
        if (cfg.tierId === tierId) {
          this.cache.delete(clientId);
        }
      }
    }
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

  async all(): Promise<(LimitConfig & { tierId: string | null; name: string })[]> {
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
    return rows.map((r) => ({
      clientId: r.client_id,
      name: r.name,
      capacity: Number(r.capacity),
      refillRatePerSec: Number(r.refill_rate_per_sec),
      tierId: r.tier_id,
    }));
  }
}
