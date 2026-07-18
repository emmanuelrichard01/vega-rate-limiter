import { Pool } from 'pg';
import request from 'supertest';
import { ClientConfigStore } from '../src/config/clientStore';
import { createApp } from '../src/api/app';
import { RateLimiter } from '../src/ratelimiter/limiter';
import { LogStreamProducer } from '../src/logging/streamProducer';
import { runMigrations } from '../src/storage/postgres';

function testPool(): Pool {
  return new Pool({
    host: process.env.PGHOST ?? 'localhost',
    port: parseInt(process.env.PGPORT ?? '5432', 10),
    user: process.env.PGUSER ?? 'ratelimiter',
    password: process.env.PGPASSWORD ?? 'ratelimiter',
    database: process.env.PGDATABASE ?? 'ratelimiter',
  });
}

describe('ClientConfigStore tier resolution and endpoints', () => {
  let pool: Pool;
  let store: ClientConfigStore;
  let redis: any;
  let app: any;

  beforeAll(async () => {
    pool = testPool();
    await runMigrations(pool);
    const Redis = require('ioredis');
    redis = new Redis({
      host: process.env.REDIS_HOST ?? 'localhost',
      port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
    });
    store = new ClientConfigStore(pool, redis);
    const limiter = new RateLimiter(redis, {
      redisTimeoutMs: 20,
      breakerFailureThreshold: 3,
      breakerCooldownMs: 2000,
    });
    const logStream = new LogStreamProducer(redis);
    
    process.env.ADMIN_API_KEY = 'test-admin-key';
    app = createApp(limiter, store, logStream, pool);
  });

  afterAll(async () => {
    store.close();
    await redis.quit();
    await pool.query(`DELETE FROM clients WHERE client_id LIKE 'tier-test-%'`);
    await pool.query(`DELETE FROM tiers WHERE tier_id LIKE 'tier-test-%'`);
    await pool.end();
  });

  afterEach(async () => {
    await pool.query(`DELETE FROM clients WHERE client_id LIKE 'tier-test-%'`);
    await pool.query(`DELETE FROM tiers WHERE tier_id LIKE 'tier-test-%'`);
  });

  it('lists the seeded tiers', async () => {
    const tiers = await store.listTiers();
    const byId = Object.fromEntries(tiers.map((t) => [t.tierId, t]));
    expect(byId.free.capacity).toBe(100);
    expect(byId.free.refillRatePerSec).toBeCloseTo(100 / 3600, 5);
    expect(byId.premium.capacity).toBe(1000);
    expect(byId.premium.refillRatePerSec).toBeCloseTo(1000 / 3600, 5);
  });

  it('a client on a tier with no override inherits the tier limits', async () => {
    await store.upsert({ clientId: 'tier-test-free-client', name: 'Free Client', tierId: 'free' });
    const cfg = await store.resolve('tier-test-free-client');
    expect(cfg?.capacity).toBe(100);
    expect(cfg?.refillRatePerSec).toBeCloseTo(100 / 3600, 5);
  });

  it("a client's own capacity/rate overrides its tier's", async () => {
    await store.upsert({
      clientId: 'tier-test-override-client',
      name: 'Override Client',
      tierId: 'free',
      capacity: 250,
      refillRatePerSec: 2.5,
    });
    const cfg = await store.resolve('tier-test-override-client');
    expect(cfg?.capacity).toBe(250);
    expect(cfg?.refillRatePerSec).toBe(2.5);
  });

  it('bumping a tier via the new PUT endpoint broadcasts and moves every client inheriting from it', async () => {
    await store.upsert({ clientId: 'tier-test-shared-a', name: 'Shared A', tierId: 'premium' });
    await store.upsert({ clientId: 'tier-test-shared-b', name: 'Shared B', tierId: 'premium' });

    // Ensure they are in the LRU cache
    await store.resolve('tier-test-shared-a');
    await store.resolve('tier-test-shared-b');

    const res = await request(app)
      .put('/v1/tiers/premium')
      .set('Authorization', 'Bearer test-admin-key')
      .send({ name: 'Premium (Updated)', capacity: 2000, refillRatePerSec: 2000.0/3600 });
      
    expect(res.status).toBe(204);

    // Give pubsub a tiny moment
    await new Promise(r => setTimeout(r, 50));

    expect((await store.resolve('tier-test-shared-a'))?.capacity).toBe(2000);
    expect((await store.resolve('tier-test-shared-b'))?.capacity).toBe(2000);

    // restore
    await request(app)
      .put('/v1/tiers/premium')
      .set('Authorization', 'Bearer test-admin-key')
      .send({ name: 'Premium', capacity: 1000, refillRatePerSec: 1000.0/3600 });
    await new Promise(r => setTimeout(r, 50));
  });

  it('changes via raw SQL with NO broadcast are eventually reconciled within the polling window', async () => {
    jest.useFakeTimers();
    
    await store.upsert({ clientId: 'tier-test-reconcile', name: 'Reconcile', tierId: 'premium' });
    // Warm the cache
    await store.resolve('tier-test-reconcile');
    
    // Bypass the API and PubSub entirely
    await pool.query(`UPDATE tiers SET capacity = 3000 WHERE tier_id = 'premium'`);
    
    // Immediately resolving should still yield the stale cache (1000 or whatever it was restored to)
    expect((await store.resolve('tier-test-reconcile'))?.capacity).toBe(1000);

    // Fast-forward 65 seconds to trigger the setInterval reconciliation loop
    jest.advanceTimersByTime(65000);
    
    // Let any async query operations finish (since setInterval callback is async)
    // In jest with fake timers and async tasks, we must tick the microtask queue
    await Promise.resolve();

    // Cache should now be refreshed
    expect((await store.resolve('tier-test-reconcile'))?.capacity).toBe(3000);

    // Restore and cleanup
    jest.useRealTimers();
    await pool.query(`UPDATE tiers SET capacity = 1000 WHERE tier_id = 'premium'`);
  });

  it('DELETE /v1/clients/:clientId soft-deletes the client and retains request logs', async () => {
    // 1. Create client
    await store.upsert({ clientId: 'tier-test-delete-client', name: 'Delete Me', tierId: 'free' });
    
    // 2. Add some fake request log data to usage_daily
    await pool.query(`INSERT INTO usage_daily (day, client_id, total_requests) VALUES (now()::date, 'tier-test-delete-client', 5)`);

    // 3. Delete the client via API
    const res = await request(app)
      .delete('/v1/clients/tier-test-delete-client')
      .set('Authorization', 'Bearer test-admin-key');
    expect(res.status).toBe(204);

    // Give pubsub a moment
    await new Promise(r => setTimeout(r, 50));

    // 4. Resolving should return undefined (404)
    expect(await store.resolve('tier-test-delete-client')).toBeUndefined();

    // 5. Query usage_daily directly to ensure the rows were NOT hard-deleted
    const { rows } = await pool.query(`SELECT * FROM usage_daily WHERE client_id = 'tier-test-delete-client'`);
    expect(rows.length).toBeGreaterThan(0);
    
    // 6. Re-creating the client should work (reactivate)
    await store.upsert({ clientId: 'tier-test-delete-client', name: 'Reactivated Client', tierId: 'free' });
    expect(await store.resolve('tier-test-delete-client')).toBeDefined();
    
    // cleanup usage_daily
    await pool.query(`DELETE FROM usage_daily WHERE client_id = 'tier-test-delete-client'`);
  });
});
