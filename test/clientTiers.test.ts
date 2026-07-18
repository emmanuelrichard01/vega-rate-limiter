import { Pool } from 'pg';
import { ClientConfigStore } from '../src/config/clientStore';

function testPool(): Pool {
  return new Pool({
    host: process.env.PGHOST ?? 'localhost',
    port: parseInt(process.env.PGPORT ?? '5432', 10),
    user: process.env.PGUSER ?? 'ratelimiter',
    password: process.env.PGPASSWORD ?? 'ratelimiter',
    database: process.env.PGDATABASE ?? 'ratelimiter',
  });
}

describe('ClientConfigStore tier resolution', () => {
  let pool: Pool;
  let store: ClientConfigStore;

  let redis: any;

  beforeAll(async () => {
    pool = testPool();
    const Redis = require('ioredis');
    redis = new Redis({
      host: process.env.REDIS_HOST ?? 'localhost',
      port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
    });
    store = new ClientConfigStore(pool, redis);
  });

  afterAll(async () => {
    store.close();
    await redis.quit();
    await pool.query(`DELETE FROM clients WHERE client_id LIKE 'tier-test-%'`);
    await pool.end();
  });

  afterEach(async () => {
    await pool.query(`DELETE FROM clients WHERE client_id LIKE 'tier-test-%'`);
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
    // explicit override wins even though a tier is also set
    expect(cfg?.capacity).toBe(250);
    expect(cfg?.refillRatePerSec).toBe(2.5);
  });

  it('a client with no tier needs explicit capacity and rate', async () => {
    await expect(
      store.upsert({ clientId: 'tier-test-invalid-client', name: 'Invalid Client' })
    ).rejects.toThrow(/tierId/);
  });

  it('bumping a tier moves every client inheriting from it', async () => {
    await store.upsert({ clientId: 'tier-test-shared-a', name: 'Shared A', tierId: 'premium' });
    await store.upsert({ clientId: 'tier-test-shared-b', name: 'Shared B', tierId: 'premium' });

    await pool.query(`UPDATE tiers SET capacity = 2000, refill_rate_per_sec = 2000.0/3600 WHERE tier_id = 'premium'`);
    await store.broadcastTierUpdate('premium');
    // Give pubsub a tiny moment
    await new Promise(r => setTimeout(r, 50));

    expect((await store.resolve('tier-test-shared-a'))?.capacity).toBe(2000);
    expect((await store.resolve('tier-test-shared-b'))?.capacity).toBe(2000);

    // restore so other tests/demo data aren't affected
    await pool.query(`UPDATE tiers SET capacity = 1000, refill_rate_per_sec = 1000.0/3600 WHERE tier_id = 'premium'`);
    await store.broadcastTierUpdate('premium');
    await new Promise(r => setTimeout(r, 50));
  });
});
