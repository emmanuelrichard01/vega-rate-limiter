import request from 'supertest';
import { createApp } from '../src/api/app';
import { ClientConfigStore } from '../src/config/clientStore';
import { LogStreamProducer } from '../src/logging/streamProducer';
import { Pool } from 'pg';
import Redis from 'ioredis';
import { RateLimiter } from '../src/ratelimiter/limiter';

describe('Input Validation', () => {
  let app: any;

  beforeAll(() => {
    // Mock the dependencies since we only care about API layer validation
    const store = {
      resolve: async (id: string) => {
        if (id === 'valid-client') return { clientId: 'valid-client', capacity: 10, refillRatePerSec: 1 };
        return null;
      },
    } as unknown as ClientConfigStore;

    const limiter = {
      check: async () => ({ allowed: true, remaining: 9, retryAfterMs: 0, source: 'redis' }),
      getBreakerState: () => 'CLOSED',
    } as unknown as RateLimiter;

    const logStream = {
      publish: () => {},
      stats: () => ({ queueLength: 0, published: 0, dropped: 0 }),
    } as unknown as LogStreamProducer;

    const pool = {} as Pool;

    // Use a fixed service key for auth
    process.env.SERVICE_API_KEY = 'test-service-key';
    process.env.ADMIN_API_KEY = 'test-admin-key';
    
    app = createApp(limiter, store, logStream, pool);
  });

  const authHeader = 'Bearer test-service-key';
  const adminHeader = 'Bearer test-admin-key';

  describe('POST /v1/check (cost validation)', () => {
    it('rejects cost = 0', async () => {
      const res = await request(app)
        .post('/v1/check')
        .set('Authorization', authHeader)
        .send({ clientId: 'valid-client', cost: 0 });
      expect(res.status).toBe(400);
      expect(res.body.error).toContain('positive finite number');
    });

    it('rejects negative cost', async () => {
      const res = await request(app)
        .post('/v1/check')
        .set('Authorization', authHeader)
        .send({ clientId: 'valid-client', cost: -5 });
      expect(res.status).toBe(400);
      expect(res.body.error).toContain('positive finite number');
    });

    it('rejects string types', async () => {
      const res = await request(app)
        .post('/v1/check')
        .set('Authorization', authHeader)
        .send({ clientId: 'valid-client', cost: 'abc' });
      expect(res.status).toBe(400);
    });

    it('rejects cost > capacity', async () => {
      const res = await request(app)
        .post('/v1/check')
        .set('Authorization', authHeader)
        .send({ clientId: 'valid-client', cost: 20 });
      expect(res.status).toBe(400);
      expect(res.body.error).toContain('cannot exceed bucket capacity');
    });

    it('accepts valid cost', async () => {
      const res = await request(app)
        .post('/v1/check')
        .set('Authorization', authHeader)
        .send({ clientId: 'valid-client', cost: 5 });
      expect(res.status).toBe(200);
    });
  });

  describe('PUT /v1/tiers/:tierId (admin validation)', () => {
    it('rejects negative capacity', async () => {
      const res = await request(app)
        .put('/v1/tiers/test-tier')
        .set('Authorization', adminHeader)
        .send({ name: 'test', capacity: -10, refillRatePerSec: 10 });
      expect(res.status).toBe(400);
      expect(res.body.error).toContain('positive finite number');
    });

    it('rejects zero refill rate', async () => {
      const res = await request(app)
        .put('/v1/tiers/test-tier')
        .set('Authorization', adminHeader)
        .send({ name: 'test', capacity: 10, refillRatePerSec: 0 });
      expect(res.status).toBe(400);
      expect(res.body.error).toContain('positive finite number');
    });

    it('rejects negative refill rate', async () => {
      const res = await request(app)
        .put('/v1/tiers/test-tier')
        .set('Authorization', adminHeader)
        .send({ name: 'test', capacity: 10, refillRatePerSec: -5 });
      expect(res.status).toBe(400);
      expect(res.body.error).toContain('positive finite number');
    });
  });
});
