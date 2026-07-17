import { FallbackLimiter } from '../src/ratelimiter/fallback';
import { LimitConfig } from '../src/ratelimiter/types';

describe('FallbackLimiter', () => {
  const cfg: LimitConfig = { clientId: 'c1', capacity: 5, refillRatePerSec: 5 };

  it('allows up to capacity as an initial burst', () => {
    const rl = new FallbackLimiter();
    for (let i = 0; i < 5; i++) {
      expect(rl.check('c1', cfg, 1).allowed).toBe(true);
    }
  });

  it('denies once capacity is exhausted', () => {
    const rl = new FallbackLimiter();
    for (let i = 0; i < 5; i++) rl.check('c1', cfg, 1);
    const result = rl.check('c1', cfg, 1);
    expect(result.allowed).toBe(false);
    expect(result.retryAfterMs).toBeGreaterThan(0);
  });

  it('refills over time at refillRatePerSec', async () => {
    const rl = new FallbackLimiter();
    for (let i = 0; i < 5; i++) rl.check('c1', cfg, 1); // drain
    expect(rl.check('c1', cfg, 1).allowed).toBe(false);

    await new Promise((r) => setTimeout(r, 250)); // ~1.25 tokens at 5/s
    expect(rl.check('c1', cfg, 1).allowed).toBe(true);
  });

  it('never exceeds capacity even after long idle periods', async () => {
    const rl = new FallbackLimiter();
    rl.check('c1', cfg, 1); // tokens: 4
    await new Promise((r) => setTimeout(r, 300));
    // should cap at 5, not overflow from the elapsed-time refill math
    let allowedCount = 0;
    for (let i = 0; i < 6; i++) {
      if (rl.check('c1', cfg, 1).allowed) allowedCount++;
    }
    expect(allowedCount).toBe(5);
  });

  it('tracks independent buckets per client', () => {
    const rl = new FallbackLimiter();
    for (let i = 0; i < 5; i++) rl.check('clientX', cfg, 1);
    expect(rl.check('clientX', cfg, 1).allowed).toBe(false);
    // a different client is unaffected
    expect(rl.check('clientY', cfg, 1).allowed).toBe(true);
  });

  it('sweeps idle buckets', async () => {
    const rl = new FallbackLimiter();
    rl.check('c1', cfg, 1);
    expect(rl.size()).toBe(1);
    await new Promise((r) => setTimeout(r, 50));
    rl.sweep(10); // maxIdleMs = 10ms, our entry is older than that
    expect(rl.size()).toBe(0);
  });
});
