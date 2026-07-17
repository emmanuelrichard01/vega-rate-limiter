import { CircuitBreaker } from '../src/ratelimiter/circuitBreaker';

describe('CircuitBreaker', () => {
  it('starts CLOSED and allows attempts', () => {
    const b = new CircuitBreaker({ failureThreshold: 3, cooldownMs: 100 });
    expect(b.getState()).toBe('CLOSED');
    expect(b.canAttempt()).toBe(true);
  });

  it('opens after N consecutive failures', () => {
    const b = new CircuitBreaker({ failureThreshold: 3, cooldownMs: 100 });
    b.onFailure();
    b.onFailure();
    expect(b.getState()).toBe('CLOSED');
    b.onFailure();
    expect(b.getState()).toBe('OPEN');
    expect(b.canAttempt()).toBe(false);
  });

  it('a success resets the failure count', () => {
    const b = new CircuitBreaker({ failureThreshold: 3, cooldownMs: 100 });
    b.onFailure();
    b.onFailure();
    b.onSuccess();
    b.onFailure();
    b.onFailure();
    expect(b.getState()).toBe('CLOSED'); // would have opened without the reset
  });

  it('transitions OPEN -> HALF_OPEN after cooldown, and closes on success', async () => {
    const b = new CircuitBreaker({ failureThreshold: 1, cooldownMs: 50 });
    b.onFailure();
    expect(b.getState()).toBe('OPEN');
    expect(b.canAttempt()).toBe(false);

    await new Promise((r) => setTimeout(r, 60));
    expect(b.canAttempt()).toBe(true); // half-open probe allowed
    expect(b.getState()).toBe('HALF_OPEN');

    b.onSuccess();
    expect(b.getState()).toBe('CLOSED');
  });

  it('re-opens if the half-open probe fails', async () => {
    const b = new CircuitBreaker({ failureThreshold: 1, cooldownMs: 50 });
    b.onFailure();
    await new Promise((r) => setTimeout(r, 60));
    expect(b.canAttempt()).toBe(true);
    b.onFailure();
    expect(b.getState()).toBe('OPEN');
  });

  it('only allows one in-flight probe while half-open', async () => {
    const b = new CircuitBreaker({ failureThreshold: 1, cooldownMs: 50 });
    b.onFailure();
    await new Promise((r) => setTimeout(r, 60));
    expect(b.canAttempt()).toBe(true);
    expect(b.canAttempt()).toBe(false); // second concurrent caller is blocked
  });
});
