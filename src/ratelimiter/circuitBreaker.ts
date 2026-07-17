/**
 * Minimal circuit breaker guarding the Redis-backed rate limiter.
 *
 * CLOSED  -> calls go to Redis normally.
 * OPEN    -> calls skip Redis entirely and go straight to the local
 *            fallback limiter, so a dead/slow Redis never blocks traffic.
 * HALF_OPEN -> after a cooldown, a single trial call is allowed through;
 *            success closes the breaker again, failure re-opens it.
 */
export type BreakerState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export interface CircuitBreakerOptions {
  failureThreshold: number; // consecutive failures before opening
  cooldownMs: number;       // time to wait before trying a half-open probe
}

export class CircuitBreaker {
  private state: BreakerState = 'CLOSED';
  private consecutiveFailures = 0;
  private openedAt = 0;
  private halfOpenInFlight = false;

  constructor(private opts: CircuitBreakerOptions) {}

  /** Should this call attempt Redis at all? */
  canAttempt(): boolean {
    if (this.state === 'CLOSED') return true;

    if (this.state === 'OPEN') {
      if (Date.now() - this.openedAt >= this.opts.cooldownMs) {
        this.state = 'HALF_OPEN';
        this.halfOpenInFlight = false;
      } else {
        return false;
      }
    }

    if (this.state === 'HALF_OPEN') {
      // only let one trial request through at a time
      if (this.halfOpenInFlight) return false;
      this.halfOpenInFlight = true;
      return true;
    }

    return false;
  }

  onSuccess(): void {
    this.consecutiveFailures = 0;
    this.state = 'CLOSED';
    this.halfOpenInFlight = false;
  }

  onFailure(): void {
    this.consecutiveFailures += 1;
    if (this.state === 'HALF_OPEN') {
      // probe failed, back to fully open
      this.state = 'OPEN';
      this.openedAt = Date.now();
      this.halfOpenInFlight = false;
      return;
    }
    if (this.consecutiveFailures >= this.opts.failureThreshold) {
      this.state = 'OPEN';
      this.openedAt = Date.now();
    }
  }

  getState(): BreakerState {
    return this.state;
  }

  trip(durationMs: number = 10000): void {
    this.state = 'OPEN';
    this.consecutiveFailures = this.opts.failureThreshold;
    this.openedAt = Date.now() + durationMs - this.opts.cooldownMs;
    this.halfOpenInFlight = false;
  }
}
