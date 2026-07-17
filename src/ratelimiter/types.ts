export interface LimitConfig {
  clientId: string;
  capacity: number;         // burst size (tokens)
  refillRatePerSec: number; // sustained rate (tokens/sec) e.g. 100 req/min = 1.667/sec
}

export interface LimitResult {
  allowed: boolean;
  remaining: number;
  retryAfterMs: number;
  source: 'redis' | 'fallback';
}
