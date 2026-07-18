import Redis from 'ioredis';
import { LogEntry } from './types';

/**
 * Publishes approved/denied-check events onto a Redis Stream
 * (`XADD`) instead of buffering them in process memory. This is the
 * durability upgrade over the original in-memory queue: once XADD
 * acknowledges, the entry survives an API process crash -- it lives in
 * Redis until a consumer explicitly XACKs it, and even a crashed
 * consumer's unacked reads are recoverable (see streamConsumer.ts).
 *
 * `publish()` is still fire-and-forget from the caller's perspective --
 * the hot path (`POST /v1/check`) does not await it -- but failures are
 * now Redis-outage-scoped rather than silent-forever: if Redis is also
 * the thing that's down, the check path has already failed open via
 * the circuit breaker, and logging naturally degrades the same way
 * (best-effort, counted, never blocking the response).
 */
export class LogStreamProducer {
  private failedPublishes = 0;

  constructor(
    private redis: Redis,
    private streamKey = 'stream:request_log',
    private approxMaxLen = 200_000 // bounds stream growth if consumers fall behind
  ) {}

  publish(entry: LogEntry): void {
    this.redis
      .xadd(
        this.streamKey,
        'MAXLEN',
        '~',
        this.approxMaxLen,
        '*',
        'clientId',
        entry.clientId,
        'allowed',
        entry.allowed ? '1' : '0',
        'latencyMs',
        entry.latencyMs.toString(),
        'source',
        entry.source,
        'occurredAt',
        entry.occurredAt.toISOString()
      )
      .catch((err: Error) => {
        this.failedPublishes++;
        console.error('[logStream] publish failed', err.message);
      });
  }

  stats() {
    return { failedPublishes: this.failedPublishes };
  }
}
