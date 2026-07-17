import { Pool } from 'pg';

export interface LogEntry {
  clientId: string;
  allowed: boolean;
  latencyMs: number;
  source: 'redis' | 'fallback';
  occurredAt: Date;
}

/**
 * In-process buffer + background flusher. `enqueue()` is synchronous and
 * never awaited by the request path — it just pushes into an array.
 * A timer periodically batch-inserts into Postgres. If the buffer hits
 * a hard cap (backpressure from a very slow DB) we drop the oldest
 * entries rather than grow unbounded memory, since logging must never
 * be allowed to threaten the availability of the check path itself.
 */
export class LogQueue {
  private buffer: LogEntry[] = [];
  private flushing = false;
  private readonly maxBufferSize = 50_000;
  private droppedCount = 0;

  constructor(
    private pool: Pool,
    private opts: { batchSize?: number; flushIntervalMs?: number } = {}
  ) {}

  enqueue(entry: LogEntry): void {
    if (this.buffer.length >= this.maxBufferSize) {
      this.buffer.shift();
      this.droppedCount++;
      return;
    }
    this.buffer.push(entry);
  }

  startFlusher(): ReturnType<typeof setInterval> {
    const interval = this.opts.flushIntervalMs ?? 1000;
    return setInterval(() => {
      this.flush().catch((err) => console.error('[logQueue] flush failed', err));
    }, interval);
  }

  async flush(): Promise<number> {
    if (this.flushing || this.buffer.length === 0) return 0;
    this.flushing = true;
    const batchSize = this.opts.batchSize ?? 500;
    const batch = this.buffer.splice(0, batchSize);

    try {
      const values: any[] = [];
      const rows: string[] = [];
      batch.forEach((e, i) => {
        const base = i * 5;
        rows.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
        values.push(e.clientId, e.allowed, e.latencyMs, e.source, e.occurredAt);
      });

      await this.pool.query(
        `INSERT INTO request_log (client_id, allowed, latency_ms, source, occurred_at)
         VALUES ${rows.join(',')}`,
        values
      );
      return batch.length;
    } catch (err) {
      // put the batch back so we retry on the next tick instead of losing it
      this.buffer.unshift(...batch);
      throw err;
    } finally {
      this.flushing = false;
    }
  }

  stats() {
    return { buffered: this.buffer.length, dropped: this.droppedCount };
  }
}
