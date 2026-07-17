import { Pool } from 'pg';
import Redis from 'ioredis';
import * as os from 'os';

export interface LogEntry {
  clientId: string;
  allowed: boolean;
  latencyMs: number;
  source: 'redis' | 'fallback';
  occurredAt: Date;
}

/**
 * Durable logging queue utilizing Redis Streams.
 *
 * `enqueue()` fires an XADD. If Redis is down, it falls back to a limited
 * in-memory buffer to prevent traffic blocking.
 *
 * A background timer reads from the Redis stream via XREADGROUP (at-least-once delivery),
 * batch inserts to Postgres, and then calls XACK.
 *
 * `recover()` reclaims and processes messages left pending by crashed workers via XAUTOCLAIM.
 */
export class LogQueue {
  private buffer: LogEntry[] = [];
  private flushing = false;
  private recovering = false;
  private readonly maxBufferSize = 50_000;
  private droppedCount = 0;
  private workerName = `${os.hostname()}-${process.pid}`;

  constructor(
    private pool: Pool,
    private redis: Redis,
    private redisWorker: Redis,
    private opts: { batchSize?: number; flushIntervalMs?: number } = {}
  ) {}

  async init() {
    try {
      await this.redisWorker.xgroup('CREATE', 'request_logs', 'log_group', '0', 'MKSTREAM');
    } catch (err: any) {
      if (!err.message.includes('BUSYGROUP')) {
        console.error('[logQueue] failed to create consumer group', err);
      }
    }
  }

  enqueue(entry: LogEntry): void {
    // Fire and forget durable write.
    this.redis.xadd('request_logs', 'MAXLEN', '~', 1000000, '*', 'payload', JSON.stringify(entry))
      .catch((err) => {
        // Fallback to in-memory if Redis is unreachable
        if (this.buffer.length >= this.maxBufferSize) {
          this.buffer.shift();
          this.droppedCount++;
          return;
        }
        this.buffer.push(entry);
      });
  }

  startFlusher(): ReturnType<typeof setInterval> {
    const interval = this.opts.flushIntervalMs ?? 1000;
    return setInterval(() => {
      this.flush().catch((err) => console.error('[logQueue] flush failed', err));
      this.recover().catch((err) => console.error('[logQueue] recover failed', err));
    }, interval);
  }

  async flush(): Promise<number> {
    if (this.flushing) return 0;
    this.flushing = true;
    const batchSize = this.opts.batchSize ?? 500;
    let processed = 0;

    try {
      // 1. Drain the fail-open fallback memory buffer first
      if (this.buffer.length > 0) {
        const memBatch = this.buffer.splice(0, batchSize);
        try {
          await this.insertToPg(memBatch);
          processed += memBatch.length;
        } catch (err) {
          // Re-queue on DB failure
          this.buffer.unshift(...memBatch);
          throw err;
        }
      }

      // 2. Consume new durable messages from Redis Stream
      const raw = await this.redisWorker.xreadgroup(
        'GROUP', 'log_group', this.workerName,
        'COUNT', batchSize,
        'STREAMS', 'request_logs', '>'
      ) as any;

      if (raw && raw.length > 0) {
        const stream = raw[0];
        const messages = stream[1];
        if (messages.length > 0) {
          const entries: LogEntry[] = [];
          const ids: string[] = [];
          for (const msg of messages) {
            ids.push(msg[0]);
            const payloadIndex = msg[1].indexOf('payload');
            if (payloadIndex >= 0) {
              entries.push(JSON.parse(msg[1][payloadIndex + 1]));
            }
          }
          await this.insertToPg(entries);
          await this.redisWorker.xack('request_logs', 'log_group', ...ids);
          processed += entries.length;
        }
      }
    } finally {
      this.flushing = false;
    }
    return processed;
  }

  async recover() {
    if (this.recovering) return;
    this.recovering = true;
    const batchSize = this.opts.batchSize ?? 500;
    
    try {
      // Claim messages pending for > 10 seconds from dead workers
      const raw = await this.redisWorker.xautoclaim('request_logs', 'log_group', this.workerName, 10000, '0-0', 'COUNT', batchSize) as any;
      if (raw && raw[1] && raw[1].length > 0) {
        const messages = raw[1];
        const entries: LogEntry[] = [];
        const ids: string[] = [];
        for (const msg of messages) {
          ids.push(msg[0]);
          const payloadIndex = msg[1].indexOf('payload');
          if (payloadIndex >= 0) {
            entries.push(JSON.parse(msg[1][payloadIndex + 1]));
          }
        }
        if (entries.length > 0) {
          await this.insertToPg(entries);
          await this.redisWorker.xack('request_logs', 'log_group', ...ids);
        }
      }
    } finally {
      this.recovering = false;
    }
  }

  private async insertToPg(batch: LogEntry[]) {
    if (batch.length === 0) return;
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
  }

  stats() {
    return { buffered: this.buffer.length, dropped: this.droppedCount };
  }
}
