import Redis from 'ioredis';
import { Pool } from 'pg';

export interface StreamConsumerOptions {
  streamKey?: string;
  groupName?: string;
  consumerName?: string;
  batchSize?: number;
  blockMs?: number;
  claimIdleMs?: number; // how long a pending entry can sit unacked before we assume its consumer died
}

interface ParsedEntry {
  id: string;
  fields: Record<string, string>;
}

function parseStreamReply(reply: any): ParsedEntry[] {
  // ioredis raw reply shape: [[streamKey, [[id, [f,v,f,v,...]], ...]]]
  if (!reply || reply.length === 0) return [];
  const [, records] = reply[0];
  return (records ?? []).map(([id, kvArray]: [string, string[]]) => {
    const fields: Record<string, string> = {};
    for (let i = 0; i < kvArray.length; i += 2) {
      fields[kvArray[i]] = kvArray[i + 1];
    }
    return { id, fields };
  });
}

function parseAutoclaimReply(reply: any): { cursor: string; entries: ParsedEntry[] } {
  // [nextCursor, [[id, [f,v,...]], ...], deletedIds?]
  const [cursor, records] = reply;
  const entries: ParsedEntry[] = (records ?? []).map(([id, kvArray]: [string, string[]]) => {
    const fields: Record<string, string> = {};
    for (let i = 0; i < kvArray.length; i += 2) {
      fields[kvArray[i]] = kvArray[i + 1];
    }
    return { id, fields };
  });
  return { cursor, entries };
}

/**
 * Reads request-log events from the Redis Stream via a consumer group
 * (so multiple worker replicas can share the load, each entry going to
 * exactly one of them), batch-inserts into Postgres, and only XACKs
 * after the insert succeeds. If this process crashes between XREADGROUP
 * and XACK, the entry stays in the group's Pending Entries List and
 * `reclaimStale()` hands it to whichever consumer calls it next --
 * that's the actual durability guarantee, not just "we wrote to Redis
 * once and hoped."
 */
export class StreamConsumer {
  private opts: Required<StreamConsumerOptions>;
  private running = false;

  constructor(private redis: Redis, private pool: Pool, opts: StreamConsumerOptions = {}) {
    this.opts = {
      streamKey: opts.streamKey ?? 'stream:request_log',
      groupName: opts.groupName ?? 'log_consumers',
      consumerName: opts.consumerName ?? `worker-${process.pid}-${Math.random().toString(36).slice(2, 8)}`,
      batchSize: opts.batchSize ?? 200,
      blockMs: opts.blockMs ?? 2000,
      claimIdleMs: opts.claimIdleMs ?? 30_000,
    };
  }

  async ensureGroup(): Promise<void> {
    try {
      // MKSTREAM: create the stream too if it doesn't exist yet, so the
      // group can be set up before any producer has published anything.
      // '0' means the group starts from the beginning of the stream
      // rather than only new entries, so nothing published in a race
      // before the group existed is silently skipped.
      await this.redis.xgroup('CREATE', this.opts.streamKey, this.opts.groupName, '0', 'MKSTREAM');
    } catch (err: any) {
      if (!String(err.message).includes('BUSYGROUP')) throw err;
      // group already exists -- fine
    }
  }

  private async insertAndAck(entries: ParsedEntry[]): Promise<void> {
    if (entries.length === 0) return;

    const values: any[] = [];
    const rows: string[] = [];
    entries.forEach((e, i) => {
      const base = i * 5;
      rows.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
      values.push(
        e.fields.clientId,
        e.fields.allowed === '1',
        parseFloat(e.fields.latencyMs),
        e.fields.source,
        new Date(e.fields.occurredAt)
      );
    });

    // Only ack what we successfully wrote. If this insert throws, we
    // return without acking -- the entries stay pending and will be
    // retried (by us on the next loop, or reclaimed by another
    // consumer if we die first).
    await this.pool.query(
      `INSERT INTO request_log (client_id, allowed, latency_ms, source, occurred_at) VALUES ${rows.join(',')}`,
      values
    );

    await this.redis.xack(this.opts.streamKey, this.opts.groupName, ...entries.map((e) => e.id));
  }

  /** Reclaim entries that some other (likely dead) consumer read but never acked. */
  async reclaimStale(): Promise<number> {
    const reply = await (this.redis as any).xautoclaim(
      this.opts.streamKey,
      this.opts.groupName,
      this.opts.consumerName,
      this.opts.claimIdleMs,
      '0-0',
      'COUNT',
      this.opts.batchSize
    );
    const { entries } = parseAutoclaimReply(reply);
    if (entries.length > 0) {
      console.log(`[streamConsumer] reclaimed ${entries.length} stale pending entries`);
      await this.insertAndAck(entries);
    }
    return entries.length;
  }

  /** One read+process cycle. Exposed separately so tests don't need a live infinite loop. */
  async runOnce(): Promise<number> {
    await this.reclaimStale();

    const reply = await (this.redis as any).xreadgroup(
      'GROUP',
      this.opts.groupName,
      this.opts.consumerName,
      'COUNT',
      this.opts.batchSize,
      'BLOCK',
      this.opts.blockMs,
      'STREAMS',
      this.opts.streamKey,
      '>'
    );
    const entries = parseStreamReply(reply);
    await this.insertAndAck(entries);
    return entries.length;
  }

  async start(): Promise<void> {
    await this.ensureGroup();
    this.running = true;
    while (this.running) {
      try {
        await this.runOnce();
      } catch (err) {
        console.error('[streamConsumer] cycle failed, retrying', err);
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }

  stop(): void {
    this.running = false;
  }

  getConsumerName(): string {
    return this.opts.consumerName;
  }
}
