import { Pool } from 'pg';

/**
 * Recomputes usage_daily for today (and yesterday, to catch stragglers
 * that land just after midnight) from the raw request_log. Keeping this
 * separate from the write-path LogQueue means dashboard trend queries
 * over 10/15/30-day windows hit a small pre-aggregated table instead of
 * scanning potentially millions of raw log rows.
 */
export async function rollupRecentDays(pool: Pool): Promise<void> {
  await pool.query(`
    INSERT INTO usage_daily (client_id, day, total_requests, allowed_count, denied_count, avg_latency_ms)
    SELECT
      client_id,
      occurred_at::date AS day,
      COUNT(*) AS total_requests,
      COUNT(*) FILTER (WHERE allowed) AS allowed_count,
      COUNT(*) FILTER (WHERE NOT allowed) AS denied_count,
      AVG(latency_ms) AS avg_latency_ms
    FROM request_log
    WHERE occurred_at::date >= (now() - interval '2 days')::date
    GROUP BY client_id, occurred_at::date
    ON CONFLICT (client_id, day) DO UPDATE SET
      total_requests = EXCLUDED.total_requests,
      allowed_count  = EXCLUDED.allowed_count,
      denied_count   = EXCLUDED.denied_count,
      avg_latency_ms = EXCLUDED.avg_latency_ms
  `);
}

export function startRollupTimer(pool: Pool, intervalMs = 30_000) {
  return setInterval(() => {
    rollupRecentDays(pool).catch((err) => console.error('[rollup] failed', err));
  }, intervalMs);
}
