CREATE TABLE IF NOT EXISTS clients (
  client_id           TEXT PRIMARY KEY,
  name                TEXT NOT NULL,
  capacity            INTEGER NOT NULL,       -- burst size, tokens
  refill_rate_per_sec DOUBLE PRECISION NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every rate-limit decision is logged for analytics; approved requests provide the billing basis, while denied requests are retained for throttling analysis.
-- billed but are counted separately below so clients can see how often
-- they're getting throttled.
CREATE TABLE IF NOT EXISTS request_log (
  id           BIGSERIAL PRIMARY KEY,
  client_id    TEXT NOT NULL REFERENCES clients(client_id),
  allowed      BOOLEAN NOT NULL,
  latency_ms   DOUBLE PRECISION NOT NULL,   -- time the check itself took
  source       TEXT NOT NULL,               -- 'redis' | 'fallback'
  occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_request_log_client_time
  ON request_log (client_id, occurred_at DESC);

-- Pre-aggregated per-day rollups so dashboard trend queries (10/15/30
-- day windows) don't have to scan raw request_log at read time.
CREATE TABLE IF NOT EXISTS usage_daily (
  client_id      TEXT NOT NULL REFERENCES clients(client_id),
  day            DATE NOT NULL,
  total_requests BIGINT NOT NULL DEFAULT 0,
  allowed_count  BIGINT NOT NULL DEFAULT 0,
  denied_count   BIGINT NOT NULL DEFAULT 0,
  avg_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
  PRIMARY KEY (client_id, day)
);

INSERT INTO clients (client_id, name, capacity, refill_rate_per_sec)
VALUES
  ('client-a', 'Client A', 100, 1.667),   -- 100 req/min
  ('client-b', 'Client B', 5000, 83.33)   -- 5000 req/min
ON CONFLICT (client_id) DO NOTHING;
