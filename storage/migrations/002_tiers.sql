-- Tiers let many clients share one limit definition (the realistic
-- case: thousands of "free" users, not thousands of hand-typed rows)
-- while individual clients can still override with their own numbers,
-- same as before this migration. Resolution: a client's effective
-- limit is COALESCE(client's own capacity/rate, its tier's).
CREATE TABLE IF NOT EXISTS tiers (
  tier_id             TEXT PRIMARY KEY,
  name                TEXT NOT NULL,
  capacity            INTEGER NOT NULL,
  refill_rate_per_sec DOUBLE PRECISION NOT NULL
);

INSERT INTO tiers (tier_id, name, capacity, refill_rate_per_sec)
VALUES
  ('free',    'Free',    100,  100.0  / 3600),  -- 100 req/hour
  ('premium', 'Premium', 1000, 1000.0 / 3600)   -- 1000 req/hour
ON CONFLICT (tier_id) DO NOTHING;

ALTER TABLE clients ALTER COLUMN capacity DROP NOT NULL;
ALTER TABLE clients ALTER COLUMN refill_rate_per_sec DROP NOT NULL;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS tier_id TEXT REFERENCES tiers(tier_id);

-- A client must resolve to *some* limit: either its own explicit
-- numbers, or a tier to inherit from (or both, where the client's own
-- numbers act as an override on top of the tier).
DO $$
BEGIN
  ALTER TABLE clients ADD CONSTRAINT clients_limit_source_chk
    CHECK (tier_id IS NOT NULL OR (capacity IS NOT NULL AND refill_rate_per_sec IS NOT NULL));
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

-- Demo clients showing pure tier inheritance (no per-client override).
INSERT INTO clients (client_id, name, tier_id)
VALUES
  ('client-free-1',    'Free Tier Demo Client',    'free'),
  ('client-premium-1', 'Premium Tier Demo Client', 'premium')
ON CONFLICT (client_id) DO NOTHING;
