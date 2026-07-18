ALTER TABLE request_log ADD COLUMN IF NOT EXISTS stream_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS request_log_stream_id_idx ON request_log (stream_id);
