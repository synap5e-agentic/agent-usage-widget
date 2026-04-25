-- Schema for agent-usage-widget persistence.
-- Bootstrap with:
--   psql "host=127.0.0.1 port=5433 dbname=agent_usage user=agent_usage" < poller/schema.sql

CREATE TABLE IF NOT EXISTS usage_provider_fetch (
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex', 'cursor')),
  account_id TEXT NOT NULL,
  organization_id TEXT NOT NULL,
  requested_url TEXT NOT NULL,
  http_status INTEGER,
  request_error TEXT,
  raw_payload JSONB NOT NULL,
  request_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  success BOOLEAN NOT NULL DEFAULT false,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS usage_metric_snapshot (
  id BIGSERIAL PRIMARY KEY,
  provider_fetch_id BIGINT NOT NULL REFERENCES usage_provider_fetch (id) ON DELETE CASCADE,
  provider TEXT NOT NULL CHECK (provider IN ('claude', 'codex', 'cursor')),
  metric_key TEXT NOT NULL,
  provider_metric_key TEXT NOT NULL DEFAULT '',
  metric_path TEXT NOT NULL,
  metric_scope TEXT NOT NULL DEFAULT '',
  metric_label TEXT NOT NULL,
  percent INTEGER NOT NULL CHECK (percent >= 0 AND percent <= 100),
  value_num DOUBLE PRECISION,
  value_text TEXT NOT NULL,
  note TEXT NOT NULL DEFAULT '',
  max_value INTEGER NOT NULL DEFAULT 100,
  window_start TIMESTAMPTZ,
  window_end TIMESTAMPTZ,
  reset_at TIMESTAMPTZ,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE usage_metric_snapshot
  ADD COLUMN IF NOT EXISTS provider_metric_key TEXT NOT NULL DEFAULT '';
ALTER TABLE usage_metric_snapshot
  ADD COLUMN IF NOT EXISTS metric_path TEXT;
ALTER TABLE usage_metric_snapshot
  ADD COLUMN IF NOT EXISTS metric_scope TEXT NOT NULL DEFAULT '';
ALTER TABLE usage_metric_snapshot
  ADD COLUMN IF NOT EXISTS value_num DOUBLE PRECISION;
ALTER TABLE usage_metric_snapshot
  ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ;
ALTER TABLE usage_metric_snapshot
  ADD COLUMN IF NOT EXISTS window_end TIMESTAMPTZ;

UPDATE usage_metric_snapshot
SET metric_path = '/' || metric_key
WHERE metric_path IS NULL OR metric_path = '';

UPDATE usage_metric_snapshot
SET provider_metric_key = metric_key
WHERE provider_metric_key IS NULL OR provider_metric_key = '';

ALTER TABLE usage_metric_snapshot
  ALTER COLUMN metric_path SET NOT NULL;

DROP INDEX IF EXISTS usage_metric_snapshot_provider_fetch_metric_key_uq;
CREATE UNIQUE INDEX IF NOT EXISTS usage_metric_snapshot_provider_fetch_metric_path_uq
  ON usage_metric_snapshot (provider_fetch_id, metric_path);

CREATE INDEX IF NOT EXISTS usage_provider_fetch_provider_fetched_at_idx
  ON usage_provider_fetch (provider, fetched_at DESC);

CREATE INDEX IF NOT EXISTS usage_metric_snapshot_fetch_created_idx
  ON usage_metric_snapshot (provider_fetch_id);

CREATE INDEX IF NOT EXISTS usage_metric_snapshot_provider_metric_key_idx
  ON usage_metric_snapshot (provider, metric_key, created_at DESC);

CREATE TABLE IF NOT EXISTS cursor_usage_event (
  id BIGSERIAL PRIMARY KEY,
  provider_fetch_id BIGINT NOT NULL REFERENCES usage_provider_fetch (id) ON DELETE CASCADE,
  event_id TEXT NOT NULL,
  event_timestamp TIMESTAMPTZ NOT NULL,
  event_timestamp_ms BIGINT NOT NULL,
  cycle_start TIMESTAMPTZ,
  cycle_end TIMESTAMPTZ,
  page INTEGER,
  model TEXT,
  kind TEXT,
  charged_cents DOUBLE PRECISION,
  is_chargeable BOOLEAN,
  is_headless BOOLEAN,
  is_token_based_call BOOLEAN,
  raw_event JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS cursor_usage_event_event_id_uq
  ON cursor_usage_event (event_id);

CREATE INDEX IF NOT EXISTS cursor_usage_event_cycle_ts_idx
  ON cursor_usage_event (cycle_end, event_timestamp_ms DESC);

CREATE TABLE IF NOT EXISTS cursor_usage_sync_state (
  cycle_end TIMESTAMPTZ PRIMARY KEY,
  cycle_start TIMESTAMPTZ,
  synced_through_timestamp_ms BIGINT NOT NULL DEFAULT 0,
  total_usage_events_count INTEGER,
  last_page_fetched INTEGER NOT NULL DEFAULT 0,
  last_inserted_count INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
