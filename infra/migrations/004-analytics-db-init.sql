-- 004-analytics-db-init.sql
-- Target: analytics_db
-- Purpose: Create raw_events table + append-only rules
-- Idempotent: YES (IF NOT EXISTS guards)

CREATE TABLE IF NOT EXISTS raw_events (
    id          BIGSERIAL       PRIMARY KEY,
    event_type  VARCHAR(50)     NOT NULL,
    session_id  VARCHAR(50)     NOT NULL,
    user_id     VARCHAR(50),
    payload     JSONB           NOT NULL,
    occurred_at TIMESTAMP       NOT NULL,
    ingested_at TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    client_ip   INET
);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_type
    ON raw_events (event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_occurred_at
    ON raw_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_session_id
    ON raw_events (session_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_user_id
    ON raw_events (user_id) WHERE user_id IS NOT NULL;

CREATE OR REPLACE RULE raw_events_no_update AS
    ON UPDATE TO raw_events DO INSTEAD NOTHING;

CREATE OR REPLACE RULE raw_events_no_delete AS
    ON DELETE TO raw_events DO INSTEAD NOTHING;
