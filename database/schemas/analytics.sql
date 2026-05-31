-- Schema: analytics
-- Description: Clickstream analytics storage (replaces MongoDB)
-- Data type: JSONB for flexible schema

-- Tables
-- ======

-- connection_aggregates: Per-platform aggregate counts
--   platform: TEXT PRIMARY KEY
--   data: JSONB (total_connections_count, last_handshake_at, engine_version, ...)
--   created_at: TIMESTAMPTZ
--   updated_at: TIMESTAMPTZ

-- events: Raw clickstream events
--   id: UUID PRIMARY KEY
--   payload: JSONB (event_name, platform, session_id, timestamp, properties, ...)
--   ingested_at: TIMESTAMPTZ

-- Indexes
-- =======
-- GIN index on analytics.connection_aggregates.data
-- GIN index on analytics.events.payload
-- BTREE index on analytics.events.ingested_at DESC

-- Usage Examples
-- ==============

-- Upsert a connection aggregate (Rust service equivalent of MongoDB $inc + $set):
--   SELECT analytics.upsert_connection_aggregate(
--     'mobile_app',
--     '{"total_connections_count": 1, "last_handshake_at": "2026-05-31T12:00:00Z", "engine_version": "1.0.0"}'::jsonb
--   );

-- Read aggregates:
--   SELECT platform, data FROM analytics.connection_aggregates;

-- Insert raw event:
--   INSERT INTO analytics.events (payload)
--   VALUES ('{"event_name": "page.viewed", "platform": "web", "session_id": "abc", "timestamp": "2026-05-31T12:00:00Z"}'::jsonb);

-- Query events by ingested time:
--   SELECT payload FROM analytics.events ORDER BY ingested_at DESC LIMIT 100;
