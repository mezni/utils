CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Analytics schema (replaces standalone MongoDB for clickstream storage)
CREATE SCHEMA IF NOT EXISTS analytics;

-- Per-platform connection aggregates (replaces MongoDB connection_aggregates collection)
-- JSONB column `data` stores: { total_connections_count, last_handshake_at, engine_version, ... }
CREATE TABLE IF NOT EXISTS analytics.connection_aggregates (
    platform    TEXT PRIMARY KEY,
    data        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_connection_aggregates_gin
    ON analytics.connection_aggregates USING GIN (data);

-- Raw clickstream events ingestion table
CREATE TABLE IF NOT EXISTS analytics.events (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payload     JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_gin
    ON analytics.events USING GIN (payload);

CREATE INDEX IF NOT EXISTS idx_events_ingested_at
    ON analytics.events (ingested_at DESC);
