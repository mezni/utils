-- Migration 0001: Create ev-platform schema
-- Creates the schema and a schema_version tracking table for audit.

CREATE SCHEMA IF NOT EXISTS "ev-platform";

-- Track which migrations have been applied
CREATE TABLE IF NOT EXISTS "ev-platform".schema_version (
    version     INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by  TEXT NOT NULL DEFAULT CURRENT_USER,
    checksum    TEXT
);
