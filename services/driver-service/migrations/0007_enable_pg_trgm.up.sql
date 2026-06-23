-- Migration: 0007_enable_pg_trgm.up.sql
-- Purpose: Enable pg_trgm extension and add trigram indexes for fuzzy station search
-- Feature: 006-driver-experience-layer
-- Created: 2026-06-22

-- Enable pg_trgm extension (idempotent)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- GiST trigram index on station_name for fuzzy name matching
CREATE INDEX IF NOT EXISTS idx_stations_name_trgm
    ON gis.osm_charging_stations USING GiST (station_name gist_trgm_ops);

-- GiST trigram index on address text for fuzzy address matching
CREATE INDEX IF NOT EXISTS idx_stations_address_trgm
    ON gis.osm_charging_stations USING GiST
    ((COALESCE(address->>'street', '') || ' ' || COALESCE(address->>'city', '') || ' ' || COALESCE(address->>'country', '')) gist_trgm_ops);
