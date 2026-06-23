-- Migration: 0007_enable_pg_trgm.down.sql
-- Purpose: Remove pg_trgm extension and trigram indexes

-- Drop trigram indexes first
DROP INDEX IF EXISTS gis.idx_stations_address_trgm;
DROP INDEX IF EXISTS gis.idx_stations_name_trgm;

-- Drop the extension (only if no other objects depend on it)
DROP EXTENSION IF EXISTS pg_trgm;
