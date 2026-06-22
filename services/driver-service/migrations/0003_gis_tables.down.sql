-- Migration: 0003_gis_tables.down.sql
-- Purpose: Drop GIS schema and tables
-- Feature: 003-gis-engine
-- Created: 2026-06-22

-- Drop materialized views first (if they exist)
DROP MATERIALIZED VIEW IF EXISTS gis.mv_stations_summary;
DROP MATERIALIZED VIEW IF EXISTS gis.mv_stations_geo;

-- Drop tables
DROP TABLE IF EXISTS gis.osm_charging_stations CASCADE;
DROP TABLE IF EXISTS gis.osm_charging_stations_temp CASCADE;

-- Drop schema
DROP SCHEMA IF EXISTS gis CASCADE;
