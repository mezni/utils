-- Migration: 0004_materialized_views.down.sql
-- Purpose: Drop materialized views
-- Feature: 003-gis-engine
-- Created: 2026-06-22

-- Drop materialized views
DROP MATERIALIZED VIEW IF EXISTS gis.mv_stations_summary;
DROP MATERIALIZED VIEW IF EXISTS gis.mv_stations_geo;

-- Drop refresh function
DROP FUNCTION IF EXISTS gis.refresh_gis_materialized_views();
