-- Migration: 0004_materialized_views.up.sql
-- Purpose: Create materialized views for query optimization
-- Feature: 003-gis-engine
-- Created: 2026-06-22

-- Create materialized view for geo queries
CREATE MATERIALIZED VIEW gis.mv_stations_geo AS
SELECT
    id,
    station_name,
    latitude,
    longitude,
    amenity,
    power,
    connector_types,
    is_available
FROM gis.osm_charging_stations
WHERE is_available = TRUE;

-- Create unique index on materialized view
CREATE UNIQUE INDEX idx_mv_stations_geo_id ON gis.mv_stations_geo(id);

-- Create materialized view for analytics
CREATE MATERIALIZED VIEW gis.mv_stations_summary AS
SELECT
    amenity,
    COUNT(*) AS station_count,
    AVG(CAST(power AS FLOAT)) AS avg_power,
    MIN(power) AS min_power,
    MAX(power) AS max_power,
    ARRAY_AGG(DISTINCT connector_type) AS connector_types
FROM gis.osm_charging_stations
GROUP BY amenity;

-- Create unique index on materialized view
CREATE UNIQUE INDEX idx_mv_stations_summary_amenity ON gis.mv_stations_summary(amenity);

-- Create function to refresh materialized views
CREATE OR REPLACE FUNCTION gis.refresh_gis_materialized_views()
RETURNS void AS $$
BEGIN
    -- Refresh mv_stations_geo
    REFRESH MATERIALIZED VIEW CONCURRENTLY gis.mv_stations_geo;

    -- Refresh mv_stations_summary
    REFRESH MATERIALIZED VIEW CONCURRENTLY gis.mv_stations_summary;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions on views
-- GRANT SELECT ON gis.mv_stations_geo TO borne_map_driver;
-- GRANT SELECT ON gis.mv_stations_summary TO borne_map_driver;
