-- Migration: 0003_gis_tables.up.sql
-- Purpose: Create GIS schema with staging and curated tables for OSM charging stations
-- Feature: 003-gis-engine
-- Created: 2026-06-22

-- Create gis schema
CREATE SCHEMA IF NOT EXISTS gis;

-- Create staging table for raw OSM data
CREATE TABLE gis.osm_charging_stations_temp (
    id VARCHAR(25) PRIMARY KEY, -- nanoid(12) with "STA-" prefix
    osm_id BIGINT NOT NULL, -- OpenStreetMap node/way ID
    osm_data JSONB NOT NULL, -- Raw OSM XML tags as JSON
    import_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE(osm_id)
);

-- Index for fast lookup by OSM ID
CREATE INDEX idx_staging_osm_id ON gis.osm_charging_stations_temp(osm_id);

-- Create curated table with normalized data
CREATE TABLE gis.osm_charging_stations (
    id VARCHAR(25) PRIMARY KEY, -- nanoid(12) with "STA-" prefix
    osm_id BIGINT, -- OpenStreetMap node/way ID
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    geom GEOMETRY(Point, 4326) NOT NULL,
    station_name VARCHAR(255),
    operator VARCHAR(255),
    address JSONB,
    amenity VARCHAR(100), -- from OSM tag
    power VARCHAR(50), -- charging power (kW)
    connector_types TEXT[], -- list of connector types
    is_available BOOLEAN NOT NULL DEFAULT TRUE,
    last_updated TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(osm_id),
    CONSTRAINT valid_coordinates CHECK (
        latitude BETWEEN -90 AND 90 AND
        longitude BETWEEN -180 AND 180
    ),
    CONSTRAINT valid_geom CHECK (
        ST_SRID(geom) = 4326 AND ST_GeometryType(geom) = 'POINT'
    )
);

-- GiST index for spatial queries (critical for performance)
CREATE INDEX idx_stations_geo ON gis.osm_charging_stations USING GiST (geom);

-- Additional indexes for common queries
CREATE INDEX idx_stations_amenity ON gis.osm_charging_stations (amenity);
CREATE INDEX idx_stations_available ON gis.osm_charging_stations (is_available);

-- Create trigger to automatically update last_updated
CREATE OR REPLACE FUNCTION gis.update_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_last_updated
    BEFORE UPDATE ON gis.osm_charging_stations
    FOR EACH ROW
    EXECUTE FUNCTION gis.update_last_updated();

-- Grant permissions (should be set up via database roles)
-- GRANT ALL PRIVILEGES ON SCHEMA gis TO borne_map_driver;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gis TO borne_map_driver;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gis TO borne_map_driver;
