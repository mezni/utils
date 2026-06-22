-- Migrations for driver-service: GIS schema
-- Purpose: Store OSM staging data and curated spatial truth

-- Create gis schema
CREATE SCHEMA IF NOT EXISTS gis;

-- OSM charging stations temporary table (staging)
CREATE TABLE IF NOT EXISTS gis.osm_charging_stations_temp (
    osm_id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    address TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    tags JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Curated charging stations table (GIS spatial truth)
CREATE TABLE IF NOT EXISTS gis.osm_charging_stations (
    osm_id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    address TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    status VARCHAR(50) NOT NULL,
    tags JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- GIST index for spatial queries
CREATE INDEX idx_gis_osm_location ON gis.osm_charging_stations USING GIST (location);

-- GiST index for spatial search (simplified as point for now)
CREATE INDEX idx_gis_osm_point ON gis.osm_charging_stations (longitude, latitude);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA gis TO bornemap_driver;
GRANT ALL PRIVILEGES ON TABLE gis.osm_charging_stations_temp TO bornemap_driver;
GRANT ALL PRIVILEGES ON TABLE gis.osm_charging_stations TO bornemap_driver;
GRANT USAGE ON SCHEMA gis TO bornemap_admin, bornemap_analytics_reader;
GRANT SELECT ON TABLE gis.osm_charging_stations TO bornemap_admin, bornemap_analytics_reader;
