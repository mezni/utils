-- Migration 0005: GIS Tables
-- Purpose: Create spatial tables for OSM data and station locations
-- Author: BorneMap Development Team
-- Date: 2026-06-07

-- OSM nodes table: Individual OSM nodes (points of interest, intersections)
CREATE TABLE IF NOT EXISTS gis.osm_nodes (
    osm_id BIGINT PRIMARY KEY,
    tags JSONB,
    geom GEOMETRY(Point,4326)
);

-- OSM ways table: OSM ways (linear features, roads, boundaries)
CREATE TABLE IF NOT EXISTS gis.osm_ways (
    osm_id BIGINT PRIMARY KEY,
    tags JSONB,
    geom GEOMETRY(LineString,4326)
);

-- Roads table: Extracted road network from OSM
CREATE TABLE IF NOT EXISTS gis.roads (
    id BIGSERIAL PRIMARY KEY,
    osm_id BIGINT,
    name TEXT,
    road_type TEXT,
    geom GEOMETRY(LineString,4326)
);

-- Boundaries table: Administrative boundaries from OSM
CREATE TABLE IF NOT EXISTS gis.boundaries (
    id BIGSERIAL PRIMARY KEY,
    osm_id BIGINT,
    name TEXT,
    admin_level INT,
    geom GEOMETRY(MultiPolygon,4326)
);

-- Amenity points table: POIs from OSM (amenities, services, landmarks)
CREATE TABLE IF NOT EXISTS gis.amenity_points (
    id BIGSERIAL PRIMARY KEY,
    osm_id BIGINT,
    amenity_type TEXT,
    name TEXT,
    tags JSONB,
    geom GEOMETRY(Point,4326)
);

-- Station locations table: Derived station geometries for spatial queries
CREATE TABLE IF NOT EXISTS gis.station_locations (
    station_id TEXT PRIMARY KEY REFERENCES inventory.station(id),
    geom GEOMETRY(Point,4326),
    snapped_road_id BIGINT,
    region_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
