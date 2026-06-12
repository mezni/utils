-- 003-gis-schema.sql
-- Target: platform_db
-- Purpose: GIS tables (osm_region, osm_road) + spatial indexes
-- Idempotent: YES (IF NOT EXISTS guards)

CREATE TABLE IF NOT EXISTS gis.osm_region (
    id          BIGINT               PRIMARY KEY,
    name        VARCHAR(255),
    admin_level INTEGER,
    boundary    GEOMETRY(Polygon, 4326),
    created_at  TIMESTAMP            DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_osm_region_boundary_gist
    ON gis.osm_region USING GIST (boundary);

CREATE TABLE IF NOT EXISTS gis.osm_road (
    id           BIGINT                  PRIMARY KEY,
    name         VARCHAR(255),
    highway_type VARCHAR(50),
    geometry     GEOMETRY(LineString, 4326),
    created_at   TIMESTAMP               DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_osm_road_geometry_gist
    ON gis.osm_road USING GIST (geometry);
