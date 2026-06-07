-- Migration 0006: GIS GiST Indexes
-- Purpose: Create GiST indexes on geometry columns for efficient spatial queries
-- Author: BorneMap Development Team
-- Date: 2026-06-07

-- GiST indexes on all OSM nodes for spatial queries
CREATE INDEX IF NOT EXISTS gis.idx_osm_nodes_geom ON gis.osm_nodes USING GIST (geom);

-- GiST indexes on all OSM ways for spatial queries
CREATE INDEX IF NOT EXISTS gis.idx_osm_ways_geom ON gis.osm_ways USING GIST (geom);

-- GiST indexes on roads for spatial queries
CREATE INDEX IF NOT EXISTS gis.idx_roads_geom ON gis.roads USING GIST (geom);

-- GiST indexes on boundaries for spatial queries
CREATE INDEX IF NOT EXISTS gis.idx_boundaries_geom ON gis.boundaries USING GIST (geom);

-- GiST indexes on amenity points for spatial queries
CREATE INDEX IF NOT EXISTS gis.idx_amenity_points_geom ON gis.amenity_points USING GIST (geom);

-- GiST indexes on station locations for spatial queries
CREATE INDEX IF NOT EXISTS gis.idx_station_locations_geom ON gis.station_locations USING GIST (geom);
