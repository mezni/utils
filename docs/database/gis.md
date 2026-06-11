# GIS — Spatial Data Layer

**Schema:** `gis` (read-only)
**Engine:** PostgreSQL 16 + PostGIS

---

## Purpose

Read-only spatial reference data derived from OpenStreetMap for context-aware station discovery.

---

## Tables

```
gis.planet_osm_point     — POIs, landmarks, addresses
gis.planet_osm_line      — roads, paths, waterways
gis.planet_osm_polygon   — buildings, land use, areas
gis.planet_osm_roads     — road network (simplified)
```

---

## Spatial Indexes

```sql
CREATE INDEX idx_point_geom ON gis.planet_osm_point  USING GIST (way);
CREATE INDEX idx_line_geom  ON gis.planet_osm_line   USING GIST (way);
CREATE INDEX idx_poly_geom  ON gis.planet_osm_polygon USING GIST (way);
CREATE INDEX idx_roads_geom ON gis.planet_osm_roads   USING GIST (way);
```

---

## Rules

- **READ-ONLY:** No service may write to GIS tables
- **Source:** OSM data imported via osm2pgsql
- **Access:** driver-service only (read queries)
- **Relationship:** No FK relationship to inventory schema

---

## Usage

- Station context (nearby POIs, road access)
- Map background enrichment
- Address reverse geocoding (future)
