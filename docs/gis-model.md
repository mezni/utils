# GIS Model — BorneMap Geospatial Layer

## Overview

The GIS layer provides high-performance geospatial queries for station discovery. All spatial logic lives in PostGIS functions — Rust services call them via SQLx and map results directly to JSON responses.

## Authority Chain

```
Source of Truth:    inventory.stations
        ↓
Replication:        PL/pgSQL trigger (AFTER INSERT OR UPDATE)
        ↓
Read Cache:         gis.osm_stations (with GIST spatial index)
        ↓
Query Function:     gis.get_nearby_stations()
        ↓
API:                driver-service /api/v1/stations/nearby
        ↓
Client:             mobile-app map markers
```

## Core Function

### `gis.get_nearby_stations()`

```sql
gis.get_nearby_stations(
    driver_longitude DOUBLE PRECISION,
    driver_latitude  DOUBLE PRECISION,
    search_radius_meters DOUBLE PRECISION DEFAULT 5000.0
) RETURNS TABLE (
    station_id        VARCHAR(64),
    station_name      VARCHAR(255),
    station_address   TEXT,
    distance_meters   DOUBLE PRECISION,
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    available_chargers JSONB
)
```

**Execution Plan**:
1. Geography cast for accurate spherical distance (`coordinates::geography`)
2. `ST_DWithin` uses GIST index for bounding-box pre-filter
3. `ST_Distance` for exact distance on pre-filtered rows
4. `jsonb_agg` with `jsonb_build_object` for charger aggregation
5. Results ordered by distance ascending

**Performance characteristics** (estimated):
- With GIST index: <50ms for 500 stations within 50km radius
- Without GIST index: >500ms scan
- Target: <150ms p95

## Spatial Indexing

```sql
CREATE INDEX idx_osm_stations_spatial ON gis.osm_stations USING GIST (coordinates);
```

- Index type: **GIST** (Generalized Search Tree)
- Column: `coordinates GEOMETRY(Point, 4326)`
- Powers: `ST_DWithin`, `ST_Distance`, `ST_Intersects`
- WGS84 coordinate system (SRID 4326) with geography cast for meter-accurate distance

## Geography vs Geometry

| Aspect | Geography (used) | Geometry |
|--------|-----------------|----------|
| Distance | Meters (accurate) | Degrees (deformed) |
| Index support | GIST (via cast) | GIST |
| Performance | Slightly slower | Faster |
| Use case | Our requirement (meters) | Planar calculations |

We cast to geography for `ST_DWithin` and `ST_Distance` to get accurate meter-based distances over Tunisia's latitude range (~30-38°N).

## Replication Trigger

### `gis.sync_inventory_station_to_gis_cache()`

Trigger function fired AFTER INSERT OR UPDATE on `inventory.stations`.

**Logic**:
```
IF NEW.is_live = FALSE:
    DELETE FROM gis.osm_stations WHERE id = NEW.id
    RETURN NEW

UPSERT gis.osm_stations:
    id = NEW.id
    name = NEW.name
    address = NEW.address
    coordinates = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)
    source = 'PLATFORM_SYNC'
    is_available = (NEW.availability = 'AVAILABLE')
    last_modified_at = NEW.updated_at
```

Security: `SECURITY DEFINER` — runs with owner privileges for cross-schema write access.

## OSM Import Path

For stations imported directly from OpenStreetMap (bypassing inventory):

```
import-tunisia-osm.sh → psql INSERT INTO gis.osm_stations (source = 'OSM_IMPORT')
```

These stations have `source = 'OSM_IMPORT'` and are NOT replicated back to inventory. They serve as initial seed data until admin-service is built.

## Tunisia Bounding Box

```
MIN_LON:  7.0000
MAX_LON: 12.0000
MIN_LAT: 30.0000
MAX_LAT: 38.0000
```

Both API-level (Rust geo-core) and database-level enforcement rejects queries outside these bounds.

## Future GIS Evolution (Post-MVP)

- `ST_ClosestPointOfApproach` for route-based search
- `ST_Within` for polygon-based area search (governorates)
- Materialized view for real-time availability aggregation
- Grid-based tile caching for high-traffic areas
