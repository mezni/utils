# Database Schema Reference

**Phase**: 1 — Foundation
**Related Tasks**: TASK-15 through TASK-24
**Database**: ev_platform
**PostgreSQL**: 16 + PostGIS 3.4
**Last Updated**: 2026-06-07

---

## Overview

| Schema | Purpose | Phase |
|---|---|---|
| inventory | Partners, stations, chargers, availability | 1 |
| gis | OSM data, spatial indexes, station_locations | 1 |
| users | User accounts, profiles, favorites, reviews | 2 |
| analytics | Raw events, aggregates | 5 |

---

## Schema: `inventory`

### `inventory.partner`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | TEXT | PRIMARY KEY | PRT-... NanoID |
| name | TEXT | NOT NULL | Display name |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |

### `inventory.station`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | TEXT | PRIMARY KEY | STN-... NanoID |
| partner_id | TEXT | NOT NULL REFERENCES inventory.partner(id) | |
| name | TEXT | NOT NULL | |
| address | TEXT | | Optional |
| latitude | NUMERIC(10,7) | NOT NULL | WGS84 |
| longitude | NUMERIC(10,7) | NOT NULL | WGS84 |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | Auto-updated |

**Indexes**:
- `idx_station_partner_id` ON (partner_id)
- `idx_station_coords` ON (latitude, longitude)

### `inventory.charger`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | TEXT | PRIMARY KEY | CHG-... NanoID |
| station_id | TEXT | NOT NULL REFERENCES inventory.station(id) | |
| connector_type | TEXT | NOT NULL | type2, ccs, chademo, type1 |
| power_kw | NUMERIC(6,2) | NOT NULL | |
| status | TEXT | NOT NULL DEFAULT 'available' | available, in_use, maintenance, offline |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |

**Indexes**:
- `idx_charger_station_id` ON (station_id)

### `inventory.station_availability`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | TEXT | PRIMARY KEY | |
| station_id | TEXT | NOT NULL REFERENCES inventory.station(id) | |
| status | TEXT | NOT NULL | available, unavailable, partial |
| updated_by | TEXT | | User/partner reference |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |

**Indexes**:
- `idx_availability_station_id` ON (station_id)

---

## Schema: `gis`

### `gis.osm_nodes`

| Column | Type | Constraints |
|---|---|---|
| osm_id | BIGINT | PRIMARY KEY |
| tags | JSONB | |
| geom | GEOMETRY(Point, 4326) | |

**Index**: `idx_osm_nodes_geom` GIST (geom)

### `gis.osm_ways`

| Column | Type | Constraints |
|---|---|---|
| osm_id | BIGINT | PRIMARY KEY |
| tags | JSONB | |
| geom | GEOMETRY(LineString, 4326) | |

**Index**: `idx_osm_ways_geom` GIST (geom)

### `gis.roads`

| Column | Type | Constraints |
|---|---|---|
| id | BIGSERIAL | PRIMARY KEY |
| osm_id | BIGINT | |
| name | TEXT | |
| road_type | TEXT | |
| geom | GEOMETRY(LineString, 4326) | |

**Index**: `idx_roads_geom` GIST (geom)

### `gis.boundaries`

| Column | Type | Constraints |
|---|---|---|
| id | BIGSERIAL | PRIMARY KEY |
| osm_id | BIGINT | |
| name | TEXT | |
| admin_level | INT | |
| geom | GEOMETRY(MultiPolygon, 4326) | |

**Index**: `idx_boundaries_geom` GIST (geom)

### `gis.amenity_points`

| Column | Type | Constraints |
|---|---|---|
| id | BIGSERIAL | PRIMARY KEY |
| osm_id | BIGINT | |
| amenity_type | TEXT | |
| name | TEXT | |
| tags | JSONB | |
| geom | GEOMETRY(Point, 4326) | |

**Index**: `idx_amenity_points_geom` GIST (geom)

### `gis.station_locations`

| Column | Type | Constraints | Notes |
|---|---|---|---|
| station_id | TEXT | PRIMARY KEY | References inventory.station(id) |
| geom | GEOMETRY(Point, 4326) | | PostGIS point |
| snapped_road_id | BIGINT | | Nearest road from gis.roads |
| region_id | BIGINT | | Containing boundary from gis.boundaries |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |

**Index**: `idx_station_locations_geom` GIST (geom)

---

## Extensions

```sql
CREATE EXTENSION IF NOT EXISTS postgis;      -- Spatial SQL
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";  -- UUID functions (reserved)
CREATE EXTENSION IF NOT EXISTS pgcrypto;     -- Hashing/crypto
```

---

## Migration History

| Migration | Name | Status |
|---|---|---|
| 0001 | Extensions (postgis, uuid-ossp, pgcrypto) | 🔴 Planned |
| 0002 | Schemas (inventory, gis) | 🔴 Planned |
| 0003 | Inventory tables (partner, station, charger, station_availability) | 🔴 Planned |
| 0004 | Inventory indexes | 🔴 Planned |
| 0005 | GIS tables (osm_nodes, osm_ways, roads, boundaries, amenity_points, station_locations) | 🔴 Planned |
| 0006 | GIS GiST indexes | 🔴 Planned |

---

## Seed Data

| File | Records |
|---|---|
| `db/seeds/dev_partners.sql` | 3 partners |
| `db/seeds/dev_stations.sql` | 15 stations |
| `db/seeds/dev_chargers.sql` | 24 chargers |
