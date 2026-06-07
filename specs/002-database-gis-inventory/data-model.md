# Data Model: Database — GIS and Inventory Schemas

## Overview

Two PostgreSQL schemas with 10 tables total. The inventory schema holds business entities (partners, stations, chargers, availability). The gis schema holds spatial reference data and derived station geometries.

## Schema: inventory

### partner

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| id | TEXT | PRIMARY KEY | PRT-... NanoID |
| name | TEXT | NOT NULL | Display name |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |

### station

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| id | TEXT | PRIMARY KEY | STN-... NanoID |
| partner_id | TEXT | NOT NULL REFERENCES partner(id) | |
| name | TEXT | NOT NULL | |
| address | TEXT | | Optional |
| latitude | NUMERIC(10,7) | NOT NULL | WGS84 |
| longitude | NUMERIC(10,7) | NOT NULL | WGS84 |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |

**Indexes**: idx_station_partner_id ON (partner_id), idx_station_coords ON (latitude, longitude)

### charger

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| id | TEXT | PRIMARY KEY | CHG-... NanoID |
| station_id | TEXT | NOT NULL REFERENCES station(id) | |
| connector_type | TEXT | NOT NULL | Type2, Type2Combo, Chademo, CCS, Schuko, Wall |
| power_kw | NUMERIC(6,2) | NOT NULL | |
| status | TEXT | NOT NULL DEFAULT 'Available' | Available, Charging, Offline, Maintenance, Reserved, Unknown |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |

**Indexes**: idx_charger_station_id ON (station_id)

### station_availability

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| id | TEXT | PRIMARY KEY | NanoID |
| station_id | TEXT | NOT NULL REFERENCES station(id) | |
| status | TEXT | NOT NULL | Available, Unavailable, Partial |
| updated_by | TEXT | | User/partner reference |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |

**Indexes**: idx_availability_station_id ON (station_id)

## Schema: gis

### osm_nodes

| Column | Type | Constraints |
|--------|------|-------------|
| osm_id | BIGINT | PRIMARY KEY |
| tags | JSONB | |
| geom | GEOMETRY(Point, 4326) | |

### osm_ways

| Column | Type | Constraints |
|--------|------|-------------|
| osm_id | BIGINT | PRIMARY KEY |
| tags | JSONB | |
| geom | GEOMETRY(LineString, 4326) | |

### roads

| Column | Type | Constraints |
|--------|------|-------------|
| id | BIGSERIAL | PRIMARY KEY |
| osm_id | BIGINT | |
| name | TEXT | |
| road_type | TEXT | |
| geom | GEOMETRY(LineString, 4326) | |

### boundaries

| Column | Type | Constraints |
|--------|------|-------------|
| id | BIGSERIAL | PRIMARY KEY |
| osm_id | BIGINT | |
| name | TEXT | |
| admin_level | INT | |
| geom | GEOMETRY(MultiPolygon, 4326) | |

### amenity_points

| Column | Type | Constraints |
|--------|------|-------------|
| id | BIGSERIAL | PRIMARY KEY |
| osm_id | BIGINT | |
| amenity_type | TEXT | |
| name | TEXT | |
| tags | JSONB | |
| geom | GEOMETRY(Point, 4326) | |

### station_locations

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| station_id | TEXT | PRIMARY KEY REFERENCES inventory.station(id) | |
| geom | GEOMETRY(Point, 4326) | | Derived from inventory.station coordinates |
| snapped_road_id | BIGINT | | Nearest road (deferred to Sprint 6.x) |
| region_id | BIGINT | | Containing boundary (deferred to Sprint 6.x) |
| updated_at | TIMESTAMPTZ | NOT NULL DEFAULT now() | |

## Relationships

```
inventory.partner  1──N inventory.station
inventory.station  1──N inventory.charger
inventory.station  1──N inventory.station_availability (audit log)
inventory.station  1──1 gis.station_locations (derived, FK only)
gis.station_locations N──1 gis.roads (snapped_road_id, deferred)
gis.station_locations N──1 gis.boundaries (region_id, deferred)
```

## Validation Rules

- Station latitude and longitude are required (NOT NULL)
- A station must belong to an existing partner
- A charger must belong to an existing station
- Charger connector_type values must match ev-core ConnectorType enum
- Charger status values must match ev-core ChargerStatus enum
- Station availability status values: Available, Unavailable, Partial
- Deleting a partner requires no stations exist
- Deleting a station requires no chargers exist
- Migrations are never edited after commit
