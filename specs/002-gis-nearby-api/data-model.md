# Data Model — GIS Data & Nearby Discovery

**Feature**: GIS Data & Nearby Discovery — MVP-2 Sprint 2.0
**Last Updated**: 2026-06-16

## Overview

This document defines the data model for the GIS data layer, including station entities, charger entities, and import tracking. The model supports spatial queries, status tracking, and pagination for nearby discovery.

## Entity: Station (inventory.station)

### Purpose

Stores charging station information with spatial coordinates for geolocation-based queries.

### Fields

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `id` | VARCHAR(32) | PK, NOT NULL | Unique identifier (e.g., "sta_xxxxx") |
| `name` | VARCHAR(255) | NOT NULL | Station name from OSM |
| `visibility` | VARCHAR(50) | NOT NULL | 'commercial', 'private_home', 'all' |
| `status` | VARCHAR(50) | NOT NULL, DEFAULT 'draft' | 'draft', 'active', 'inactive', 'closed' |
| `location` | GEOGRAPHY(POINT, 4326) | NOT NULL | Lat/lon coordinates (Web Mercator) |
| `address` | TEXT | NULL | Street address |
| `city` | VARCHAR(100) | NULL | City name |
| `created_at` | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Record creation timestamp |
| `updated_at` | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Last update timestamp |
| `deleted_at` | TIMESTAMPTZ | NULL | Soft delete marker |

### Indexes

```sql
CREATE INDEX idx_station_location ON inventory.station USING GIST (location);
CREATE INDEX idx_station_status ON inventory.station (status);
CREATE INDEX idx_station_visibility ON inventory.station (visibility);
CREATE INDEX idx_station_city ON inventory.station (city);
```

### Relationships

- **One station** → **Many chargers** (via `inventory.charger.station_id`)
- **One station** → **One partner** (via partner_id, not in MVP-2)
- **No cycles** (self-referential constraints: none)

### State Transitions

```
draft → active (import complete)
active → inactive (manual or automated)
active → closed (manual)
any → draft (rollback)
any → deleted (soft delete via deleted_at)
```

### Validation Rules

1. **id**: 4-8 characters, alphanumeric + underscore, must be unique
2. **name**: 1-255 characters, not empty
3. **visibility**: Must be one of ['commercial', 'private_home', 'all']
4. **status**: Must be one of ['draft', 'active', 'inactive', 'closed']
5. **location**: Must be within -90 to 90 (lat), -180 to 180 (lon)
6. **city**: 1-100 characters, nullable
7. **address**: 0-10000 characters, nullable

### Example Data

```json
{
  "id": "sta_abc123",
  "name": "Station Menzah",
  "visibility": "commercial",
  "status": "active",
  "location": {
    "type": "Point",
    "coordinates": [10.19, 36.84]
  },
  "address": "Rue des Jasmins, Menzah",
  "city": "Tunis",
  "created_at": "2026-06-16T15:00:00Z",
  "updated_at": "2026-06-16T15:30:00Z"
}
```

## Entity: Charger (inventory.charger)

### Purpose

Stores individual charging connector details within a station.

### Fields

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `id` | VARCHAR(32) | PK, NOT NULL | Unique identifier (e.g., "chg_xyz789") |
| `station_id` | VARCHAR(32) | FK → inventory.station(id), NOT NULL | Reference to parent station |
| `connector_type` | VARCHAR(50) | NOT NULL | 'type1', 'type2', 'ccs', 'chademo', 'other' |
| `connector_count` | INTEGER | NOT NULL, DEFAULT 1 | Number of identical connectors |
| `power_kw` | DECIMAL(5,2) | NOT NULL | Power rating (0-999.99 kW) |
| `status` | VARCHAR(50) | NOT NULL, DEFAULT 'available' | 'available', 'occupied', 'unavailable' |
| `created_at` | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Record creation timestamp |
| `updated_at` | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Last update timestamp |
| `deleted_at` | TIMESTAMPTZ | NULL | Soft delete marker |

### Indexes

```sql
CREATE INDEX idx_charger_station ON inventory.charger (station_id);
CREATE INDEX idx_charger_status ON inventory.charger (status);
CREATE INDEX idx_charger_connector_type ON inventory.charger (connector_type);
```

### Relationships

- **One station** → **Many chargers** (1:N via station_id)
- **One charger** → **None** (leaf entity)
- **No cycles**

### State Transitions

```
available → occupied (user plugs in)
occupied → available (user unplugs)
available → unavailable (maintenance)
any → deleted (soft delete)
```

### Validation Rules

1. **id**: 4-8 characters, alphanumeric + underscore
2. **station_id**: Must reference existing station.id
3. **connector_type**: Must be one of ['type1', 'type2', 'ccs', 'chademo', 'other']
4. **connector_count**: 1-100 connectors per charger record
5. **power_kw**: 0.1-999.9 kW
6. **status**: Must be one of ['available', 'occupied', 'unavailable']

### Example Data

```json
{
  "id": "chg_xyz789",
  "station_id": "sta_abc123",
  "connector_type": "type2",
  "connector_count": 2,
  "power_kw": 22.0,
  "status": "available",
  "created_at": "2026-06-16T15:00:00Z",
  "updated_at": "2026-06-16T15:30:00Z"
}
```

## Entity: Import Log (gis.import_log)

### Purpose

Tracks import process execution, status, and statistics.

### Fields

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `id` | SERIAL | PK, auto-increment | Auto-increment ID |
| `status` | VARCHAR(50) | NOT NULL | 'success', 'failed' |
| `start_time` | TIMESTAMPTZ | NOT NULL | Import started timestamp |
| `end_time` | TIMESTAMPTZ | NULL | Import completed timestamp |
| `bbox` | JSONB | NOT NULL | Bounding box used for import |
| `stations_imported` | INTEGER | NOT NULL, DEFAULT 0 | Count of stations processed |
| `stations_updated` | INTEGER | NOT NULL, DEFAULT 0 | Count of stations updated |
| `stations_failed` | INTEGER | NOT NULL, DEFAULT 0 | Count of stations that failed |
| `error_message` | TEXT | NULL | Error details if failed |
| `created_at` | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Log entry timestamp |

### Indexes

```sql
CREATE INDEX idx_import_log_time ON gis.import_log (start_time DESC);
CREATE INDEX idx_import_log_status ON gis.import_log (status);
```

### Relationships

- **One import** → **Many stations** (through import_log)
- **No reverse relationships**
- **No cycles**

### Validation Rules

1. **status**: Must be one of ['success', 'failed']
2. **bbox**: Must contain min_lat, min_lon, max_lat, max_lon
3. **stations_imported**: Must be >= 0
4. **stations_updated**: Must be >= 0
5. **stations_failed**: Must be >= 0
6. **stations_imported** ≥ **stations_updated** + **stations_failed**

### Example Data

```json
{
  "id": 1,
  "status": "success",
  "start_time": "2026-06-16T15:00:00Z",
  "end_time": "2026-06-16T15:45:30Z",
  "bbox": {
    "min_lat": 30.0,
    "min_lon": 7.5,
    "max_lat": 37.5,
    "max_lon": 11.6
  },
  "stations_imported": 1250,
  "stations_updated": 340,
  "stations_failed": 0,
  "error_message": null,
  "created_at": "2026-06-16T15:00:00Z"
}
```

## Entity: OSM Station (gis.osm_station) - Reference Only

### Purpose

Stores raw OpenStreetMap data as a reference source (not used in MVP-2 queries).

### Fields

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `id` | BIGINT | PK | OSM node/way/relation ID |
| `osm_type` | VARCHAR(10) | NOT NULL | 'node', 'way', 'relation' |
| `name` | VARCHAR(255) | NULL | OSM name tag |
| `operator` | VARCHAR(255) | NULL | OSM operator tag |
| `network` | VARCHAR(255) | NULL | OSM network tag |
| `location` | GEOGRAPHY(POINT, 4326) | NOT NULL | Centroid coordinates |
| `address` | TEXT | NULL | Derived from OSM address tags |
| `city` | VARCHAR(100) | NULL | OSM addr:city tag |
| `opening_hours` | VARCHAR(255) | NULL | OSM opening_hours tag |
| `socket_types` | TEXT[] | NULL | Array of connector tags |
| `raw_tags` | JSONB | NULL | Full OSM tag set |
| `imported_at` | TIMESTAMPTZ | NOT NULL | Last import timestamp |

### Indexes

```sql
CREATE INDEX idx_osm_station_location ON gis.osm_station USING GIST (location);
CREATE INDEX idx_osm_station_city ON gis.osm_station (city);
CREATE INDEX idx_osm_station_imported ON gis.osm_station (imported_at DESC);
```

### Notes

- This entity is reference data only
- Queries use `inventory.station`, not `gis.osm_station`
- Used for data validation and reconciliation
- Can be extended in MVP-3+ for analysis

## Spatial Function: gis.nearby()

### Purpose

Return active stations within a radius from a given coordinate, ordered by distance ascending.

### Signature

```sql
CREATE OR REPLACE FUNCTION gis.nearby(
  p_lat        DOUBLE PRECISION,
  p_lon        DOUBLE PRECISION,
  p_radius_m   INTEGER DEFAULT 5000,
  p_max        INTEGER DEFAULT 50
) RETURNS TABLE (
  id           VARCHAR(32),
  name         VARCHAR(255),
  lat          DOUBLE PRECISION,
  lon          DOUBLE PRECISION,
  address      TEXT,
  city         VARCHAR(100),
  visibility   VARCHAR(50),
  status       VARCHAR(50),
  distance_m   DOUBLE PRECISION
) LANGUAGE sql
STABLE
AS $$
  SELECT
    s.id,
    s.name,
    ST_Y(s.location::geometry)  AS lat,
    ST_X(s.location::geometry)  AS lon,
    s.address,
    s.city,
    s.visibility,
    s.status,
    ST_Distance(
      s.location,
      ST_GeogFromText('SRID=4326;POINT(' || p_lon || ' ' || p_lat || ')')
    ) AS distance_m
  FROM inventory.station s
  WHERE
    s.deleted_at IS NULL
    AND s.status = 'active'
    AND ST_DWithin(
      s.location,
      ST_GeogFromText('SRID=4326;POINT(' || p_lon || ' ' || p_lat || ')'),
      p_radius_m
    )
  ORDER BY distance_m ASC
  LIMIT p_max;
$$;
```

### Parameters

- `p_lat`: Latitude (-90 to 90)
- `p_lon`: Longitude (-180 to 180)
- `p_radius_m`: Radius in meters (1–50000)
- `p_max`: Maximum number of results (1–100)

### Returns

Table with columns: `id`, `name`, `lat`, `lon`, `address`, `city`, `visibility`, `status`, `distance_m`

### Usage Example

```sql
SELECT * FROM gis.nearby(36.8, 10.18, 5000, 10);
```

Returns 10 active stations within 5km of (36.8, 10.18), ordered by distance.

## Database Schema DDL

```sql
-- Inventory schema
CREATE SCHEMA IF NOT EXISTS inventory;

-- Station table
CREATE TABLE inventory.station (
  id VARCHAR(32) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  visibility VARCHAR(50) NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'draft',
  location GEOGRAPHY(POINT, 4326) NOT NULL,
  address TEXT,
  city VARCHAR(100),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at TIMESTAMPTZ,
  CONSTRAINT station_visibility_check CHECK (visibility IN ('commercial', 'private_home', 'all')),
  CONSTRAINT station_status_check CHECK (status IN ('draft', 'active', 'inactive', 'closed'))
);

-- Charger table
CREATE TABLE inventory.charger (
  id VARCHAR(32) PRIMARY KEY,
  station_id VARCHAR(32) NOT NULL REFERENCES inventory.station(id) ON DELETE CASCADE,
  connector_type VARCHAR(50) NOT NULL,
  connector_count INTEGER NOT NULL DEFAULT 1 CHECK (connector_count >= 1 AND connector_count <= 100),
  power_kw DECIMAL(5,2) NOT NULL CHECK (power_kw >= 0 AND power_kw <= 999.99),
  status VARCHAR(50) NOT NULL DEFAULT 'available',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at TIMESTAMPTZ,
  CONSTRAINT charger_connector_type_check CHECK (connector_type IN ('type1', 'type2', 'ccs', 'chademo', 'other')),
  CONSTRAINT charger_status_check CHECK (status IN ('available', 'occupied', 'unavailable'))
);

-- Import log table
CREATE TABLE gis.import_log (
  id SERIAL PRIMARY KEY,
  status VARCHAR(50) NOT NULL,
  start_time TIMESTAMPTZ NOT NULL,
  end_time TIMESTAMPTZ,
  bbox JSONB NOT NULL,
  stations_imported INTEGER NOT NULL DEFAULT 0,
  stations_updated INTEGER NOT NULL DEFAULT 0,
  stations_failed INTEGER NOT NULL DEFAULT 0,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT import_log_status_check CHECK (status IN ('success', 'failed'))
);

-- Spatial indexes
CREATE INDEX idx_station_location ON inventory.station USING GIST (location);
CREATE INDEX idx_station_status ON inventory.station (status);
CREATE INDEX idx_station_visibility ON inventory.station (visibility);
CREATE INDEX idx_charger_station ON inventory.charger (station_id);
CREATE INDEX idx_charger_status ON inventory.charger (status);
CREATE INDEX idx_charger_connector_type ON inventory.charger (connector_type);
CREATE INDEX idx_import_log_time ON gis.import_log (start_time DESC);
CREATE INDEX idx_import_log_status ON gis.import_log (status);

-- Text search indexes
CREATE INDEX idx_station_name_gin ON inventory.station USING GIN (to_tsvector('english', name));
CREATE INDEX idx_station_city_gin ON inventory.station USING GIN (to_tsvector('english', city));
```

## Data Migration & Import

### Initial Setup

```sql
-- Create inventory schema if not exists
CREATE SCHEMA IF NOT EXISTS inventory;

-- Create tables
-- (from DDL above)

-- Add spatial extensions
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create indexes
-- (from DDL above)
```

### Import Process

1. Fetch OSM data via API
2. Transform to station/charger entities
3. Insert/update in database using `ON CONFLICT (id) DO UPDATE`
4. Log import statistics
5. Handle failures with retry logic
