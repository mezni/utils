# Data Model: GIS Engine Foundation

**Feature**: 003 - GIS Engine Foundation
**Version**: 1.0.0
**Date**: 2026-06-22

## Overview

This document defines the data entities and relationships for the GIS engine foundation, including OSM ingestion, spatial queries, and caching layer.

## Entity Relationship Diagram

```
[OSM Data Source]
        ↓
[osm_charging_stations_temp] (staging)
        ↓ (ETL)
[osm_charging_stations] (curated)
        ↓
[PostGIS Spatial Indexes]
        ↓
[Redis Spatial Cache] (geo:radius:lat:lon:radius)
        ↓
[Map Rendering API]
```

## Entities

### Entity 1: ChargingStationStaging

**Table**: `gis.osm_charging_stations_temp`

**Purpose**: Staging table for raw OSM data before ETL processing

**Schema**:
```sql
CREATE TABLE gis.osm_charging_stations_temp (
    id VARCHAR(25) PRIMARY KEY, -- nanoid(12) with "STA-" prefix
    osm_id BIGINT NOT NULL, -- OpenStreetMap node/way ID
    osm_data JSONB NOT NULL, -- Raw OSM XML as JSON
    import_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE(osm_id)
);

CREATE INDEX idx_staging_osm_id ON gis.osm_charging_stations_temp(osm_id);
```

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | VARCHAR(25) | YES | nanoid(12) with "STA-" prefix (e.g., "STA-abc123456789") |
| osm_id | BIGINT | YES | OpenStreetMap node/way ID (unique identifier) |
| osm_data | JSONB | YES | Raw OSM XML tags as JSON (preserves all OSM data) |
| import_timestamp | TIMESTAMP | YES | When this data was imported |
| processed | BOOLEAN | YES | Whether ETL has been run |

**Relationships**:
- `osm_id`: References OpenStreetMap node/way IDs (external reference, not a foreign key)
- One-to-many: One raw OSM record can produce multiple stations (if OSM has multiple connector types)

**Constraints**:
- `osm_id`: UNIQUE constraint (prevents duplicate OSM records)
- `osm_data`: JSONB (allows arbitrary OSM tags)
- `processed`: Default FALSE (only true after ETL runs)

**Index**:
- `idx_staging_osm_id` on `osm_id` (fast lookup by OSM ID)

---

### Entity 2: ChargingStation

**Table**: `gis.osm_charging_stations`

**Purpose**: Curated table with normalized station data

**Schema**:
```sql
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
    CONSTRAINT valid_geom CHECK (ST_SRID(geom) = 4326 AND ST_GeometryType(geom) = 'POINT')
);

-- GiST index for spatial queries
CREATE INDEX idx_stations_geo ON gis.osm_charging_stations USING GiST (geom);

-- Additional indexes for common queries
CREATE INDEX idx_stations_amenity ON gis.osm_charging_stations (amenity);
CREATE INDEX idx_stations_available ON gis.osm_charging_stations (is_available);
```

**Fields**:
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | VARCHAR(25) | YES | nanoid(12) with "STA-" prefix (unique station identifier) |
| osm_id | BIGINT | NO | OpenStreetMap node/way ID (nullable, for traceability) |
| latitude | DOUBLE PRECISION | YES | Latitude coordinate (WGS 84) |
| longitude | DOUBLE PRECISION | YES | Longitude coordinate (WGS 84) |
| geom | GEOMETRY(Point, 4326) | YES | PostGIS point geometry (SRID 4326) |
| station_name | VARCHAR(255) | NO | Station display name |
| operator | VARCHAR(255) | NO | Operator name |
| address | JSONB | NO | Address components (street, city, etc.) |
| amenity | VARCHAR(100) | NO | OSM amenity type (e.g., "charging_station") |
| power | VARCHAR(50) | NO | Charging power capacity (e.g., "50kW", "7kW") |
| connector_types | TEXT[] | NO | Array of connector types (e.g., ["Type 2", "CCS"]) |
| is_available | BOOLEAN | YES | Whether station is currently available |
| last_updated | TIMESTAMP | YES | Last time data was updated |
| created_at | TIMESTAMP | YES | When record was created |

**Relationships**:
- `osm_id`: One-to-one with `osm_charging_stations_temp.osm_id` (after ETL)
- No foreign keys to other tables (standalone entity)

**Constraints**:
- `id`: UNIQUE constraint (nanoid(12) with "STA-" prefix)
- `osm_id`: UNIQUE constraint (one curated record per OSM node/way)
- `latitude`: Valid range (-90 to 90)
- `longitude`: Valid range (-180 to 180)
- `geom`: Must be POINT with SRID 4326
- `amenity`: Must be valid amenity type (enum)
- `connector_types`: Must contain at least one valid type (validated by application)

**Indexes**:
- `idx_stations_geo`: GiST index for spatial queries (radius, bounding box, nearest)
- `idx_stations_amenity`: B-tree index for filtering by amenity type
- `idx_stations_available`: B-tree index for filtering by availability

**Triggers** (auto-generated):
- Update `last_updated` on row change
- Set `created_at` on first insert
- Set `is_available` default based on OSM data (if OSM has `charging_station:available` tag)

---

### Entity 3: Materialized View - Stations Geo

**Materialized View**: `gis.mv_stations_geo`

**Purpose**: Pre-computed query results for frequent spatial queries

**Definition**:
```sql
CREATE MATERIALIZED VIEW gis.mv_stations_geo AS
SELECT
    id,
    name AS station_name,
    latitude,
    longitude,
    amenity,
    power,
    connector_types,
    is_available,
    ST_Distance(
        ST_MakePoint(longitude, latitude),
        ST_MakePoint(:center_lon, :center_lat)
    ) AS distance
FROM gis.osm_charging_stations
WHERE is_available = TRUE;

CREATE UNIQUE INDEX idx_mv_stations_geo_id ON gis.mv_stations_geo(id);
```

**Fields**:
- Same fields as `osm_charging_stations` plus `distance` (computed on refresh)

**Refresh Strategy**:
- Refresh: Hourly (scheduled)
- Lock type: CONCURRENTLY (no blocking reads)
- Window: 2 AM - 4 AM UTC (low traffic)

---

### Entity 4: Materialized View - Stations Summary

**Materialized View**: `gis.mv_stations_summary`

**Purpose**: Aggregated statistics for analytics and reporting

**Definition**:
```sql
CREATE MATERIALIZED VIEW gis.mv_stations_summary AS
SELECT
    amenity,
    COUNT(*) AS station_count,
    AVG(CAST(power AS FLOAT)) AS avg_power,
    MIN(power) AS min_power,
    MAX(power) AS max_power,
    ARRAY_AGG(DISTINCT connector_type) AS connector_types
FROM gis.osm_charging_stations
GROUP BY amenity;

CREATE UNIQUE INDEX idx_mv_stations_summary_amenity ON gis.mv_stations_summary(amenity);
```

**Fields**:
- `amenity`: Station amenity type
- `station_count`: Total number of stations with this amenity
- `avg_power`: Average charging power (kW)
- `min_power`: Minimum charging power
- `max_power`: Maximum charging power
- `connector_types`: Array of distinct connector types

**Refresh Strategy**:
- Refresh: Hourly (scheduled)

---

## Relationships

### Ingestion Pipeline Relationship

```
[OSM XML] → [osm_charging_stations_temp] → [ETL] → [osm_charging_stations]
    (raw)        (staging)                    (normalized)
```

- One OSM XML record → Multiple OSM tags → Multiple `osm_charging_stations_temp` records (if OSM has multiple connector types)
- One `osm_charging_stations_temp` record → One `osm_charging_stations` record (after normalization)

### Spatial Query Relationship

```
[osm_charging_stations] → [PostGIS Spatial Index] → [Redis Spatial Cache] → [Map Rendering API]
```

- One station → One PostGIS point geometry → One cache key (by radius) → One API response

### Data Ownership Relationship

```
[driver-service] → (WRITE) → gis schema
[admin-service] → (READ)   → gis schema (via API calls)
```

- Driver-service: WRITE to gis schema (ingestion, ETL, queries)
- Admin-service: READ only (no write access)
- No direct writes from other services

---

## Data Flow

### Ingestion Flow

1. **Trigger**: Admin triggers OSM ingestion (via API or scheduled job)
2. **Fetch**: System fetches OSM XML from overpass-api.de (batch export)
3. **Parse**: System parses OSM XML, extracts charging station data
4. **Normalize**: System normalizes OSM tags to internal schema (field-based normalization + JSONB)
5. **Insert**: System inserts into `osm_charging_stations_temp` table
6. **ETL**: System processes staging data, validates, normalizes
7. **Approve**: Admin reviews and approves stations (or automated if confidence high)
8. **Move**: System moves approved stations to `osm_charging_stations` table
9. **Invalidate Cache**: System deletes Redis spatial cache keys

### Query Flow

1. **Request**: Client sends spatial query (lat, lon, radius)
2. **Check Cache**: System checks Redis cache by key `geo:radius:lat:lon:radius`
3. **Cache Hit**: Return cached results
4. **Cache Miss**: System executes PostGIS spatial query:
   - Query: `SELECT * FROM osm_charging_stations WHERE geom && ST_MakePoint(lon, lat)::geography @> ST_MakePoint(lon, lat)::geography`
   - Order by distance
   - Limit results
5. **Store Cache**: System stores results in Redis cache
6. **Return**: System returns results to client

### Cache Invalidation Flow

1. **Trigger**: Station is updated (approved/rejected, modified data)
2. **Invalidate**: System deletes all cache keys matching pattern `geo:radius:*`
3. **Next Query**: Client query results in cache miss, re-executes PostGIS query
4. **Update**: System stores updated results in cache

---

## Validation Rules

### Entity: ChargingStationStaging

- `osm_id`: Must be unique (no duplicates)
- `osm_data`: Must be valid JSONB
- `import_timestamp`: Required, default NOW()
- `processed`: Default FALSE

### Entity: ChargingStation

- `id`: Must be unique, follow pattern "STA-" + nanoid(12)
- `osm_id`: Must be unique (one-to-one with staging)
- `latitude`: Must be between -90 and 90
- `longitude`: Must be between -180 and 180
- `geom`: Must be POINT with SRID 4326
- `amenity`: Must be one of ["charging_station", "power"]
- `connector_types`: Must contain at least one valid type (["Type 2", "CCS", "CHAdeMO", etc.)
- `is_available`: Default TRUE

### Constraint: Spatial Index

- GiST index on `geom` for spatial queries
- Index created automatically on table creation

---

## Migration Files

### Migration 0003: Create GIS Tables

```sql
-- Migration: 0003_gis_tables.up.sql
CREATE TABLE gis.osm_charging_stations_temp (
    id VARCHAR(25) PRIMARY KEY,
    osm_id BIGINT NOT NULL,
    osm_data JSONB NOT NULL,
    import_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE(osm_id)
);

CREATE INDEX idx_staging_osm_id ON gis.osm_charging_stations_temp(osm_id);

CREATE TABLE gis.osm_charging_stations (
    id VARCHAR(25) PRIMARY KEY,
    osm_id BIGINT,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    geom GEOMETRY(Point, 4326) NOT NULL,
    station_name VARCHAR(255),
    operator VARCHAR(255),
    address JSONB,
    amenity VARCHAR(100),
    power VARCHAR(50),
    connector_types TEXT[],
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

CREATE INDEX idx_stations_geo ON gis.osm_charging_stations USING GiST (geom);
CREATE INDEX idx_stations_amenity ON gis.osm_charging_stations (amenity);
CREATE INDEX idx_stations_available ON gis.osm_charging_stations (is_available);
```

### Migration 0004: Materialized Views

```sql
-- Migration: 0004_materialized_views.up.sql
CREATE MATERIALIZED VIEW gis.mv_stations_geo AS
SELECT
    id,
    station_name,
    latitude,
    longitude,
    amenity,
    power,
    connector_types,
    is_available
FROM gis.osm_charging_stations
WHERE is_available = TRUE;

CREATE UNIQUE INDEX idx_mv_stations_geo_id ON gis.mv_stations_geo(id);

CREATE MATERIALIZED VIEW gis.mv_stations_summary AS
SELECT
    amenity,
    COUNT(*) AS station_count,
    AVG(CAST(power AS FLOAT)) AS avg_power,
    MIN(power) AS min_power,
    MAX(power) AS max_power,
    ARRAY_AGG(DISTINCT connector_type) AS connector_types
FROM gis.osm_charging_stations
GROUP BY amenity;

CREATE UNIQUE INDEX idx_mv_stations_summary_amenity ON gis.mv_stations_summary(amenity);

-- Create refresh schedule (if cron extension is available)
CREATE EXTENSION IF NOT EXISTS pg_cron;
SELECT cron.schedule(
    'refresh-gis-views',
    '0 2 * * *',
    $$REFRESH MATERIALIZED VIEW CONCURRENTLY gis.mv_stations_geo;$$
);
```

---

## Performance Considerations

### Index Usage

- **Spatial Queries**: Use GiST index on `geom` (optimal for radius, bounding box queries)
- **Filter Queries**: Use B-tree indexes on `amenity` and `is_available` (fast filtering)
- **Lookup Queries**: Use primary key index on `id` (O(1) lookup by station ID)

### Cache Usage

- **Spatial Cache**: Redis keys `geo:radius:{lat}:{lon}:{radius}` (O(1) lookup)
- **Cache Size**: ~100KB per 1000 stations
- **Cache TTL**: 5 minutes (configurable)

### Materialized Views

- **Refresh Frequency**: Hourly (2 AM UTC)
- **Lock Type**: CONCURRENTLY (no blocking reads during refresh)
- **Impact**: Reduces query latency from ~500ms to ~50ms

---

## Security Considerations

### Data Access Control

- **Write Access**: ONLY driver-service can write to gis schema (enforced by database roles)
- **Read Access**: driver-service and admin-service can read (via API calls)
- **No Direct Access**: No SQL queries allowed from admin-service (must use API)

### Validation

- **Data Validation**: OSM data validated before insertion (required fields, coordinate ranges)
- **Spatial Validation**: PostGIS validates geometry (SRID 4326, POINT type)
- **Business Logic**: Connector types validated against known list (no arbitrary values)

---

## Backup Strategy

### PostgreSQL Backup

- **Full Backup**: Daily full backup (pg_dump)
- **Point-in-Time Recovery**: Enabled (WAL archiving)
- **Retention**: 7 days (keep daily backups)
- **Backup Location**: External storage (S3-compatible object storage)

### Redis Backup

- **RDB Snapshot**: Weekly RDB snapshot
- **AOF**: Append Only File (for durability)
- **Backup Location**: External storage

---

## Audit Trail

### Tables with Audit Fields

- `osm_charging_stations_temp`: `import_timestamp`, `processed`
- `osm_charging_stations`: `last_updated`, `created_at`

### Ingestion Events

- Ingestion events logged to `analytics_db.telemetry.raw_events`
- Event type: `gis.ingestion_start`, `gis.ingestion_complete`, `gis.ingestion_error`
- Metadata: osm_id, file_hash, row_count, duration

---

## Version History

| Version | Date | Description |
|---------|------|-------------|
| 1.0.0 | 2026-06-22 | Initial data model for GIS Engine Foundation |
