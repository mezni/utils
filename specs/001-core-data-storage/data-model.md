# Data Model: Core Data & Storage Foundations

## Schema Overview

```
platform_db (PostgreSQL 16 + PostGIS 3.4)
├── gis schema              (OSM reference + mirrored map layers)
│   ├── osm_roads           (road network geometries — imported via osm2pgsql)
│   ├── osm_cities          (populated place boundaries — imported)
│   ├── osm_points          (points of interest — imported)
│   └── osm_stations        (station geometry mirrored from inventory — sync layer)
├── inventory schema        (application infrastructure — managed)
│   ├── partner             (station operator/owner)
│   ├── station             (physical charging location)
│   ├── charger             (individual charging unit)
│   └── sync_outbox         (event outbox for inventory→GIS replication)
```

---

## inventory.partner

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | TEXT | PK, prefix `OPR_` | NanoID partner identifier |
| name | TEXT | NOT NULL | Legal or trading name |
| contact_email | TEXT | | Business contact email |
| contact_phone | TEXT | | Business contact phone |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Record creation timestamp |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Last modification timestamp |
| deleted_at | TIMESTAMPTZ | NULLABLE | Soft-delete timestamp |

**Relationships**: A partner has many stations (1:N).

---

## inventory.station

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | TEXT | PK, prefix `STA_` | NanoID station identifier |
| partner_id | TEXT | FK → inventory.partner.id, NULLABLE | Owning partner (NULL for private) |
| name | TEXT | NOT NULL | Station display name |
| address | TEXT | | Street address |
| city | TEXT | | City name |
| latitude | DOUBLE PRECISION | NOT NULL | WGS84 latitude |
| longitude | DOUBLE PRECISION | NOT NULL | WGS84 longitude |
| location | GEOGRAPHY(Point, 4326) | NOT NULL, GENERATED | PostGIS geography point |
| is_private | BOOLEAN | NOT NULL, DEFAULT FALSE | True for home chargers |
| metadata | JSONB | | Custom fields (hours, pricing notes) |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |
| deleted_at | TIMESTAMPTZ | NULLABLE | Soft-delete timestamp |

**Indexes**:
- GIST index on `location` (geography column)
- BTREE index on `partner_id`
- BTREE index on `city`

**Constraints**:
- latitude: -90 to 90
- longitude: -180 to 180
- `location` is auto-populated from latitude/longitude via trigger or
  generated column: `ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography`

---

## inventory.charger

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | TEXT | PK, prefix `CHG_` | NanoID charger identifier |
| station_id | TEXT | FK → inventory.station.id, NOT NULL | Parent station |
| connector_type | TEXT | NOT NULL | Type (Type2, CCS, CHAdeMO) |
| power_kw | NUMERIC(5,1) | NOT NULL | Rated power output |
| status | TEXT | NOT NULL, DEFAULT 'unknown' | Operational status |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |
| deleted_at | TIMESTAMPTZ | NULLABLE | Soft-delete timestamp |

**Indexes**:
- BTREE index on `station_id`

---

## gis Schema (Reference Data)

Imported via osm2pgsql with a custom style file selecting:

| Table | Content | Geometry Type |
|-------|---------|---------------|
| `gis.osm_roads` | Major and minor road network | MULTILINESTRING |
| `gis.osm_cities` | Populated place boundaries | MULTIPOLYGON / POINT |
| `gis.osm_points` | Points of interest (amenities, landmarks) | POINT |

All tables include:
- `osm_id` (BIGINT) — OpenStreetMap node/way/relation ID
- `name` (TEXT) — Display name
- `geometry` (GEOMETRY) — Spatial column with GIST index
- Additional tags as columns per osm2pgsql style file

---

## gis.osm_stations (Mirrored Layer)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| osm_id | TEXT | PK, matches `inventory.station.id` | Station identifier (STA_ prefix) |
| name | TEXT | NOT NULL | Station display name |
| tags | JSONB | DEFAULT '{}' | Enriched metadata (operator, city, address, inventory metadata) |
| way | GEOMETRY(Point, 4326) | NOT NULL | PostGIS geometry point for spatial joins with OSM data |

**Indexes**:
- GIST index on `way`

**Purpose**: Maintained by the inventory→GIS sync pipeline. Used for spatial background analyses (road proximity, boundary caching) without querying the operational inventory tables directly. The `way` column uses the `GEOMETRY` type (matching osm2pgsql convention) rather than `GEOGRAPHY`, since this layer is designed for spatial overlay and visualization, not geodesic distance computation.

---

## inventory.sync_outbox (Event Outbox)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | BIGSERIAL | PK | Auto-incrementing event ID |
| entity_type | VARCHAR(50) | NOT NULL | Entity type discriminator ('STATION') |
| entity_id | VARCHAR(50) | NOT NULL | Entity identifier (e.g. 'STA_001') |
| action_type | VARCHAR(20) | NOT NULL | 'INSERT', 'UPDATE', or 'DELETE' |
| processed | BOOLEAN | DEFAULT FALSE | Whether the sync worker has consumed this event |
| retry_count | INT | DEFAULT 0 | Number of failed processing attempts |
| created_at | TIMESTAMPTZ | DEFAULT CURRENT_TIMESTAMP | Event creation time |
| processed_at | TIMESTAMPTZ | NULLABLE | When the sync worker processed this event |

**Indexes**:
- BTREE index on `(processed, created_at)` — supports the worker's unprocessed-rows query

**Architecture**: Outbox pattern avoids coupling inventory writes to GIS sync on the API thread. When a station is created/updated/deleted, the trigger `inventory.tr_queue_station_sync()` appends an event to this table within the same transaction. A separate sync worker (`gis.process_sync_outbox()`) polls unprocessed rows and upserts/deletes the corresponding row in `gis.osm_stations`. Failed events are retried up to `max_retries` times by incrementing `retry_count`.

---

## Stored Function: gis.get_nearby_stations

### Signature
FUNCTION gis.get_nearby_stations(
    lng DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    radius_meters DOUBLE PRECISION
) RETURNS TABLE(
    station_id TEXT,
    station_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    distance_meters DOUBLE PRECISION,
    is_private BOOLEAN,
    partner_name TEXT
)
```

**Behavior**:
- Converts input coordinates to geography point
- Uses `ST_DWithin` with geography cast for indexed distance filter
- Orders results by `ST_Distance` ascending
- Only returns non-deleted stations (`deleted_at IS NULL`)
- Returns empty set (not NULL) when no stations match

---

## Sync Functions

### gis.sync_station
`FUNCTION gis.sync_station(target_id TEXT) RETURNS VOID`

Upserts a single station from `inventory.station` into `gis.osm_stations`. Shared between the seed data INSERT and the `process_sync_outbox` worker. Merges `inventory.station.metadata` into the `gis.osm_stations.tags` JSONB column.

### gis.process_sync_outbox
`FUNCTION gis.process_sync_outbox(max_retries INT DEFAULT 3) RETURNS TABLE(processed_id BIGINT, entity_id TEXT, action_type TEXT, status TEXT)`

Drains unprocessed events from `inventory.sync_outbox`:
- **INSERT / UPDATE**: Calls `gis.sync_station()` to upsert the row
- **DELETE**: Removes the row from `gis.osm_stations`
- Marks successfully processed events with `processed = TRUE` and `processed_at`
- On error, increments `retry_count` and leaves `processed = FALSE` for retry
- Uses `FOR UPDATE SKIP LOCKED` for safe concurrent execution
- Skips events whose `retry_count >= max_retries`
