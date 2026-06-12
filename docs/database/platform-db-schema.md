# platform_db Schema Documentation

**Database:** PostgreSQL 16 + PostGIS  
**Owner:** Admin service (write) + Driver service (read)  
**Purpose:** System of record for stations, chargers, partners

---

## Overview

Three schemas under `platform_db`:

| Schema | Owner | Access | Purpose |
|--------|-------|--------|---------|
| `inventory` | Admin service | WRITE | Stations, chargers, partners |
| `gis` | OSM import | READ-ONLY | Derived geographic data |
| `users` | Keycloak | Auth scope | User identity (via Keycloak) |

---

## Inventory Schema

### partner

Charging network partners (companies, operators).

```sql
CREATE TABLE inventory.partner (
    id VARCHAR(50) PRIMARY KEY,  -- PRT-{nanoid}
    name VARCHAR(255) NOT NULL UNIQUE,
    contact_email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_partner_email ON inventory.partner(contact_email);
```

**Columns:**

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | VARCHAR(50) | NO | Primary key, format: `PRT-{nanoid}` |
| `name` | VARCHAR(255) | NO | Partner name, globally unique |
| `contact_email` | VARCHAR(255) | NO | Contact email |
| `created_at` | TIMESTAMP | NO | Creation time (UTC) |
| `updated_at` | TIMESTAMP | NO | Last update time (UTC) |

**Constraints:**
- Primary key: `id`
- Unique: `name`
- Index: `contact_email` (fast lookup)

---

### station

Charging stations (physical locations).

```sql
CREATE TABLE inventory.station (
    id VARCHAR(50) PRIMARY KEY,  -- STA-{nanoid}
    name VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    location GEOMETRY(Point, 4326) GENERATED ALWAYS AS
        (ST_SetSRID(ST_Point(lng, lat), 4326)) STORED,
    status VARCHAR(20) NOT NULL DEFAULT 'offline',  -- available|busy|offline|unknown
    opening_hours VARCHAR(255),
    partner_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    FOREIGN KEY (partner_id) REFERENCES inventory.partner(id)
);

-- Spatial index for nearby search (critical for performance)
CREATE INDEX idx_station_location_gist
    ON inventory.station USING GIST(location)
    WHERE deleted_at IS NULL;

-- Fast lookup by partner
CREATE INDEX idx_station_partner_id ON inventory.station(partner_id)
    WHERE deleted_at IS NULL;

-- Fast lookup by status
CREATE INDEX idx_station_status ON inventory.station(status)
    WHERE deleted_at IS NULL;
```

**Columns:**

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | VARCHAR(50) | NO | Primary key, format: `STA-{nanoid}` |
| `name` | VARCHAR(255) | NO | Station name |
| `address` | VARCHAR(255) | NO | Human-readable address |
| `lat` | DOUBLE PRECISION | NO | Latitude (WGS 84) |
| `lng` | DOUBLE PRECISION | NO | Longitude (WGS 84) |
| `location` | GEOMETRY(Point, 4326) | NO | PostGIS point, auto-generated from lat/lng |
| `status` | VARCHAR(20) | NO | Station status (see enum below) |
| `opening_hours` | VARCHAR(255) | YES | Human-readable hours (e.g., "24/7", "06:00-23:00") |
| `partner_id` | VARCHAR(50) | NO | Foreign key to partner |
| `created_at` | TIMESTAMP | NO | Creation time (UTC) |
| `updated_at` | TIMESTAMP | NO | Last update time (UTC) |
| `deleted_at` | TIMESTAMP | YES | Soft-delete marker (NULL = active) |

**Status Enum:**
- `offline` — station offline, not available to drivers
- `available` — station available, has available chargers
- `busy` — station available, all chargers busy
- `unknown` — status unknown (sensor failure)

**Constraints:**
- Primary key: `id`
- Foreign key: `partner_id` → `inventory.partner(id)`
- Spatial index: `location` (GiST) where not deleted
- Index: `partner_id` for admin lookups
- Index: `status` for filtering

**Soft Delete:**
- Records are never physically deleted
- `deleted_at IS NULL` filters active records
- All indexes exclude deleted rows

---

### charger

Charging connectors (within a station).

```sql
CREATE TABLE inventory.charger (
    id VARCHAR(50) PRIMARY KEY,  -- CHR-{nanoid}
    station_id VARCHAR(50) NOT NULL,
    type VARCHAR(20) NOT NULL,  -- CCS2|CHAdeMO|Type2|GBT|Type1
    power_kw FLOAT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'offline',  -- available|busy|faulted|offline
    price_per_kwh FLOAT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES inventory.station(id)
);

CREATE INDEX idx_charger_station_id ON inventory.charger(station_id)
    WHERE deleted_at IS NULL;

CREATE INDEX idx_charger_type ON inventory.charger(type)
    WHERE deleted_at IS NULL;

CREATE INDEX idx_charger_status ON inventory.charger(status)
    WHERE deleted_at IS NULL;
```

**Columns:**

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `id` | VARCHAR(50) | NO | Primary key, format: `CHR-{nanoid}` |
| `station_id` | VARCHAR(50) | NO | Foreign key to station |
| `type` | VARCHAR(20) | NO | Charger type (see enum below) |
| `power_kw` | FLOAT | NO | Charging power in kW (> 0) |
| `status` | VARCHAR(20) | NO | Charger status (see enum below) |
| `price_per_kwh` | FLOAT | NO | Price per kWh (≥ 0) |
| `created_at` | TIMESTAMP | NO | Creation time (UTC) |
| `updated_at` | TIMESTAMP | NO | Last update time (UTC) |
| `deleted_at` | TIMESTAMP | YES | Soft-delete marker (NULL = active) |

**Type Enum:**
- `CCS2` — Combined Charging System Type 2 (DC fast)
- `CHAdeMO` — CHAdeMO (DC fast, declining in EU)
- `Type2` — Type 2 Mennekes (AC, 22-43 kW)
- `GBT` — Guobiao (GB/T) (China, DC fast)
- `Type1` — Type 1 J1772 (AC, legacy)

**Status Enum:**
- `offline` — charger offline, non-functional
- `available` — charger available, no active session
- `busy` — charger in use
- `faulted` — charger error (requires service)

**Constraints:**
- Primary key: `id`
- Foreign key: `station_id` → `inventory.station(id)`
- Index: `station_id` (join queries)
- Index: `type` (filter by connector type)
- Index: `status` (real-time availability)
- Soft delete via `deleted_at`

---

## GIS Schema

Read-only derived geographic data loaded from OpenStreetMap.

### osm_region

Administrative boundaries (municipalities, regions).

```sql
CREATE TABLE gis.osm_region (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    admin_level INTEGER,
    boundary GEOMETRY(Polygon, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_osm_region_boundary_gist
    ON gis.osm_region USING GIST(boundary);
```

**Purpose:** Enable queries like "stations in Tunis municipality" (future, MVP-3+)

---

### osm_road

Road network for routing (future, MVP-4+).

```sql
CREATE TABLE gis.osm_road (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    highway_type VARCHAR(50),
    geometry GEOMETRY(LineString, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_osm_road_geometry_gist
    ON gis.osm_road USING GIST(geometry);
```

**Purpose:** Future integration with routing engines (e.g., OSRM)

---

## Users Schema

Identity schema (managed by Keycloak). **Services never access directly.**

Keycloak manages user records and authentication. BorneMap services validate JWTs and read `sub`, `realm`, `role`, `partner_id` from tokens.

---

## Data Integrity Rules

1. **Soft Delete Only**
   - Infrastructure entities (partner, station, charger) are soft-deleted
   - User-generated data (events) are hard-deleted
   - All queries exclude deleted rows: `WHERE deleted_at IS NULL`

2. **Timestamps**
   - All timestamps are UTC, ISO 8601 format
   - `created_at` is immutable (set at INSERT)
   - `updated_at` updates on every WRITE

3. **Geospatial Accuracy**
   - SRID 4326 (WGS 84) for all geometries
   - Coordinates valid: lat [-90, 90], lng [-180, 180]
   - `location` is auto-generated from lat/lng

4. **Referential Integrity**
   - Cascading deletes not used (soft delete instead)
   - Foreign keys enforced
   - Cannot create charger without parent station
   - Cannot delete partner with active stations

5. **Uniqueness**
   - Partner names are globally unique
   - Station IDs are unique
   - Charger IDs are unique

---

## Queries by Use Case

### Driver Service: Get Nearby Stations

```sql
SELECT id, name, address, lat, lng, status, charger_count, available_chargers,
       ST_Distance(location, ST_SetSRID(ST_Point($lng, $lat), 4326)) as distance_m
FROM inventory.station
WHERE deleted_at IS NULL
  AND ST_DWithin(
    location,
    ST_SetSRID(ST_Point($lng, $lat), 4326),
    $radius_km * 1000
  )
ORDER BY distance_m ASC
LIMIT $limit;
```

Uses `idx_station_location_gist` for fast lookup.

### Driver Service: Get Station Detail

```sql
SELECT s.id, s.name, s.address, s.lat, s.lng, s.status, s.opening_hours,
       s.partner_id, p.name as partner_name,
       json_agg(
         json_build_object(
           'id', c.id,
           'type', c.type,
           'power_kw', c.power_kw,
           'status', c.status,
           'price_per_kwh', c.price_per_kwh
         ) ORDER BY c.created_at
       ) as chargers
FROM inventory.station s
JOIN inventory.partner p ON s.partner_id = p.id
LEFT JOIN inventory.charger c ON s.id = c.station_id AND c.deleted_at IS NULL
WHERE s.id = $station_id AND s.deleted_at IS NULL
GROUP BY s.id, s.name, s.address, s.lat, s.lng, s.status, s.opening_hours, s.partner_id, p.name;
```

### Admin Service: Get Partner's Stations

```sql
SELECT id, name, address, lat, lng, status, partner_id, COUNT(*) as charger_count
FROM inventory.station
WHERE partner_id = $partner_id AND deleted_at IS NULL
GROUP BY id
ORDER BY created_at DESC
LIMIT $per_page OFFSET $offset;
```

---

## Migration Strategy

1. **0001-platform-db-init.sql** — Create database, extensions, schemas
2. **0002-inventory-schema.sql** — Create partner, station, charger tables + indexes
3. **0003-gis-schema.sql** — Create GIS tables (loaded from OSM later)
4. **0004-users-schema.sql** — Create users schema (linked to Keycloak)
5. **0005-seed-data.sql** — Load Tunisia test data (dev only)

All migrations are idempotent (safe to re-run).

---

## Monitoring & Maintenance

### Index Bloat
Monthly check for index bloat:
```sql
SELECT schemaname, tablename, indexname, idx_size, idx_ratio
FROM pg_stat_user_indexes
WHERE schemaname = 'inventory' AND idx_ratio > 0.5;
```

### Query Performance
Monitor slow queries (>100ms for nearby, >50ms for detail):
```sql
SELECT query, mean_exec_time, calls
FROM pg_stat_statements
WHERE query LIKE '%inventory%'
ORDER BY mean_exec_time DESC;
```

### Replication Lag (Production)
For HA setups, monitor replication lag:
```sql
SELECT client_addr, pg_wal_lsn_diff(pg_current_wal_lsn(), reply_time)
FROM pg_stat_replication;
```
