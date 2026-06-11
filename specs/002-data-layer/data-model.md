# Data Model: Data Layer

**Feature**: Data Layer | **Branch**: `002-data-layer` | **Date**: 2026-06-10

**Schema**: `inventory` (platform_db)

## Entities

### Partner

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| id | TEXT | PK | UUID or slug |
| name | TEXT | NOT NULL | Display name |
| type | TEXT | NOT NULL, CHECK IN ('business', 'personal') | Partner classification |
| is_verified | BOOLEAN | DEFAULT FALSE | Admin-verified identity |
| is_active | BOOLEAN | DEFAULT TRUE | Soft delete flag |
| is_live | BOOLEAN | DEFAULT FALSE, requires verified | Production-ready flag |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | |
| created_by | TEXT | NULLABLE | Who created this record |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | |
| updated_by | TEXT | NULLABLE | Who last updated |

**Relationships**: Has many stations (1:N via `station.partner_id`)

### Station

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| id | TEXT | PK | UUID or slug |
| partner_id | TEXT | NOT NULL, FK → partner.id | Station owner |
| name | TEXT | NOT NULL | Display name |
| address | TEXT | NULLABLE | Street address text |
| latitude | NUMERIC(10,7) | NOT NULL, CHECK (-90 to 90) | WGS84 latitude |
| longitude | NUMERIC(10,7) | NOT NULL, CHECK (-180 to 180) | WGS84 longitude |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | |
| created_by | TEXT | NULLABLE | |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | |
| updated_by | TEXT | NULLABLE | |

**Relationships**: Belongs to Partner (N:1), Has many Chargers (1:N)

**Indexes**:
- `idx_station_partner` (partner_id) — FK lookup
- `idx_station_location` (latitude, longitude) — naive coordinate filter
- `idx_station_geog` — GiST index on `ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)` — PostGIS spatial search

### Charger

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| id | TEXT | PK | UUID or slug |
| station_id | TEXT | NOT NULL, FK → station.id, CASCADE | Parent station |
| connector_type | TEXT | NOT NULL | e.g. CCS2, Type2, CHAdeMO |
| power_kw | NUMERIC(6,2) | NOT NULL, CHECK (> 0) | Rated power output |
| status | TEXT | NOT NULL, DEFAULT 'available' | Current operational status |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | |
| created_by | TEXT | NULLABLE | |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | |
| updated_by | TEXT | NULLABLE | |

**Relationships**: Belongs to Station (N:1)

**Indexes**: `idx_charger_station` (station_id)

### ConnectorType (Reference Table)

| Field | Type | Constraints |
|-------|------|-------------|
| code | TEXT | PK |
| label | TEXT | NOT NULL |
| description | TEXT | NULLABLE |

### ChargerStatus (Reference Table)

| Field | Type | Constraints |
|-------|------|-------------|
| code | TEXT | PK |
| label | TEXT | NOT NULL |
| description | TEXT | NULLABLE |

## Query Types

### SpatialSearch

| Attribute | Type | Notes |
|-----------|------|-------|
| latitude | f64 | Center point latitude (WGS84) |
| longitude | f64 | Center point longitude (WGS84) |
| radius_meters | f64 | Search radius in meters |
| result | Vec<Station> | Stations ordered by distance (ascending) |

**Implementation**: PostGIS `ST_DWithin(ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography, station.geography, :radius_meters)`

### StationDetail

| Attribute | Type | Notes |
|-----------|------|-------|
| station_id | String | UUID or slug |
| result | (Station, Vec<Charger>, Partner) | Station + its chargers + partner |

**Implementation**: JOIN across station, charger, partner tables.

## Migration Files

Migration files live at `source/services/libs/borne-data/migrations/` named with timestamp prefix:
- `YYYYMMDDHHMMSS_description.sql`

Tracking table: `_sqlx_migrations` (created by SQLx migrate, records filename, checksum, applied timestamp)

