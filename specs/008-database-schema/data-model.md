# Data Model: Database Schema

**Phase**: Phase 1 — Entity definitions for Sprint 2.2

**Date**: 2026-06-09

## Schema: `ev-platform`

All tables live under the `ev-platform` PostgreSQL schema.

## Table: `partner`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `TEXT` | `PRIMARY KEY` | Unique partner identifier (NanoID) |
| `name` | `TEXT` | `NOT NULL` | Partner display name |
| `type` | `TEXT` | `NOT NULL, CHECK (type IN ('business', 'personal'))` | Partner category |
| `is_verified` | `BOOLEAN` | `NOT NULL DEFAULT false` | Whether partner has been verified |
| `is_live` | `BOOLEAN` | `NOT NULL DEFAULT false` | Whether partner stations are visible to drivers |
| `is_active` | `BOOLEAN` | `NOT NULL DEFAULT true` | Whether partner is active or deactivated |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL` | Row creation timestamp |
| `created_by` | `TEXT` | `NOT NULL` | Who created the row |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL` | Last update timestamp |
| `updated_by` | `TEXT` | `NOT NULL` | Who last updated the row |

**Constraints**:
- `ck_partner_type`: `type IN ('business', 'personal')`

## Table: `station`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `TEXT` | `PRIMARY KEY` | Unique station identifier (NanoID) |
| `partner_id` | `TEXT` | `NOT NULL REFERENCES partner(id)` | Owning partner |
| `name` | `TEXT` | `NOT NULL` | Station display name |
| `address` | `TEXT` | — | Physical address |
| `latitude` | `DOUBLE PRECISION` | `NOT NULL, CHECK (-90 <= latitude AND latitude <= 90)` | WGS84 latitude |
| `longitude` | `DOUBLE PRECISION` | `NOT NULL, CHECK (-180 <= longitude AND longitude <= 180)` | WGS84 longitude |
| `location` | `GEOMETRY(Point, 4326)` | `NOT NULL` | Computed PostGIS point from lat/lng |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL` | Row creation timestamp |
| `created_by` | `TEXT` | `NOT NULL` | Who created the row |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL` | Last update timestamp |
| `updated_by` | `TEXT` | `NOT NULL` | Who last updated the row |

**Constraints**:
- `ck_station_latitude`: `latitude BETWEEN -90 AND 90`
- `ck_station_longitude`: `longitude BETWEEN -180 AND 180`
- `fk_station_partner`: `partner_id REFERENCES partner(id)`

**Indexes**:
- `idx_station_location`: `GIST (location)` — spatial index for ST_DWithin queries

**Trigger**:
- `trg_station_location`: `BEFORE INSERT OR UPDATE` — sets `location = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)`

## Table: `charger`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `TEXT` | `PRIMARY KEY` | Unique charger identifier (NanoID) |
| `station_id` | `TEXT` | `NOT NULL REFERENCES station(id)` | Parent station |
| `connector_type` | `TEXT` | `NOT NULL, CHECK (connector_type IN ('type2', 'type3', 'ccs', 'chademo'))` | Connector standard |
| `power_kw` | `DOUBLE PRECISION` | `NOT NULL, CHECK (power_kw > 0)` | Power rating in kW |
| `status` | `TEXT` | `NOT NULL, CHECK (status IN ('available', 'in_use', 'maintenance', 'offline'))` | Operational status |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL` | Row creation timestamp |
| `created_by` | `TEXT` | `NOT NULL` | Who created the row |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL` | Last update timestamp |
| `updated_by` | `TEXT` | `NOT NULL` | Who last updated the row |

**Constraints**:
- `ck_charger_connector_type`: `connector_type IN ('type2', 'type3', 'ccs', 'chademo')`
- `ck_charger_power_kw`: `power_kw > 0`
- `ck_charger_status`: `status IN ('available', 'in_use', 'maintenance', 'offline')`
- `fk_charger_station`: `station_id REFERENCES station(id)`

## Table: `station_availability`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `TEXT` | `PRIMARY KEY` | Unique record identifier (NanoID) |
| `station_id` | `TEXT` | `NOT NULL REFERENCES station(id)` | Target station |
| `status` | `TEXT` | `NOT NULL, CHECK (status IN ('available', 'partial', 'unavailable'))` | Availability status |
| `updated_by` | `TEXT` | `NOT NULL` | Who set this status |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL` | When this status was set |

**Constraints**:
- `ck_availability_status`: `status IN ('available', 'partial', 'unavailable')`
- `fk_availability_station`: `station_id REFERENCES station(id)`

## Entity Relationships

```
partner 1───* station 1───* charger
                     1───* station_availability
```

## State Transitions

### partner.type
- Set at creation — never changes
- Values: `business`, `personal`

### partner.is_verified
- `false` → `true` (admin verifies)
- No reverse transition

### partner.is_live
- `false` → `true` (admin sets live)
- `true` → `false` (admin removes from live)

### partner.is_active
- `true` → `false` (admin deactivates)
- `false` → `true` (admin reactivates)

### charger.status
- `available` ↔ `in_use` (normal operation)
- Any state → `maintenance` (partner sets maintenance)
- Any state → `offline` (system or admin)
- `maintenance` → `available` (maintenance complete)
- `offline` → `available` (back online)

### station_availability.status
- Stores history — rows are appended, not updated
- Latest record per station is the current availability
- Values: `available`, `partial`, `unavailable`
