# Sprint 04 — EV Domain Bootstrap (ev Schema)

**Status**: SPEC WRITTEN (Phase 0)
**Date**: 2026-06-25
**Constitution Version**: v1.15.2

---

## Scope Lock (HARD CONSTRAINT)

| Domain | Included | Excluded |
|--------|----------|----------|
| **Schema** | `ev` in `platform_db` | Any other schemas |
| **Tables** | Lookup tables, `partners`, `stations`, `chargers` | Any other tables |
| **Migration** | GIS → EV pipeline | Any other pipelines |
| **Services** | ❌ None | No `auth-service`, `driver-service`, `admin-service` changes |
| **API** | ❌ None | No HTTP endpoints |
| **Frontend** | ❌ None | No apps changes |

---

## System Behavior

### Objective

Create the canonical EV domain model that will become the authoritative source for:
- operators (partners)
- charging stations
- chargers/connectors

while preserving imported GIS data from Sprint 01.

### Database Architecture

```
platform_db
  ├── gis/        (Sprint 01 — ingestion layer, owned by driver-service)
  └── ev/         (Sprint 04 — business domain layer, owned by admin-service)
```

### Identity Compliance (§2.4)

| Entity | Prefix | Example |
|--------|--------|---------|
| Partner | `OPR` | `OPR-k9x2lm8q1v7z` |
| Station | `STA` | `STA-abc123def456` |
| Charger | `CHG` | `CHG-m9n4op7q2r5s` |

### Known Bug Fixes Applied

| Bug ID | Issue | Fix |
|--------|-------|-----|
| KNOWN-001 | Test data leaking | All queries must filter test data |
| KNOWN-002 | Missing `deleted_at` | `deleted_at TIMESTAMPTZ` on all entities |

---

## Schema: `ev`

Owned by: `admin-service` (per §4.1 — ownership assigned, service not yet implemented)

### Extensions

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS hstore;
```

### Lookup Tables

**`ev.access_types`**

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| name | VARCHAR(50) | UNIQUE NOT NULL |
| description | TEXT | |

**`ev.data_sources`**

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| name | VARCHAR(50) | UNIQUE NOT NULL |
| description | TEXT | |

**`ev.connector_types`**

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| name | VARCHAR(50) | UNIQUE NOT NULL |
| description | TEXT | |

**`ev.current_types`**

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| name | VARCHAR(20) | UNIQUE NOT NULL |
| description | TEXT | |

**`ev.connector_statuses`**

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| name | VARCHAR(20) | UNIQUE NOT NULL |
| description | TEXT | |

### Partners / Operators

**`ev.partners`**

| Column | Type | Constraints |
|--------|------|-------------|
| partner_id | VARCHAR(16) | PRIMARY KEY (OPR-nanoid(12)) |
| name | VARCHAR(255) | NOT NULL |
| partner_type | VARCHAR(20) | CHECK IN ('INDIVIDUAL','COMPANY') |
| support_phone | VARCHAR(50) | |
| support_email | VARCHAR(255) | |
| is_verified | BOOLEAN | DEFAULT FALSE |
| created_by_uuid | UUID | |
| updated_by_uuid | UUID | |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT NOW() |
| updated_at | TIMESTAMPTZ | |
| deleted_at | TIMESTAMPTZ | |

### Stations

**`ev.stations`**

| Column | Type | Constraints |
|--------|------|-------------|
| station_id | VARCHAR(16) | PRIMARY KEY (STA-nanoid(12)) |
| osm_id | BIGINT | UNIQUE |
| partner_id | VARCHAR(16) | REFERENCES ev.partners(partner_id) |
| name | VARCHAR(255) | NOT NULL |
| address | TEXT | |
| location | GEOGRAPHY(Point,4326) | NOT NULL |
| tags | HSTORE | |
| created_by_uuid | UUID | |
| updated_by_uuid | UUID | |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT NOW() |
| updated_at | TIMESTAMPTZ | |
| deleted_at | TIMESTAMPTZ | |

**Spatial Index:**
```sql
CREATE INDEX idx_stations_location ON ev.stations USING GIST(location);
```

### Chargers

**`ev.chargers`**

| Column | Type | Constraints |
|--------|------|-------------|
| charger_id | VARCHAR(16) | PRIMARY KEY (CHG-nanoid(12)) |
| station_id | VARCHAR(16) | NOT NULL, FK → ev.stations, ON DELETE CASCADE |
| connector_type_id | INTEGER | NOT NULL, FK → ev.connector_types |
| status_id | INTEGER | NOT NULL, FK → ev.connector_statuses |
| current_type_id | INTEGER | NOT NULL, FK → ev.current_types |
| power_kw | DECIMAL(5,2) | |
| voltage | INTEGER | |
| amperage | INTEGER | |
| count_available | INTEGER | DEFAULT 1, CHECK >= 0 |
| count_total | INTEGER | DEFAULT 1, CHECK >= 1 AND >= count_available |
| created_by_uuid | UUID | |
| updated_by_uuid | UUID | |
| created_at | TIMESTAMPTZ | NOT NULL DEFAULT NOW() |
| updated_at | TIMESTAMPTZ | |
| deleted_at | TIMESTAMPTZ | |

**Unique Constraint:**
```sql
CONSTRAINT unique_connector UNIQUE (station_id, connector_type_id, current_type_id)
```

---

## GIS → EV Migration

### Source
`gis.osm_charging_stations`

### Target
`ev.stations`

### Migration Rules

| GIS Field | EV Field | Transformation |
|-----------|----------|----------------|
| osm_id | osm_id | Direct copy |
| name | name | Direct copy |
| lat, lon | location | `ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography` |
| operator | partner_id | NULL (future relationship) |
| — | station_id | `STA`-nanoid(12) generated |
| — | created_at | NOW() |

### Constraints
- Idempotent: duplicate `osm_id` ignored
- Only active records: `deleted_at IS NULL`
- Soft-delete aware

---

## Security Rules

- UUID audit fields only (no UUID as entity ID)
- Constitution ID separation: STA- for stations, OPR- for partners, CHG- for chargers
- FK integrity enforced
- Soft-delete on all entities
- Schema ownership: ev → admin-service

---

## Testing Strategy

### Unit Tests (SQL-based)

| Test | Description |
|------|-------------|
| ID format | STA-, OPR-, CHG- nanoid(12) format |
| Lookup constraints | Unique name enforcement |
| Charger count constraints | count_available >= 0, count_total >= 1, count_total >= count_available |
| Soft delete | deleted_at filter |

### Integration Tests (SQL-based)

| Test | Description |
|------|-------------|
| FK relationships | partner_id → ev.partners, station_id → ev.stations |
| PostGIS geography | location stored correctly as GEOGRAPHY(Point,4326) |
| Migration correctness | GIS → EV data fidelity |
| Duplicate osm_id | Idempotent migration |
| Spatial index | GIST index exists on location |

---

## Implementation Flow

| Step | Description |
|------|-------------|
| STEP 1 | Branch: `sprint/04-ev-domain-bootstrap` |
| STEP 2 | Create ev schema + extensions |
| STEP 3 | Create lookup tables |
| STEP 4 | Create ev.partners |
| STEP 5 | Create ev.stations + spatial index |
| STEP 6 | Create ev.chargers |
| STEP 7 | Create GIS → EV migration |
| STEP 8 | Apply migrations + validate SQL |
| STEP 9 | Integration tests |
| STEP 10 | Generate delivery artifacts |

---

## Hard Stop Conditions

| Condition | Action |
|-----------|--------|
| UUID used as entity IDs | HALT |
| nanoid used for users | HALT |
| Spatial index omitted | HALT |
| Soft-delete omitted | HALT |
| Schema ownership violated | HALT |
| Scope expansion | HALT |
| SQL syntax error | HALT |
