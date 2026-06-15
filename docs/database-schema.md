# Database Schema — BorneMap platform_db

**Engine**: PostgreSQL 17 + PostGIS 3.4  
**Database**: `bornemap_platform`  
**Connection**: `postgres://bornemap:bornemap@localhost:5432/bornemap_platform`

## Schemas

| Schema | Purpose | MVP-1 Status |
|--------|---------|--------------|
| `configuration` | Immutable lookup tables (plug types) | Active |
| `inventory` | Domain entities (partners, stations, chargers) | Active |
| `gis` | Read-optimized spatial cache | Active |
| `users` | User accounts | Deferred (Phase 2) |

---

## Tables

### `configuration.plug_types`

Immutable reference table for charging connector standards.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| `code_key` | VARCHAR(32) | PK | Short code (ccs2, type2, chademo) |
| `display_name` | VARCHAR(100) | NOT NULL | Human-readable name |
| `description` | TEXT | — | Optional description |
| `created_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Audit timestamp |
| `updated_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Audit timestamp |

**Seed Data**:
| code_key | display_name | description |
|----------|-------------|-------------|
| ccs2 | Combined Charging System 2 | DC fast charging, dominant in Europe/Tunisia |
| type2 | Mennekes Type 2 | AC standard for destination charging |
| chademo | CHAdeMO | Legacy Japanese DC fast-charging standard |

---

### `inventory.partners`

Organizations that own and operate charging stations.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| `id` | VARCHAR(64) | PK | PRT-XXXX format |
| `name` | VARCHAR(255) | NOT NULL | Organization name |
| `type` | VARCHAR(20) | NOT NULL, CHECK (BUSINESS, PRIVATE) | Partner classification |
| `email` | VARCHAR(255) | NOT NULL | Contact email |
| `phone` | VARCHAR(50) | NOT NULL | Contact phone |
| `verified` | BOOLEAN | NOT NULL DEFAULT FALSE | Verification status |
| `created_by` | VARCHAR(64) | NOT NULL DEFAULT 'usr-mvp1-fallback' | Audit |
| `updated_by` | VARCHAR(64) | NOT NULL DEFAULT 'usr-mvp1-fallback' | Audit |
| `created_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Audit timestamp |
| `updated_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Audit timestamp |

**Indexes**: None beyond PK.

---

### `inventory.stations`

Physical charging station locations. Source of truth for all station data.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| `id` | VARCHAR(64) | PK | STA-XXXX format |
| `partner_id` | VARCHAR(64) | NOT NULL, FK → partners(id) ON DELETE RESTRICT | Owning partner |
| `name` | VARCHAR(255) | NOT NULL | Station name |
| `address` | TEXT | NOT NULL | Physical address |
| `email` | VARCHAR(255) | NOT NULL | Station contact email |
| `latitude` | DOUBLE PRECISION | NOT NULL | WGS84 latitude |
| `longitude` | DOUBLE PRECISION | NOT NULL | WGS84 longitude |
| `availability` | VARCHAR(32) | NOT NULL DEFAULT 'AVAILABLE', CHECK (AVAILABLE, OCCUPIED, OUT_OF_SERVICE) | Current availability |
| `verified` | BOOLEAN | NOT NULL DEFAULT FALSE | Verification status |
| `is_live` | BOOLEAN | NOT NULL DEFAULT FALSE | Live on platform |
| `created_by` | VARCHAR(64) | NOT NULL DEFAULT 'usr-mvp1-fallback' | Audit |
| `updated_by` | VARCHAR(64) | NOT NULL DEFAULT 'usr-mvp1-fallback' | Audit |
| `created_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Audit timestamp |
| `updated_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Audit timestamp |

**Triggers**: `trg_replicate_station_to_gis_cache` — AFTER INSERT OR UPDATE, syncs to `gis.osm_stations`.

---

### `inventory.chargers`

Individual charging hardware units at stations.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| `id` | VARCHAR(64) | PK | CHR-XXXX format |
| `station_id` | VARCHAR(64) | NOT NULL, FK → stations(id) ON DELETE CASCADE | Parent station |
| `identifier_code` | VARCHAR(50) | NOT NULL | Local hardware label |
| `plug_type_code` | VARCHAR(32) | NOT NULL, FK → configuration.plug_types(code_key) ON DELETE RESTRICT | Connector standard |
| `max_power_kw` | INT | NOT NULL | Max power in kW |
| `status` | VARCHAR(32) | NOT NULL DEFAULT 'ONLINE', CHECK (ONLINE, CHARGING, FAULTED, OFFLINE) | Operational status |
| `created_by` | VARCHAR(64) | NOT NULL DEFAULT 'usr-mvp1-fallback' | Audit |
| `updated_by` | VARCHAR(64) | NOT NULL DEFAULT 'usr-mvp1-fallback' | Audit |
| `created_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Audit timestamp |
| `updated_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Audit timestamp |

**Constraints**: `UNIQUE (station_id, identifier_code)` — no duplicate labels per station.

---

### `gis.osm_stations`

Read-optimized spatial cache. Populated by trigger from `inventory.stations` or directly by OSM import.

| Column | Type | Constraints | Description |
|--------|------|------------|-------------|
| `id` | VARCHAR(64) | PK | Matches inventory.stations.id |
| `name` | VARCHAR(255) | NOT NULL | Station name |
| `address` | TEXT | — | Physical address |
| `coordinates` | GEOMETRY(Point, 4326) | NOT NULL | Spatial point (lon, lat) |
| `source` | VARCHAR(32) | NOT NULL | 'OSM_IMPORT' or 'PLATFORM_SYNC' |
| `is_available` | BOOLEAN | NOT NULL DEFAULT TRUE | Computed from availability |
| `last_modified_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | Sync timestamp |

**Indexes**: `GIST (coordinates)` — spatial index for `ST_DWithin`.

---

## Automated Timestamps

All inventory and configuration tables use a shared trigger function `inventory.update_modified_timestamp_column()` that sets `updated_at = CURRENT_TIMESTAMP` on every UPDATE.

**Bound triggers**:
- `configuration.plug_types`
- `inventory.partners`
- `inventory.stations`
- `inventory.chargers`

---

## Replication Trigger Logic

`gis.sync_inventory_station_to_gis_cache()`:
- If `NEW.is_live = FALSE` → DELETE from `gis.osm_stations`
- If `NEW.is_live = TRUE` → UPSERT into `gis.osm_stations` with:
  - `coordinates = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)`
  - `is_available = (NEW.availability = 'AVAILABLE')`
  - `source = 'PLATFORM_SYNC'`
