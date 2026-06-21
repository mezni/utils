# Sprint 001 — End-to-End Spatial Flow
**Version:** 1.1
**Date:** June 2026
**Phase:** SPEC (draft — pending validation)

---

## Goal

End-to-end spatial flow working with minimal components:
OSM Tunisia → PostGIS → nearby query → API → map

---

## Feature 1 — Platform Database (PostGIS Ready)

**User Story:** As the system, I need a spatial database so I can store and query geographic data.

**Scope:**
- Docker Compose starts PostgreSQL 16
- PostGIS extension enabled
- `platform_db` accessible by services

**Done When:**
- DB boots via `docker compose up`
- `SELECT PostGIS_version()` works

**OpenAPI:** `platform/api/openapi/driver.yaml` — no API endpoints required

---

## Feature 2 — Full Inventory Schema

**User Story:** As the system, I need tables to store EV charging station locations, partners, and charging equipment.

**Scope:**

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Lookup tables
CREATE TABLE inventory.access_types ( ... );
CREATE TABLE inventory.data_sources ( ... );
CREATE TABLE inventory.connector_types ( ... );
CREATE TABLE inventory.current_types ( ... );
CREATE TABLE inventory.connector_statuses ( ... );
CREATE TABLE inventory.station_statuses ( ... );
CREATE TABLE inventory.charger_statuses ( ... );
```

Schema: `inventory`

**Lookup tables:**
- `access_types` — partner user access levels
- `data_sources` — source system enumeration (`osm`, `manual`, `partner`)
- `connector_types`, `current_types`, `connector_statuses`, `station_statuses`, `charger_statuses`

**Entity tables:**

`inventory.partners`:
- `partner_id` VARCHAR(32) PK — `PAR-<nanoid(12)>` with regex check
- `name`, `partner_type` (INDIVIDUAL|COMPANY), `support_phone`, `support_email`
- `is_verified`, `metadata` JSONB
- Soft-delete: `is_deleted`, `deleted_at`, `deleted_by`
- Audit: `created_by`, `updated_by`

`inventory.partner_users` — join table (partner_id, user_id, access_type_id)

`inventory.stations`:
- `station_id` VARCHAR(32) PK — `STA-<nanoid(12)>` with regex check
- `partner_id` FK → partners
- `osm_id` BIGINT UNIQUE — source OSM node ID
- `name` TEXT NOT NULL
- `address` TEXT
- `location` GEOGRAPHY(Point, 4326) NOT NULL — PostGIS spatial column
- `tags` HSTORE — raw OSM key/value tags
- `status_id` FK → station_statuses
- `source_id` FK → data_sources, `source_external_id`
- `is_test` BOOLEAN NOT NULL DEFAULT FALSE — KNOWN-001 compliance
- `metadata` JSONB
- `version` BIGINT DEFAULT 1 — optimistic locking
- Soft-delete: `is_deleted`, `deleted_at`, `deleted_by`
- Audit: `created_by`, `updated_by`

`inventory.chargers`:
- `charger_id` VARCHAR(32) PK — `CHR-<nanoid(12)>` with regex check
- `station_id` FK → stations
- `serial_number`, `vendor`, `model`, `firmware_version`
- Soft-delete + audit columns

`inventory.connectors`:
- `connector_id` VARCHAR(32) PK — `CON-<nanoid(12)>` with regex check
- `charger_id` FK → chargers
- `connector_type_id`, `current_type_id`, `status_id` — FK to lookup tables
- `max_power_kw`, `min_voltage`, `max_voltage`, `min_amperage`, `max_amperage`
- `count_available`, `count_total`
- UNIQUE(charger_id, connector_type_id, current_type_id)
- Soft-delete + audit columns

ID format enforced via CHECK constraint: `CHECK (id ~ '^(STA|PAR|CHR|CON)-[A-Za-z0-9_-]{12}$')`

**Done When:**
- All extensions, lookup tables, and entity tables exist in `inventory` schema
- Entity IDs validate against nanoid(12) regex
- `location` GEOGRAPHY column passes PostGIS ST_DWithin queries
- Seed data for lookup tables populated

---

## Feature 3 — OSM Tunisia Import (Raw Ingestion)

**User Story:** As a system, I want to ingest EV charging stations from OpenStreetMap for Tunisia.

**Scope:**
- Hardcoded Tunisia bounding box (approx: 7° to 12° E, 30° to 38° N)
- Fetch OSM data via Overpass API: `node[amenity=charging_station]`
- Insert raw results into `gis.osm_charging_stations_temp`
- No filtering or enrichment logic

**Done When:**
- Running importer populates raw stations in DB
- Duplicate OSM node IDs are handled via `ON CONFLICT (osm_id) DO NOTHING`

---

## Feature 4 — Station Normalization (GIS → Inventory)

**User Story:** As a system, I want raw OSM data converted into usable station records.

**Scope:**
- Transform `gis.osm_charging_stations_temp` → `inventory.stations`
- Extract:
  - `station_id` = `'STA-' || nanoid(12)` — generated via pgcrypto
  - `name` from OSM tags
  - `location` from node lat/lng → ST_SetSRID(ST_MakePoint(lng, lat), 4326)::GEOGRAPHY
  - `tags` = hstore of all OSM tags
  - `source_id` = lookup value for 'osm'
  - `is_test` = FALSE
- Postgres function: `sync_osm_charging_stations()`
- Called by `platform/scripts/import.sh`

**Cross-schema note:** This function writes from `gis` to `inventory`. Accepted as validation-phase exception — the function runs with elevated DB privileges via `platform/scripts/import.sh`, not through a service. A future sprint will route this through admin-service for proper service mediation.

**Done When:**
- `inventory.stations` contains rows with valid `STA-<nanoid(12)>` IDs and GEOGRAPHY(Point, 4326) locations

---

## Feature 5 — Nearby Stations Query Function

**User Story:** As a driver system, I want to find stations near a location.

**Scope:**
- PostGIS function in `inventory` schema:
  ```sql
  find_nearby_stations(lat DOUBLE PRECISION, lng DOUBLE PRECISION, radius_meters DOUBLE PRECISION)
  ```
- Uses `ST_DWithin(location, ST_SetSRID(ST_MakePoint(lng, lat), 4326)::GEOGRAPHY, radius_meters)`
- Returns: `station_id`, `name`, `distance` (via `ST_Distance(location, ...)`)
- Includes `WHERE is_test = FALSE` (KNOWN-001)
- Excludes soft-deleted rows: `WHERE is_deleted = FALSE`
- Sorted by distance ascending
- Returns empty array, never null/error for no results

**Done When:**
- Function returns sorted nearby stations
- Empty results return `[]` not error
- Invalid coordinates return graceful error, not PANIC

---

## Feature 6 — Driver API Service (Core Endpoint)

**User Story:** As a frontend, I need an API to retrieve nearby stations.

**Scope:**
- Service: `driver-service` (Rust, Actix-web)
- Rust clean architecture: `api/` handlers, `application/` use-cases, `domain/` models, `infrastructure/` DB
- Endpoint: `GET /api/v1/driver/nearby?lat={lat}&lng={lng}&radius={meters}`
- Calls `find_nearby_stations()` SQL function via SQLx
- Returns JSON array: `[{station_id, name, distance}]`
- Query params validated: lat ±90, lng ±180, radius > 0
- JWT middleware placeholder (auth not enforced yet in Sprint 1 — tech debt logged)
- OpenAPI spec: `sprints/sprint-001/api/openapi.yaml`

**Done When:**
- API returns real DB results from `find_nearby_stations`
- Returns 400 for invalid params
- Returns empty `[]` when no stations nearby

---

## Feature 7 — Health Check Endpoint

**User Story:** As an operator, I need to verify service availability.

**Scope:**
- Endpoint: `GET /api/v1/driver/health`
- Checks: service alive, DB connectivity (`SELECT 1`)
- Returns: `{ "status": "ok", "db": "connected", "timestamp": "..." }`

**Done When:**
- Returns OK status reliably
- Returns 503 when DB is unreachable

---

## Feature 8 — Driver Map Web UI (Minimal Visualization)

**User Story:** As a driver, I want to see nearby charging stations on a map.

**Scope:**
- App: `source/apps/web`
- Stack: React + Tailwind CSS + Leaflet
- Map center: Tunisia (lat: 34.0, lng: 9.5)
- Fetches from `GET /api/v1/driver/nearby` with default center coordinates
- Renders station markers (no popups, no info — minimal)
- Loading spinner while fetching
- Error state with retry if API unreachable
- Empty state with message when no stations found

**Done When:**
- Stations appear on map from live API
- Loading, error, and empty states are handled

---

## Dependency Chain

```
Feature 1 (Platform DB)
  └── Feature 2 (GIS + Inventory Schema)
        └── Feature 3 (OSM Import)
              └── Feature 4 (Normalization)
                    └── Feature 5 (Nearby SQL Function)
                          ├── Feature 6 (Driver API)
                          │     └── Feature 8 (Web Map UI)
                          └── Feature 7 (Health Check)
```

---

## Sprint 001 Success Criteria

Sprint is DONE only if:

1. Docker stack (PostgreSQL 16 + PostGIS + driver-service + Traefik) runs clean
2. OSM Tunisia data is loaded into `gis.osm_charging_stations_temp`
3. Stations normalized into `inventory.stations` with valid `STA-<nanoid(12)>` IDs and PostGIS GEOGRAPHY(Point, 4326)
4. `find_nearby_stations()` function returns sorted results
5. `GET /api/v1/driver/nearby` returns real station data
6. `GET /api/v1/driver/health` returns OK
7. Map at `source/apps/web` shows station markers from live API
8. All success criteria verified by passing tests
9. `docs/SYSTEM_STATE.md` updated
10. `execution/sprints/sprint-001/review/sprint_review.md` created
