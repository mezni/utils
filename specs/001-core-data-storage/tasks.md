# Tasks: Core Data & Storage Foundations

**Input**: Design documents from `specs/001-core-data-storage/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create `source/infra/db/` and `source/infra/osm-importer/` directory structure
- [ ] T002 [P] Initialize Cargo workspace at `source/Cargo.toml` with `crates/db-models` and `crates/validation` members
- [ ] T003 [P] Create `source/.env.template` with `DB_PASSWORD=change_me` placeholder
- [ ] T003b [P] Create `source/.gitignore` with entries for `.env`, `*.env`, `target/`, and `node_modules/` to protect credentials per constitution §7

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 Create `source/infra/docker-compose.yml` with `platform_db` service (postgis/postgis:16-3.4), named volume `pgdata`, healthcheck, and port mapping
- [ ] T005 Create `source/infra/db/init.sql` with `gis` schema (empty) and `inventory` schema (empty) — tables added in user story phases

**Checkpoint**: Foundation ready — PostGIS container starts, schemas exist, user story implementation can begin

---

## Phase 3: User Story 1 - Deploy Spatially-Enabled Database with Schema (Priority: P1) 🎯 MVP

**Goal**: Running PostGIS database with `inventory.partner`, `inventory.station`, and `inventory.charger` tables.

**Independent Test**: `docker compose up -d platform_db`, connect with `psql`, verify `gis` and `inventory` schemas present with expected tables.

- [ ] T006 [US1] Define `inventory.partner` table in `source/infra/db/init.sql` with columns per data-model.md (id TEXT PK OPR_, name, contact_email, contact_phone, created_at, updated_at, deleted_at)
- [ ] T007 [US1] Define `inventory.station` table in `source/infra/db/init.sql` with columns per data-model.md (id TEXT PK STA_, partner_id FK, name, address, city, latitude, longitude, location GEOGRAPHY(Point,4326), is_private, metadata JSONB, timestamps, deleted_at)
- [ ] T008 [US1] Define `inventory.charger` table in `source/infra/db/init.sql` with columns per data-model.md (id TEXT PK CHG_, station_id FK, connector_type, power_kw, status, timestamps, deleted_at)
- [ ] T009 [P] [US1] Add GIST index on `inventory.station.location` and BTREE indexes on partner_id and station_id foreign keys in `source/infra/db/init.sql`
- [ ] T010 [US1] Create `source/.env` with `DB_PASSWORD=bornemap_dev` for local development (verify `source/.gitignore` already excludes `.env`)
- [ ] T011 [US1] Verify: `docker compose -f source/infra/docker-compose.yml up -d` completes, `\dn` shows both schemas, `\dt inventory.*` shows 3 tables

**Checkpoint**: PostGIS database running with complete schema — independently testable via psql connection

---

## Phase 4: User Story 2 - Load Tunisian Geospatial Reference Data (Priority: P2)

**Goal**: Tunisia OSM data loaded into `gis` schema with spatial indexes.

**Independent Test**: Run osm-importer, query `gis.osm_roads` for record count > 0.

- [ ] T012 [P] [US2] Create `source/infra/osm-importer/Dockerfile` based on `osgeo/gdal:ubuntu-small-latest` with osm2pgsql installed
- [ ] T013 [US2] Create `source/infra/osm-importer/import.sh`: download `tunisia-latest.osm.pbf` from Geofabrik, run osm2pgsql with custom style file, create GIST indexes on geometry columns
- [ ] T014 [P] [US2] Create `source/infra/osm-importer/osm2pgsql.style` selecting roads (highway=*), populated places (place=city/town/village), and administrative boundaries
- [ ] T015 [US2] Verify: run importer, check `gis.osm_roads` has records, spatial indexes exist on geometry columns

**Checkpoint**: Tunisia OSM reference data loaded in `gis` schema — independently testable by record count

---

## Phase 5: User Story 3 - Query Nearby Charging Stations by Distance (Priority: P3)

**Goal**: `get_nearby_stations` function returns stations sorted by geodesic distance.

**Independent Test**: Call `inventory.get_nearby_stations(10.1, 36.8, 5000)` with seed data, verify stations within 5km returned sorted.

- [ ] T016 [US3] Add seed data to `source/infra/db/init.sql`: 1 partner record (OPR_001), 10 station records across Tunis (4), Sousse (3), Sfax (3) with valid coordinates and station names
- [ ] T017 [US3] Write `inventory.get_nearby_stations(lng, lat, radius_meters)` function in `source/infra/db/init.sql` using `ST_SetSRID(ST_MakePoint(lng,lat),4326)::geography`, `ST_DWithin` filter, `ST_Distance` sort, input validation raises on invalid bounds
- [ ] T018 [US3] Verify: function returns records within radius, empty set for no-match, proper error for invalid coords, stations ordered nearest-first; validate result distances are within 1m of known true geodesic distance for a control pair; confirm query in Tunis returns no Sfax stations

**Checkpoint**: Spatial proximity query function operational — independently testable via function call with known coordinates

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Verification, documentation updates, and final validation

- [ ] T019 Update `source/docs/sprint_backlog.md` — mark Sprint 1.1 tasks complete
- [ ] T020 Update `source/docs/roadmap_status.md` — mark MVP-1 Sprint 1.1 tasks complete
- [ ] T021 Update `source/docs/system_state.md` — reflect platform_db running, all Sprint 1.1 artifacts deployed
- [ ] T022 Final verification: run all 9 acceptance scenarios from spec.md (3 US1 + 3 US2 + 3 US3) and document results

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phases 3-5)**: All depend on Foundational completion
  - **US1 (P1)**: No dependencies on other stories — start first
  - **US2 (P2)**: Depends on US1 for database availability with gis schema
  - **US3 (P3)**: Depends on US1 (station table) + US2 (gis context optional but recommended)
- **Polish (Phase 6)**: Depends on all user stories complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational — no other story dependencies
- **User Story 2 (P2)**: Requires database running (US1) — gis schema target
- **User Story 3 (P3)**: Requires station table (US1) — gis data from US2 provides context but function works without it

### Within Each User Story

- Models/tables first (DDL)
- Data/seeding next
- Functions/logic after data
- Verification at end

### Parallel Opportunities

- T002 and T003 can run in parallel (Cargo workspace + .env template)
- T009 (indexes) is parallel with T006-T008 (table definitions) — different sections of init.sql
- T012 (Dockerfile) parallel with T013-T014 (script + style) — different files
- All polish tasks (T019-T022) can run in parallel

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1
4. **STOP**: Validate database with psql — confirm schemas, tables, and connectivity
5. Proceed to US2 and US3 as capacity allows

### Incremental Delivery

1. Setup + Foundational → PostGIS container starts
2. Add US1 → Database with full inventory schema ✅ MVP (independently testable)
3. Add US2 → Tunisia OSM reference data available
4. Add US3 → Spatial proximity function operational
5. Polish → Documentation sync and final verification
