# Tasks: Database — GIS and Inventory Schemas

**Input**: Design documents from `specs/002-database-gis-inventory/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Tests included where specified in the feature specification (migration apply verification, spatial query test).

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **DB migrations**: `db/migrations/`
- **DB seeds**: `db/seeds/`
- **Migration runner**: `db/migrations/migrate.sh`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Database migration and seed directories, runner script

- [X] T001 Create `db/migrations/` and `db/seeds/` directories
- [X] T002 [P] Create `db/migrations/migrate.sh` that applies `.sql` files in numeric order, accepts `DATABASE_URL`, stops on first error, exits 0 on success — gracefully handles missing/empty migration/seed directories with a clear message

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Database connection and extensions setup that MUST be complete before ANY user story

**CRITICAL**: No user story work can begin until this phase is complete

- [X] T003 Create `db/migrations/0001_extensions.sql` with `CREATE EXTENSION IF NOT EXISTS postgis`, `uuid-ossp`, and `pgcrypto`
- [X] T004 Create `db/migrations/0002_schemas.sql` with `CREATE SCHEMA IF NOT EXISTS inventory` and `CREATE SCHEMA IF NOT EXISTS gis`

**Checkpoint**: Foundation ready — PostgreSQL 16 + PostGIS 3.4 ready with schemas created

---

## Phase 3: User Story 1 — Core Inventory Schema (Priority: P1)

**Goal**: A developer runs migrations and the inventory schema is created with tables for partners, stations, chargers, and station availability, with proper constraints and indexes.

**Independent Test**: Run all migrations 0001-0004 on a fresh PostgreSQL instance. Verify `inventory.partner`, `inventory.station`, `inventory.charger`, and `inventory.station_availability` tables exist with correct columns, foreign keys, and indexes. Run migrations a second time — no errors.

### Implementation for User Story 1

- [X] T005 [P] [US1] Create `db/migrations/0003_inventory_tables.sql` with `inventory.partner` (id TEXT PK, name TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now()), `inventory.station` (id TEXT PK, partner_id TEXT NOT NULL REFERENCES inventory.partner(id), name TEXT NOT NULL, address TEXT, latitude NUMERIC(10,7) NOT NULL, longitude NUMERIC(10,7) NOT NULL, created_at TIMESTAMPTZ DEFAULT now(), updated_at TIMESTAMPTZ DEFAULT now()), `inventory.charger` (id TEXT PK, station_id TEXT NOT NULL REFERENCES inventory.station(id), connector_type TEXT NOT NULL CHECK (connector_type IN ('Type2','Type2Combo','Chademo','CCS','Schuko','Wall')), power_kw NUMERIC(6,2) NOT NULL, status TEXT NOT NULL DEFAULT 'Available' CHECK (status IN ('Available','Charging','Offline','Maintenance','Reserved','Unknown')), updated_at TIMESTAMPTZ DEFAULT now()), `inventory.station_availability` (id TEXT PK, station_id TEXT NOT NULL REFERENCES inventory.station(id), status TEXT NOT NULL CHECK (status IN ('Available','Unavailable','Partial')), updated_by TEXT, updated_at TIMESTAMPTZ DEFAULT now())
- [X] T006 [P] [US1] Create `db/migrations/0004_inventory_indexes.sql` with composite index on `station(latitude, longitude)`, and individual indexes on `station(partner_id)`, `charger(station_id)`, and `station_availability(station_id)`
- [X] T007 [US1] Verify migrations 0001-0004 apply cleanly — run `psql` against a fresh database, confirm all 4 inventory tables exist with correct columns, constraints, and indexes

**Checkpoint**: Inventory schema fully operational — partners, stations, chargers, and availability tables ready for service integration.

---

## Phase 4: User Story 2 — GIS Schema for Spatial Queries (Priority: P1)

**Goal**: A developer runs migrations and the GIS schema is created with spatial tables and GiST indexes, enabling efficient geographic queries.

**Independent Test**: Run migrations 0005-0006. Verify 6 GIS tables exist with geometry columns and GiST indexes. Run a `ST_DWithin` spatial query — confirm index usage.

### Implementation for User Story 2

- [X] T008 [P] [US2] Create `db/migrations/0005_gis_tables.sql` with `gis.osm_nodes` (osm_id BIGINT PK, tags JSONB, geom GEOMETRY(Point,4326)), `gis.osm_ways` (osm_id BIGINT PK, tags JSONB, geom GEOMETRY(LineString,4326)), `gis.roads` (id BIGSERIAL PK, osm_id BIGINT, name TEXT, road_type TEXT, geom GEOMETRY(LineString,4326)), `gis.boundaries` (id BIGSERIAL PK, osm_id BIGINT, name TEXT, admin_level INT, geom GEOMETRY(MultiPolygon,4326)), `gis.amenity_points` (id BIGSERIAL PK, osm_id BIGINT, amenity_type TEXT, name TEXT, tags JSONB, geom GEOMETRY(Point,4326)), `gis.station_locations` (station_id TEXT PK REFERENCES inventory.station(id), geom GEOMETRY(Point,4326), snapped_road_id BIGINT, region_id BIGINT, updated_at TIMESTAMPTZ DEFAULT now())
- [X] T009 [P] [US2] Create `db/migrations/0006_gis_indexes.sql` with GiST indexes on `gis.osm_nodes(geom)`, `gis.osm_ways(geom)`, `gis.roads(geom)`, `gis.boundaries(geom)`, `gis.amenity_points(geom)`, and `gis.station_locations(geom)`
- [X] T010 [US2] Verify migrations 0005-0006 apply cleanly — run against a database with inventory schema, confirm all 6 GIS tables exist with geometry columns and GiST indexes

**Checkpoint**: GIS schema fully operational — spatial queries ready for "stations nearby" feature in Driver Service.

---

## Phase 5: User Story 3 — Development Seed Data (Priority: P2)

**Goal**: A developer runs seed scripts after migrations and has realistic sample data in their local database. The seeds insert partners, stations, and chargers so the developer can test frontend and backend features immediately without connecting to a production database.

**Independent Test**: Run seed scripts against a migrated database. Verify exactly 3 partners, 15 stations, and 24 chargers are inserted and that referential integrity is maintained.

### Implementation for User Story 3

- [X] T011 [P] [US3] Create `db/seeds/dev_partners.sql` with 3 partners (names referencing Tunisian organizations, IDs using PRT- prefix) — use TRUNCATE + INSERT for idempotent re-runs
- [X] T012 [P] [US3] Create `db/seeds/dev_stations.sql` with 15 stations across Tunis (5), Sfax (3), Sousse (2), Nabeul (1), Bizerte (1), Gabès (1), Kairouan (1), Monastir (1) — each linked to a partner via NanoID, with realistic coordinates — use TRUNCATE + INSERT for idempotent re-runs
- [X] T013 [P] [US3] Create `db/seeds/dev_chargers.sql` with 24 chargers across 15 stations (1-2 per station), using connector types Type2, CCS, Chademo, Type2Combo and power ratings matching real chargers (7-350 kW), with varied statuses (mostly Available, some Charging/Offline/Maintenance) — use TRUNCATE + INSERT for idempotent re-runs
- [X] T014 [US3] Verify all 3 seed scripts apply cleanly — run against migrated database, confirm 3 partners, 15 stations, 24 chargers inserted with correct FK relationships

**Checkpoint**: Development environment populated with realistic test data.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final verification and documentation updates

- [X] T015 Run all 6 migrations from zero on a fresh database — verify under 30 seconds (SC-001)
- [X] T016 Run spatial query `ST_DWithin(gis.station_locations.geom, point, 5000)` on seeded database — verify under 100ms with index usage (SC-002)
- [X] T017 Run all 3 seed scripts — verify under 5 seconds total (SC-003)
- [X] T018 Run all 6 migrations twice in succession — verify zero errors and zero duplicate records (SC-004)
- [X] T019 Update `docs/planning/planning-bug-tracker.md` — mark Sprint 1.2 tasks (TASK-15 through TASK-24) as validated
- [X] T020 Verify `db/migrations/migrate.sh` runs standalone — confirm a new developer can set up the database with only the script and DATABASE_URL

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — creates extensions and inventory tables
- **User Story 2 (Phase 4)**: Depends on US1 completion (station_locations FK references inventory.station)
- **User Story 3 (Phase 5)**: Depends on US1 completion (station/charger seeds reference inventory tables)
- **Polish (Phase 6)**: Depends on Phase 1-5 completion

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — extensions, schemas, inventory tables, indexes
- **US2 (P1)**: Depends on US1 (needs inventory.station for GIS station_locations FK)
- **US3 (P2)**: Depends on US1 (needs inventory tables for FK references in seed data)
- US2 and US3 have no dependency on each other — can proceed in parallel once US1 is complete

### Within Each User Story

- Create migration SQL files before verifying them
- Story complete before moving to next priority

### Parallel Opportunities

- All Phase 1 tasks marked [P] can run in parallel
- All Phase 2 tasks marked [P] can run in parallel
- T005 and T006 (US1 migrations) can run in parallel
- T008 and T009 (US2 migrations) can run in parallel
- T011, T012, T013 (US3 seeds) can run in parallel
- Once US1 is complete, US2 and US3 can proceed in parallel

---

## Parallel Example: User Story 3

```bash
# Launch all seed scripts together:
Task: "Create dev_partners.sql with 3 Tunisian partners"
Task: "Create dev_stations.sql with 15 stations across Tunisia"
Task: "Create dev_chargers.sql with 24 chargers"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

 1. Complete Phase 1: Setup — db/migrations and db/seeds directories, migrate.sh
 2. Complete Phase 2: Foundational — extensions and schemas
 3. Complete Phase 3: US1 — inventory tables with indexes
 4. **STOP and VALIDATE**: Run migrations 0001-0004, verify tables exist
 5. Deploy/demo if ready

### Incremental Delivery

 1. Setup + Foundational → Database ready with extensions and schemas
 2. US1 → Inventory tables operational (MVP!)
 3. US2 → GIS spatial queries enabled
 4. US3 → Development seed data available
 5. Polish → Performance verification and docs update

### Parallel Team Strategy

With multiple developers:
 1. Complete Phase 1 + Phase 2 together
 2. Once Foundational is done: Developer A handles US1 (inventory tables + indexes)
 3. Once US1 is complete: Developer A continues to US2 (GIS), Developer B starts US3 (seeds)
 4. Final polish together

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
- Migrations are never edited after commit (constitution rule) — verify correctness before committing
