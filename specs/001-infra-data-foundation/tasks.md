---

description: "Task list for MVP-1 Sprint 0 — Infrastructure & Data Foundation"
---

# Tasks: MVP-1 Sprint 0 — Infrastructure & Data Foundation

**Input**: Design documents from `/specs/001-infra-data-foundation/`

**Prerequisites**: plan.md (required), spec.md (required), research.md

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Infrastructure project**: All files under `infra/` at repository root
- **Migrations**: `infra/migrations/`
- **OSM import**: `infra/osm-import/`

---

## Phase 1: Setup — Directory Structure

**Purpose**: Create the infrastructure directory layout

- [ ] T001 Create infra/ directory structure (docker-compose.yml, .env.example at root of infra/)
- [ ] T002 Create infra/migrations/ directory for SQL migration scripts
- [ ] T003 [P] Create infra/osm-import/ directory for OSM processing scripts

---

## Phase 2: Foundational — Docker Infrastructure Config (Blocks ALL Stories)

**Purpose**: Core Docker Compose and environment configuration. Without these files running, NO user story can be implemented.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 Create infra/.env.example template with dev defaults for platform_db credentials (`POSTGRES_DB=platform_db`, `POSTGRES_USER=bornemap`, `POSTGRES_PASSWORD=bornemap_dev`) and analytics_db credentials (`ANALYTICS_DB=analytics_db`, `ANALYTICS_USER=bornemap`, `ANALYTICS_PASSWORD=bornemap_dev`)
- [ ] T005 Create infra/docker-compose.yml with two PostgreSQL services:
  - `platform_db`: `postgis/postgis:16-3.4` image, port 5432, .env credentials, volumes for data persistence (`pgdata_platform`) and init scripts (`./migrations:/docker-entrypoint-initdb.d/`)
  - `analytics_db`: `postgres:16` image, port 5433, .env credentials, volume for data persistence (`pgdata_analytics`)
  - `healthcheck` on each service using `pg_isready -U ${POSTGRES_USER}`
- [ ] T005a Create infra/osm-import/prereqs.sh to check and install prerequisites: `which osmium || apt-get install -y osmium-tool`, `which ogr2ogr || apt-get install -y gdal-bin`, `which curl || apt-get install -y curl`

**Checkpoint**: Foundation ready — `cp .env.example .env && docker compose up -d` starts both databases

---

## Phase 3: User Story 1 — DevOps Engineer Sets Up Docker Infrastructure (Priority: P1)

**Goal**: PostgreSQL + PostGIS containers running with `inventory` and `gis` schemas, `inventory.station` table created with GEOGRAPHY column and GIST index

**Independent Test**: Run `docker compose up -d`, then exec into platform_db and verify:
- `SELECT PostGIS_version();` returns version 3.4
- `\dn` shows `inventory` and `gis` schemas
- `\d inventory.station` shows 7 columns with GIST index

### Implementation for User Story 1

- [ ] T006 [US1] Create infra/migrations/001_extensions.sql to enable PostGIS extension (`CREATE EXTENSION IF NOT EXISTS postgis;`)
- [ ] T007 [US1] Create infra/migrations/002_schema_inventory.sql with:
  - `CREATE SCHEMA IF NOT EXISTS inventory;`
  - `CREATE SCHEMA IF NOT EXISTS gis;`
  - `CREATE TABLE inventory.station (id VARCHAR(20) PRIMARY KEY, name VARCHAR(255), status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'maintenance', 'inactive')), latitude NUMERIC(10,8), longitude NUMERIC(11,8), location GEOGRAPHY(POINT, 4326) NOT NULL, created_at TIMESTAMP DEFAULT NOW());`
  - `CREATE INDEX idx_station_location ON inventory.station USING GIST(location);`
- [ ] T008 [US1] Verify both databases connect via psql using .env credentials and SQL `SELECT version();` returns PostgreSQL 16

**Checkpoint**: US1 complete — Docker infrastructure running with PostGIS enabled, schemas created, station table indexed

---

## Phase 4: User Story 2 — Data Engineer Imports OSM Station Data (Priority: P1)

**Goal**: Download Tunisia OSM extract, filter for EV charging stations, convert to SQL, seed 50–300 stations into `inventory.station`

**Independent Test**: Run `SELECT COUNT(*) FROM inventory.station WHERE status='active';` — returns >= 50 records

### Implementation for User Story 2

- [ ] T009 [P] [US2] Create infra/osm-import/download.sh to download Geofabrik Tunisia extract: `curl -L -o tunisia-latest.osm.pbf https://download.geofabrik.de/africa/tunisia-latest.osm.pbf`
- [ ] T010 [P] [US2] Create infra/osm-import/filter.sh to filter for charging stations: `osmium tags-filter tunisia-latest.osm.pbf amenity=charging_station -o charging_stations.osm.pbf`
- [ ] T011 [US2] Create infra/osm-import/transform.sh to convert filtered OSM to SQL INSERT format using ogr2ogr or Python script, outputting `INSERT INTO inventory.station (id, name, status, latitude, longitude, location) VALUES (...)` with ST_MakePoint and ST_SetSRID for GEOGRAPHY(POINT, 4326) format; include deduplication logic for duplicate OSM IDs
- [ ] T012 [US2] Create infra/migrations/003_seed_stations.sql by running transform.sh output; seed file placed in migrations/ for container startup auto-execution
- [ ] T013 [US2] Execute `docker compose down && docker compose up -d` and verify station data loads on startup with `SELECT COUNT(*) FROM inventory.station` (expect >= 50)

**Checkpoint**: US2 complete — 50–300 real OSM charging stations seeded in inventory.station with valid GEOGRAPHY points

---

## Phase 5: User Story 3 — QA Engineer Validates PostGIS Geospatial Queries (Priority: P1)

**Goal**: Validate ST_DWithin queries, distance ordering, GIST index efficiency, and <200ms latency

**Independent Test**: Run nearby query `SELECT id, name, ST_Distance(location, ST_SetSRID(ST_MakePoint(10.2, 36.8), 4326)::geography) AS dist FROM inventory.station WHERE ST_DWithin(location, ST_SetSRID(ST_MakePoint(10.2, 36.8), 4326)::geography, 5000) ORDER BY dist ASC;` — returns stations within 5km of Tunis center, ordered by distance

### Implementation for User Story 3

- [ ] T014 [US3] Create infra/osm-import/validate.sh with SQL validation queries:
  - `\d inventory.station` to verify GIST index exists
  - `SELECT PostGIS_version();` to verify PostGIS enabled
  - `SELECT COUNT(*) FROM inventory.station WHERE status='active';` (expect >= 50)
  - `SELECT COUNT(DISTINCT id), COUNT(*) FROM inventory.station;` (both equal = no duplicates)
  - `SELECT COUNT(*) FROM inventory.station WHERE location IS NULL OR name IS NULL;` (expect 0)
- [ ] T015 [US3] Add ST_DWithin query validation in validate.sh:
  - Run EXPLAIN ANALYZE of nearby query (Tunis center, 5000m radius) and confirm "Index Scan" in output
  - Run nearby query and verify result count > 0 and ordered by distance ASC
  - Run timing benchmark for ST_DWithin query and assert < 200ms
- [ ] T016 [US3] Validate GIST index effectiveness: create temp unindexed copy `CREATE TABLE inventory.station_unindexed AS SELECT * FROM inventory.station;`, run EXPLAIN ANALYZE on it, then compare with the indexed `inventory.station`; verify latency reduction > 50%

**Checkpoint**: US3 complete — PostGIS queries validated with GIST index, <200ms latency confirmed

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final touches — .gitignore entries, validation, and documentation

- [ ] T017 Add `.gitignore` entries in infra/ to exclude `.env` files and downloaded OSM `.pbf` files
- [ ] T018 End-to-end validation: run `validate.sh` from fresh `docker compose down -v && docker compose up -d` and confirm all 3 user story independent tests pass
- [ ] T019 Update specs/001-infra-data-foundation/checklists/requirements.md to mark all 12 FRs and 6 SCs as complete
- [ ] T020 Measure cold start time for `docker compose up -d` using `time docker compose up -d` and assert <120s; record result in checklists/requirements.md

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — docker-compose.yml and .env must exist
- **User Story 2 (Phase 4)**: Depends on US1 — migration scripts (001/002) must exist for station table to seed into
- **User Story 3 (Phase 5)**: Depends on US2 — seeded data required for validation queries
- **Polish (Phase 6)**: Depends on all user stories complete

### Blocking Graph

```
Setup (Phase 1)
    └── Foundational (Phase 2)
            └── US1: Docker Infrastructure (Phase 3)
                    └── US2: OSM Data Import (Phase 4)
                            └── US3: PostGIS Validation (Phase 5)
                                    └── Polish (Phase 6)
```

**Note**: Unlike typical software projects, Sprint 0 has a strictly linear dependency chain. Each story produces output consumed by the next. No parallel user story execution possible.

### Parallel Opportunities

- **Within Phase 1**: T002 and T003 can run in parallel (different directories)
- **Within Phase 4**: T009 (download.sh) and T010 (filter.sh) can run in parallel (independent scripts)
- **Within Phase 6**: All tasks can run in parallel (different files)

---

## Implementation Strategy

### MVP First (Sprint 0 Complete Only)

Since all 3 stories are P1 and linear, the MVP boundary is **the entire Sprint 0**:

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: US1
4. Complete Phase 4: US2
5. Complete Phase 5: US3
6. **STOP and VALIDATE**: Run validate.sh end-to-end
7. Sprint 0 complete — unblocks Sprint 1 (backend driver-service development)

### Sequential Execution

1. Setup directories → Docker Compose config → SQL migrations → OSM import scripts → Validation
2. Each step builds on previous; no skipping allowed
3. Validate after each user story before proceeding to next

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently verifiable
- No automated test framework for Sprint 0 (validation is manual SQL)
- Commit after each phase or logical group
- Stop at any checkpoint to validate story independently
- After Phase 5 completion, Sprint 0 is done and Sprint 1 can begin
