# Tasks: EV Charging Platform Foundation

**Input**: Design documents from `specs/001-ev-charging-foundation/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create Rust service scaffold for driver-api in services/driver-api/ with Cargo.toml, src/main.rs
- [ ] T002 [P] Create Rust service scaffold for sync-engine in services/sync-engine/
- [ ] T003 [P] Create Rust service scaffold for ingestion in services/ingestion/
- [ ] T004 [P] Create Node.js web app scaffold in apps/web/ with package.json, index.html, tsconfig
- [ ] T005 [P] Create shared SQL migrations directory at db/migrations/ with init script
- [ ] T006 [P] Create Docker Compose with PostgreSQL 16 + PostGIS, Redis, and service stubs in docker/docker-compose.yml
- [ ] T007 [P] Create Dockerfiles for all services in docker/Dockerfile.driver-api, Dockerfile.admin-api, Dockerfile.sync-engine, Dockerfile.ingestion, Dockerfile.web
- [ ] T008 [P] Create PostGIS initialization script at docker/postgres/init.sql with postgis, hstore, pgcrypto extensions
- [ ] T009 [P] Configure Rust workspace at root Cargo.toml with shared crate for models/types

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T010 Create inventory schema migration at db/migrations/001_inventory_schema.sql (partners, stations, chargers, connectors tables with typed nanoid PKs)
- [ ] T011 [P] Create lookup tables migration at db/migrations/002_lookup_tables.sql (connector_types, current_types, data_sources, station_status, charger_status)
- [ ] T012 [P] Create sync_jobs table migration at db/migrations/003_sync_jobs.sql
- [ ] T013 [P] Create nanoid generation function in shared Rust crate at services/shared/src/nanoid.rs
- [ ] T014 [P] Create database connection pool utility in shared Rust crate at services/shared/src/db.rs
- [ ] T015 Create GiST index on stations.location in migration at db/migrations/004_indexes.sql
- [ ] T016 Create init database script at db/init.sh for deterministic schema initialization on Docker startup
- [ ] T017 Create internal Docker network configuration in docker/docker-compose.yml for all services
- [ ] T017.5 [P] Create Rust service scaffold for admin-api in services/admin-api/ with Cargo.toml, src/main.rs

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 — Driver finds nearby charging stations (Priority: P1) 🎯 MVP

**Goal**: Drivers can search for nearby stations sorted by distance with power tier information

**Independent Test**: Deploy with pre-loaded station data. Query from known coordinate — returns stations sorted by distance on map.

- [ ] T018 [P] [US1] Create mv_stations_geo materialized view at db/migrations/005_mv_stations_geo.sql with power tier, availability aggregation
- [ ] T019 [P] [US1] Create find_nearby_stations SQL function at db/functions/find_nearby_stations.sql using ST_DWithin + ST_Distance on mv_stations_geo only
- [ ] T020 [P] [US1] Implement GET /health endpoint in services/driver-api/src/routes/health.rs
- [ ] T021 [US1] Implement GET /nearby endpoint in services/driver-api/src/routes/nearby.rs calling find_nearby_stations
- [ ] T022 [US1] Implement driver API router and main server in services/driver-api/src/main.rs
- [ ] T023 [P] [US1] Create MapView component in apps/web/src/components/MapView.tsx with Leaflet map, user location tracking
- [ ] T024 [US1] Create StationList component in apps/web/src/components/StationList.tsx sorted by proximity with power tier badges
- [ ] T025 [US1] Implement API client service in apps/web/src/services/api.ts with /nearby and /stations/:id calls
- [ ] T026 [US1] Wire MapView + StationList in apps/web/src/App.tsx with /nearby data flow
- [ ] T027 [US1] Handle empty results and loading states in MapView and StationList components
- [ ] T028 [P] [US1] Create seed data script at db/migrations/006_seed_data.sql with test stations across Tunis
- [ ] T029 [P] [US1] Create refresh-mv.sh script at scripts/refresh-mv.sh
- [ ] T030 [US1] Add query latency tracking middleware in services/driver-api/src/main.rs

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 — Partner manages charging station inventory (Priority: P2)

**Goal**: Partners can register and manage stations, chargers, and connectors under their organization

**Independent Test**: Register a partner, add station with 2 chargers (2 connectors each), verify hierarchy integrity and search visibility.

- [ ] T031 [P] [US2] Create Station CRUD service in services/admin-api/src/services/stations.rs
- [ ] T032 [P] [US2] Create Charger CRUD service in services/admin-api/src/services/chargers.rs
- [ ] T033 [P] [US2] Create Connector CRUD service in services/admin-api/src/services/connectors.rs
- [ ] T034 [US2] Implement POST /stations endpoint in services/admin-api/src/routes/stations.rs
- [ ] T035 [US2] Implement PUT /stations/:id endpoint in services/admin-api/src/routes/stations.rs
- [ ] T036 [US2] Implement DELETE /stations/:id endpoint in services/admin-api/src/routes/stations.rs with FK cascade
- [ ] T037 [P] [US2] Implement charger CRUD endpoints in services/admin-api/src/routes/chargers.rs
- [ ] T038 [P] [US2] Implement connector CRUD endpoints in services/admin-api/src/routes/connectors.rs
- [ ] T039 [US2] Add validation for entity hierarchy (no orphans allowed) in services/admin-api/src/services/validators.rs
- [ ] T040 [US2] Add FK cascade enforcement in db/migrations/001_inventory_schema.sql (ON DELETE CASCADE)
- [ ] T040.5 [US2] Trigger mv_stations_geo REFRESH after station status update in services/admin-api/src/services/stations.rs

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 — System operator imports geospatial data (Priority: P3)

**Goal**: Operators can import charging station data from OSM with idempotent deduplication

**Independent Test**: Run OSM import with 50 known POIs — 50 stations created. Re-run — zero duplicates. Tracked in sync_jobs.

- [ ] T041 [P] [US3] Create OSM raw staging table at db/migrations/007_osm_staging.sql with GEOGRAPHY(Point, 4326) column
- [ ] T042 [P] [US3] Create OSM Overpass API fetcher in services/ingestion/src/fetcher.rs
- [ ] T043 [US3] Create OSM data parser in services/ingestion/src/parser.rs (POI extraction, geometry extraction)
- [ ] T044 [P] [US3] Create sync_pipeline module in services/sync-engine/src/sync/pipeline.rs
- [ ] T045 [US3] Implement idempotent upsert logic in services/sync-engine/src/sync/deduplicate.rs (by osm_id then spatial proximity)
- [ ] T046 [US3] Implement sync_jobs audit trail logging in services/sync-engine/src/sync/audit.rs
- [ ] T047 [US3] Create import-osm.sh script at scripts/import-osm.sh (fetcher → parser → sync → refresh MV)
- [ ] T048 [US3] Handle import errors with rollback in services/sync-engine/src/sync/pipeline.rs

**Checkpoint**: All user stories up to P3 should now be independently functional

---

## Phase 6: User Story 4 — Driver views station details (Priority: P4)

**Goal**: Drivers can view full station details including charger and connector breakdown

**Independent Test**: Load one station with 4 chargers (CCS and CHAdeMO). View detail page — all connectors and statuses displayed.

- [ ] T049 [P] [US4] Implement GET /stations/:id endpoint in services/driver-api/src/routes/stations.rs with chargers + connectors breakdown
- [ ] T050 [P] [US4] Create StationDetail component in apps/web/src/components/StationDetail.tsx with charger/connector breakdown
- [ ] T051 [US4] Add navigation action from StationList to StationDetail in apps/web/src/components/StationList.tsx
- [ ] T052 [US4] Handle 404 and station with no chargers in StationDetail component
- [ ] T053 [US4] Add distance indicator display in StationDetail component

**Checkpoint**: All user stories should now be independently functional

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T054 [P] Create Redis caching layer in services/driver-api/src/cache/redis.rs for /nearby responses
- [ ] T055 Add structured logging across all services using tracing crate
- [ ] T056 [P] Add error handling middleware in services/driver-api/src/middleware/error.rs
- [ ] T057 Create seed-dev.sh script at scripts/seed-dev.sh for development data
- [ ] T058 Update SYSTEM_STATE.md with completion status
- [ ] T059 Add CORS configuration for web app in services/driver-api/src/main.rs
- [ ] T060 Add rate limiting for /nearby endpoint in services/driver-api/src/middleware/rate_limit.rs
- [ ] T061 Validate all spatial queries use GiST index (EXPLAIN ANALYZE check)
- [ ] T062 Validate all API response times meet SLA (<150ms)
- [ ] T063 Implement sync_jobs retention cleanup (30-day) and listing endpoint in services/admin-api/src/routes/sync_jobs.rs

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **User Stories (Phase 3-6)**: All depend on Foundational
  - US1 (P1): No dependency on other stories — MVP
  - US2 (P2): No dependency on US1 — can be parallel
  - US3 (P3): No dependency on US1/US2 — can be parallel
  - US4 (P4): Depends on US1 (uses /nearby data flow for detail navigation)
- **Polish (Phase 7)**: Depends on desired stories being complete

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — No dependencies on other stories
- **US2 (P2)**: Can start after Foundational — Independently testable (seed data, no search needed)
- **US3 (P3)**: Can start after Foundational — Independently testable
- **US4 (P4)**: Can start after US1 (uses same API + web app scaffold)

### Parallel Opportunities

- All Phase 1 tasks marked [P] can run in parallel
- All Phase 2 tasks marked [P] can run in parallel
- US1, US2, US3 can start in parallel after Phase 2
- US4 must follow US1

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (P1)
4. **STOP and VALIDATE**: Test nearby search independently
5. Deploy/demo MVP

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add US1 (nearby search) → Test → Deploy/Demo (MVP!)
3. Add US2 (partner inventory) → Test → Deploy/Demo
4. Add US3 (OSM import) → Test → Deploy/Demo
5. Add US4 (station detail) → Test → Deploy/Demo

### Parallel Team Strategy

With multiple developers:

1. Team completes Phase 1 + Phase 2 together
2. Once Foundational is done:
   - Developer A: US1 (P1) — driver search + map
   - Developer B: US2 (P2) — partner inventory CRUD
   - Developer C: US3 (P3) — OSM ingestion pipeline
3. US4 (P4) can start after US1 completion

---

## Summary

| Phase | Story | Priority | Tasks | Independent |
|-------|-------|----------|-------|-------------|
| 1 | Setup | — | 9 | — |
| 2 | Foundational | — | 9 | — |
| 3 | Nearby search | P1 🎯 | 13 | ✅ |
| 4 | Partner inventory | P2 | 11 | ✅ |
| 5 | OSM import | P3 | 8 | ✅ |
| 6 | Station detail | P4 | 5 | ✅ (after US1) |
| 7 | Polish | — | 10 | — |
| **Total** | | | **65** | |
