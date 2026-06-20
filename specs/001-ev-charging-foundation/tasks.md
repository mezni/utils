# Tasks: EV Charging Platform Foundation

**Input**: Design documents from `specs/001-ev-charging-foundation/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

## Phase 1: Setup & Docker Compose (Infrastructure)

**Purpose**: Project initialization, Docker Compose orchestration, and database foundation

- [ ] T001 Create Rust service scaffolds (driver-api, admin-api, sync-engine, ingestion) with Cargo.toml and src/main.rs
- [ ] T002 [P] Create Node.js web app scaffold with package.json, index.html, tsconfig, src/App.tsx
- [ ] T003 [P] Create shared SQL migrations directory and init.sql script at db/migrations/
- [ ] T004 [P] Create Docker Compose configuration in docker/docker-compose.yml with PostgreSQL, Redis, and service stubs
- [ ] T005 [P] Create Dockerfiles for all services in docker/Dockerfile.driver-api, Dockerfile.admin-api, Dockerfile.sync-engine, Dockerfile.ingestion, Dockerfile.web
- [ ] T006 [P] Create PostGIS initialization script at docker/postgres/init.sql with postgis, hstore, pgcrypto extensions
- [ ] T007 [P] Configure Rust workspace at root Cargo.toml with shared crate
- [ ] T008 Create nanoid generation utility in shared Rust crate at services/shared/src/nanoid.rs
- [ ] T009 Create database connection pool utility in shared Rust crate at services/shared/src/db.rs

**Checkpoint**: Docker Compose stack starts and PostGIS functions are available

---

## Phase 2: OSM → GIS Ingestion

**Purpose**: Import OpenStreetMap charging station data into the GIS staging layer

**Independent Test**: Run OSM import script with known dataset — verify geometries stored in staging table with correct coordinates.

- [ ] T010 [P] Create OSM raw staging table at db/migrations/002_osm_staging.sql with GEOGRAPHY(Point, 4326) column
- [ ] T011 [P] Create OSM Overpass API fetcher in services/ingestion/src/fetcher.rs (fetch from Tunisia bounding box)
- [ ] T012 Create OSM data parser in services/ingestion/src/parser.rs (extract POIs, parse OSM tags, geometry parsing)
- [ ] T013 [P] Create import-osm.sh script at scripts/import-osm.sh (fetcher → parser → staging)
- [ ] T014 Handle import errors with rollback in services/ingestion/src/main.rs

**Checkpoint**: OSM data can be fetched, parsed, and stored in staging with zero duplicates on re-run

---

## Phase 3: Inventory Schema

**Purpose**: Create the canonical EV inventory domain model with typed nanoid IDs and FK hierarchy

**Independent Test**: Apply migrations and verify all tables exist, FKs enforce integrity, and cascade deletes work correctly.

- [ ] T015 Create partners table at db/migrations/003_inventory_schema.sql (PAR- nanoid, name, type, verification_status, metadata JSONB)
- [ ] T016 [P] Create stations table at db/migrations/003_inventory_schema.sql (STA- nanoid, partner_id FK, location GEOGRAPHY(Point, 4326), address, status, tags HSTORE, osm_id, is_test)
- [ ] T017 [P] Create chargers table at db/migrations/003_inventory_schema.sql (CHR- nanoid, station_id FK ON DELETE CASCADE, vendor, model, firmware_version, max_power_kw, status)
- [ ] T018 [P] Create connectors table at db/migrations/003_inventory_schema.sql (CON- nanoid, charger_id FK ON DELETE CASCADE, connector_type, current_type, max_power_kw, status, available_count, total_count)
- [ ] T019 [P] Create sync_jobs table at db/migrations/003_inventory_schema.sql (JOB- nanoid, source_type, status, records_imported, records_updated, records_failed, error_message, started_at, completed_at)
- [ ] T020 [P] Create lookup tables migration at db/migrations/004_lookup_tables.sql (connector_types, current_types, data_sources, station_status, charger_status)
- [ ] T021 Add GiST index on stations.location at db/migrations/004_indexes.sql
- [ ] T022 Add FK cascade enforcement for station deletion in db/migrations/003_inventory_schema.sql

**Checkpoint**: All inventory tables exist with typed nanoid PKs and strict FK integrity

---

## Phase 4: Sync System + Nearby SQL Function

**Purpose**: Build the sync pipeline and spatial query function for driver features

**Independent Test**: Run sync pipeline to map staging → inventory, then execute find_nearby_stations and verify sorted results with power tiers.

- [ ] T023 [P] Create mv_stations_geo materialized view at db/migrations/005_mv_stations_geo.sql with pre-joined station data and computed power_tier
- [ ] T024 Create find_nearby_stations SQL function at db/functions/find_nearby_stations.sql using ST_DWithin + ST_Distance on mv_stations_geo only
- [ ] T025 Create sync_pipeline module in services/sync-engine/src/sync/pipeline.rs (ingestion → inventory mapping logic)
- [ ] T026 Implement idempotent upsert logic in services/sync-engine/src/sync/deduplicate.rs (ON CONFLICT DO UPDATE by osm_id then spatial proximity)
- [ ] T027 Implement sync_jobs audit trail logging in services/sync-engine/src/sync/audit.rs (record source, status, result counts, timestamps)
- [ ] T028 [P] Create refresh-mv.sh script at scripts/refresh-mv.sh
- [ ] T029 Create seed data script at db/migrations/006_seed_data.sql with test partners, stations, chargers, connectors across Tunis
- [ ] T030 Validate materialized view refresh time <2s and find_nearby_stations query <2s

**Checkpoint**: OSM data syncs into inventory and nearby queries work with sorted results and power tier classification

---

## Phase 5: Driver Service API

**Purpose**: Expose health check and nearby search endpoints for applications and the web app

**Independent Test**: Start driver service, call GET /health, then GET /nearby with coordinates and verify sorted station results.

- [ ] T031 [P] Implement GET /health endpoint in services/driver-api/src/routes/health.rs (return status, database, timestamp)
- [ ] T032 [P] Implement GET /nearby endpoint in services/driver-api/src/routes/nearby.rs calling find_nearby_stations SQL function
- [ ] T033 [P] Create query latency tracking middleware in services/driver-api/src/middleware/latency.rs
- [ ] T034 [P] Implement error handling middleware in services/driver-api/src/middleware/error.rs
- [ ] T035 Create API client service in apps/web/src/services/api.ts with /health and /nearby calls
- [ ] T036 [P] Create route handlers for /health and /nearby in services/driver-api/src/main.rs
- [ ] T037 Add structured logging across services using tracing crate
- [ ] T038 Validate API response times meet SLA (<150ms p95)

**Checkpoint**: Driver API returns health status and sorted nearby stations in under 150ms

---

## Phase 6: Driver Web Application

**Purpose**: Build the frontend map application that renders nearby stations visually

**Independent Test**: Load web app, allow location, verify station markers appear on map with badges and distance indicators.

- [ ] T039 [P] Create MapView component in apps/web/src/components/MapView.tsx with Leaflet map, user location tracking
- [ ] T040 [P] Create StationList component in apps/web/src/components/StationList.tsx sorted by proximity with power tier badges
- [ ] T041 [P] Create StationDetail component in apps/web/src/components/StationDetail.tsx with charger/connector breakdown
- [ ] T042 Wire MapView + StationList in apps/web/src/App.tsx with /nearby data flow
- [ ] T043 Handle empty results and loading states in components
- [ ] T044 Handle 404 and "no chargers configured" scenarios in StationDetail
- [ ] T045 Add distance indicators to station markers and badges
- [ ] T046 Handle geolocation permission denial gracefully
- [ ] T047 Add CORS configuration for web app in services/driver-api/src/main.rs
- [ ] T048 Create seed-dev.sh script at scripts/seed-dev.sh for development testing

**Checkpoint**: Web app renders station markers accurately on a map with distance, power tier, and availability

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1**: No dependencies — can start immediately
- **Phase 2**: Depends on Phase 1
- **Phase 3**: Depends on Phase 2 (need staging data)
- **Phase 4**: Depends on Phase 3 (need inventory tables)
- **Phase 5**: Depends on Phase 4 (need nearby function)
- **Phase 6**: Depends on Phase 5 (need API)
- **Polish**: Depends on desired stories being complete

### Parallel Opportunities

- All Phase 1 tasks marked [P] can run in parallel
- Phase 2 and 3 can be parallel (both in par after Phase 2 start)
- Phase 5 can parallelize T031-T034
- Phase 6 can parallelize T039-T041

---

## Implementation Strategy

### MVP First (Phases 1-5)

1. Complete Phase 1: Setup & Docker Compose
2. Complete Phase 2: OSM → GIS
3. Complete Phase 3: Inventory Schema
4. Complete Phase 4: Sync + Nearby
5. Complete Phase 5: Driver API
6. **STOP and VALIDATE**: Test full stack (API + seed data) works

### Full Sprint Delivery

1. Phases 1-3: Infrastructure + OSM + Schema
2. Add Phase 4: Sync + Nearby (incremental value)
3. Add Phase 5: Driver API (external interface)
4. Add Phase 6: Web app (end-user product)

---

## Summary

| Phase | Story | Tasks | Independent |
|-------|-------|-------|-------------|
| 1 | Docker + DB | 9 | — |
| 2 | OSM Ingestion | 5 | ✅ |
| 3 | Inventory Schema | 8 | ✅ |
| 4 | Sync + Nearby | 8 | ✅ |
| 5 | Driver API | 8 | ✅ |
| 6 | Web App | 10 | ✅ |
| **Total** | | **48** | |
