# Tasks: GIS Engine Foundation

**Input**: Design documents from `/specs/003-gis-engine/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: Integration tests for OSM ingestion, spatial queries, and Redis cache (as specified in spec.md Test Strategy)

**Organization**: Tasks are grouped by user scenario to enable independent implementation and testing of each scenario.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which scenario this task belongs to (e.g., SC1, SC2, SC3)
- Include exact file paths in descriptions

## Path Conventions

- Services: `services/{name}/src/`
- Migrations: `services/{name}/migrations/`
- CI Tools: `tools/`
- Contracts: `specs/003-gis-engine/contracts/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Set up database schemas, Redis cache layer, and core data types that are needed by all scenarios

- [X] T001 [P] Create migration 0003_gis_tables.up.sql in services/driver-service/migrations/ — add gis schema with osm_charging_stations_temp and osm_charging_stations tables, PostGIS geometry columns (POINT, SRID 4326), constraints, unique constraints on osm_id, validate_coordinates check constraint, valid_geom check constraint
- [X] T002 [P] Create migration 0004_materialized_views.up.sql in services/driver-service/migrations/ — add mv_stations_geo and mv_stations_summary materialized views, create unique indexes, set up refresh schedule (pg_cron for hourly refresh at 2 AM UTC), CREATE EXTENSION IF NOT EXISTS pg_cron
- [X] T003 [P] Create services/driver-service/src/middleware/spatial.rs — spatial query helper functions (ST_Distance, ST_MakePoint, ST_Within), validate radius (100-100000 meters), validate coordinates (-90 to 90, -180 to 180)
- [X] T004 [P] Create services/driver-service/src/telemetry/ingestion.rs — OSM ingestion service with HTTP client for overpass-api.de, idempotency key generation, batch processing support, error handling
- [X] T005 [P] Create services/driver-service/src/redis/spatial_cache.rs — Redis spatial cache with cache key pattern geo:radius:{lat}:{lon}:{radius}, TTL configuration (default 5 minutes), read/write operations, cache invalidation logic
- [X] T006 [P] Create services/driver-service/src/domain/types/gis.rs in apps/packages/domain-types/src — define GIS DTOs (Station, StationDetail, StationList, StationSearchQuery) matching API contracts with serde Serialize/Deserialize

**Checkpoint**: Database schemas exist with PostGIS geometry columns, Redis cache layer operational with flat key pattern, core data types defined in domain-types

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Create blocking dependencies for all scenarios (PostGIS spatial query engine, OSM ingestion pipeline, ETL pipeline, CI gates)

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T007 [P] Implement services/driver-service/src/queries/spatial.rs — spatial query builder with SQLx compile-time verification, circular radius queries, bounding box queries, validate radius and coordinates, return results ordered by distance
- [X] T008 [P] Implement services/driver-service/src/queries/nearest.rs — nearest neighbor queries with ST_Distance ordering, result limit support, SQLx compile-time verification, no raw SQL string construction
- [X] T009 [P] Implement services/driver-service/src/queries/bbox.rs — bounding box queries with ST_Within, support pagination parameters (page, limit, lat, lon, radius for viewport), SQLx compile-time verification
- [X] T010 [P] Create services/driver-service/src/handlers/stations.rs — GET /api/v1/driver/stations handler with pagination, integrate spatial queries, validate RBAC, return StationList matching domain-types
- [X] T011 [P] Create services/driver-service/src/handlers/nearby.rs — GET /api/v1/driver/nearby handler with spatial query engine, validate radius and coordinates, call Redis cache first, return StationList with distance field
- [X] T012 [P] Implement services/driver-service/src/ingestion/osm_parser.rs — parse OSM XML, extract charging station data, validate required fields, handle multiple connector types, error handling
- [X] T013 [P] Implement services/driver-service/src/ingestion/tag_normalizer.rs — map OSM tags to internal schema (amenity, power, connector_types, address), store rare tags in JSONB, validate power values
- [X] T014 [P] Implement services/driver-service/src/ingestion/staging_upsert.rs — upsert to osm_charging_stations_temp table with idempotency key based on osm_id, mark as processed=False, handle duplicates
- [X] T015 [P] Implement services/driver-service/src/ingestion/deduplication.rs — check for duplicate osm_id, prevent duplicate ingests, idempotency enforcement, log events
- [X] T016 [P] Create services/driver-service/src/api/ingestion.rs — POST /api/v1/gis/ingest endpoint, trigger ingestion job, return job_id, integrate with telemetry events
- [X] T017 [P] Implement services/driver-service/src/etl/validation.rs — validate OSM tags against business rules, validate coordinates, check connector types, fail fast on invalid data
- [X] T018 [P] Implement services/driver-service/src/etl/normalization.rs — normalize tags to internal fields, create gis.osm_charging_stations record from osm_charging_stations_temp, handle multiple connector types
- [X] T019 [P] Implement services/driver-service/src/etl/approval.rs — admin review workflow (approve/reject stations), store approval status, update curated table, ETL moves approved stations
- [X] T020 [P] Create services/driver-service/src/handlers/ingestion.rs — GET /api/v1/gis/ingest/status/{job_id} endpoint, return job status (pending/processing/completed/failed), integrate with analytics_db

**Checkpoint**: Foundation ready - all blocking tasks complete (spatial queries, OSM ingestion, ETL pipeline functional)

---

## Phase 3: User Story 1 - Driver Finds Nearby Chargers

**Goal**: Driver service can find charging stations within a radius using PostGIS spatial queries, with Redis caching for performance

**Independent Test**: Query `GET /api/v1/driver/nearby?lat=40.7829&lon=-73.9654&radius=1000`, verify stations within 1km, response time < 500ms (without cache), < 50ms (with cache)

### Implementation for Scenario 1

- [X] T021 [P] [SC1] Implement services/driver-service/src/queries/cache.rs — spatial cache wrapper that checks Redis before executing PostGIS query, cache hit → return cached results, cache miss → execute query and store results, validate cache TTL
- [X] T022 [P] [SC1] Update services/driver-service/src/handlers/nearby.rs — add Redis cache integration, validate cache TTL configuration, implement cache invalidation on write, return StationList with distance field, integrate with JWT auth
- [X] T023 [P] [SC1] Update services/driver-service/src/handlers/stations.rs — add Redis cache integration for station list with pagination, return StationList matching domain-types, integrate with spatial queries

**Checkpoint**: Scenario 1 fully functional - spatial queries return accurate results (< 500ms without cache, < 50ms with cache)

---

## Phase 4: User Story 2 - Mobile App Displays Map

**Goal**: Map rendering API exposes station markers, supports clustering, and handles network failures gracefully

**Independent Test**: Mobile app fetches map tiles and station markers from driver service, verifies station coordinates, connector types, availability status, clustering groups dense markers (>50 markers)

### Implementation for Scenario 2

- [X] T024 [P] [SC2] Update services/driver-service/src/handlers/nearby.rs — add clustering support (marker density threshold >50), optimize response for mobile app (reduce payload size, include only necessary fields), handle network failures gracefully
- [X] T025 [P] [SC2] Update services/driver-service/src/handlers/stations.rs — optimize station detail endpoint for mobile map rendering, return StationDetail with coordinates, connector_types, is_available, error handling for 404
- [X] T026 [P] [SC2] Create services/driver-service/src/handlers/ingestion.rs — GET /api/v1/gis/ingest/status/{job_id} endpoint, return job status with station count, integration with analytics_db

**Checkpoint**: Scenario 2 fully functional - map renders correctly with station markers, clustering enabled, popups show accurate station information

---

## Phase 5: User Story 3 - Admin Verifies Station Data

**Goal**: Admin can trigger OSM batch import, verify ingestion produces consistent results, and approve stations for curated table

**Independent Test**: Admin calls ingestion API to trigger OSM batch import, system processes OSM XML file, extracts charging station data, normalizes tags, staging table populated with raw data, admin reviews and approves stations, ETL pipeline moves approved stations to curated table

### Implementation for Scenario 3

- [X] T027 [P] [SC3] Complete services/driver-service/src/ingestion/osm_parser.rs — parse OSM XML, extract charging station data with all tags, validate required fields, handle multiple connector types, error handling for malformed XML
- [X] T028 [P] [SC3] Complete services/driver-service/src/ingestion/tag_normalizer.rs — map OSM tags to internal schema (amenity, power, connector_types, address), store rare tags in JSONB, validate power values (7kW, 22kW, 50kW), validate connector types (Type 2, CCS, CHAdeMO)
- [X] T029 [P] [SC3] Complete services/driver-service/src/ingestion/staging_upsert.rs — upsert to osm_charging_stations_temp table with idempotency key based on osm_id, mark as processed=False, handle duplicates, log import events
- [X] T030 [P] [SC3] Complete services/driver-service/src/ingestion/deduplication.rs — check for duplicate osm_id, prevent duplicate ingests, idempotency enforcement, log reproducibility events, validate deterministic behavior
- [X] T031 [P] [SC3] Complete services/driver-service/src/etl/validation.rs — validate OSM tags against business rules, validate coordinates, check connector types, fail fast on invalid data, log validation events
- [X] T032 [P] [SC3] Complete services/driver-service/src/etl/normalization.rs — normalize tags to internal fields, create gis.osm_charging_stations record from osm_charging_stations_temp, handle multiple connector types, map amenity types
- [X] T033 [P] [SC3] Complete services/driver-service/src/etl/approval.rs — admin review workflow (approve/reject stations), store approval status in curated table, ETL moves approved stations, update last_updated timestamp

**Checkpoint**: Scenario 3 fully functional - OSM ingestion is deterministic and idempotent, staging and curated tables contain correct data, spatial queries work on curated data

---

## Phase 6: CI Security Gates

**Purpose**: Implement 5 CI gates to enforce GIS security policies (ownership, query safety, Redis access, OSM reproducibility, API contract)

- [X] T034 [P] Create tools/ci_gate_gis_ownership.sh — fail if any service other than driver-service writes to gis schema, validate database roles, check schema ownership
- [X] T035 [P] Create tools/ci_gate_spatial_query_safety.sh — fail if raw SQL string construction in spatial queries or non-SQLx queries in driver-service, validate all queries use SQLx compile-time verification
- [X] T036 [P] Create tools/ci_gate_redis_access.sh — fail if Redis accessed outside driver-service, validate Redis integration only in driver-service
- [X] T037 [P] Create tools/ci_gate_osm_reproducibility.sh — fail if ingestion pipeline is non-deterministic or missing idempotency key, validate idempotency enforcement
- [X] T038 [P] Create tools/ci_gate_map_api_contract.sh — fail if API response deviates from domain-types contracts, validate response format matches DTO definitions

**Checkpoint**: All 5 CI gates implemented and passing

---

## Phase 7: Documentation & Testing

**Purpose**: Complete documentation, integration tests, and verify exit criteria

- [X] T039 [P] Update services/driver-service/src/main.rs — wire spatial queries and ingestion endpoints, integrate JWT auth, integrate RBAC, wire CI gates
- [X] T040 [P] Create tests/integration_gis_spatial_queries.rs — unit tests for spatial queries (radius, bbox, nearest) with PostgreSQL test fixtures, verify performance targets (< 500ms without cache, < 50ms with cache)
- [X] T041 [P] Create tests/integration_gis_ingestion.rs — test OSM ingestion is deterministic and idempotent, test tag normalization, test ETL pipeline, verify staging and curated tables
- [X] T042 [P] Create tests/integration_gis_redis.rs — test Redis cache read/write, cache hit/miss scenarios, cache invalidation, verify cache isolation to driver-service
- [X] T043 [P] Update docs/SYSTEM_STATE.md — add GIS engine sections with schema definitions (osm_charging_stations_temp, osm_charging_stations, materialized views), API contracts, performance targets, Redis cache key pattern
- [X] T044 [P] Update docs/sprints/sprint_02/review/sprint_review.md — document implementation progress, exit criteria status, test results, challenges and mitigations

**Checkpoint**: All integration tests passing, documentation complete, exit criteria verified

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-5)**: All depend on Foundational phase completion
- **CI Gates (Phase 6)**: Can run in parallel with User Stories after Foundational
- **Documentation & Testing (Phase 7)**: Depends on all previous phases - final validation

### User Story Dependencies

- **Scenario 1 (Driver Finds Nearby Chargers)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **Scenario 2 (Mobile App Displays Map)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **Scenario 3 (Admin Verifies Station Data)**: Can start after Foundational (Phase 2) - No dependencies on other stories

**All user stories are independently testable and can be implemented in parallel after Foundational phase completes.**

### Within Each Phase

- Parallelizable tasks (marked [P]) can be executed simultaneously
- Non-parallelizable tasks depend on previous tasks within the phase
- Stories are fully independent of each other

### Parallel Opportunities

- **Phase 1**: All 6 tasks marked [P] can run in parallel (database migrations, core types, cache layer)
- **Phase 2**: All 14 tasks marked [P] can run in parallel (queries, ingestion, ETL, handlers)
- **Phase 3**: All 3 tasks marked [P] can run in parallel (cache integration)
- **Phase 4**: All 3 tasks marked [P] can run in parallel (map rendering optimization)
- **Phase 5**: All 7 tasks marked [P] can run in parallel (ingestion and ETL completion)
- **Phase 6**: All 5 tasks marked [P] can run in parallel (CI gates)
- **Phase 7**: All 6 tasks marked [P] can run in parallel (tests, documentation)
- **Once Phase 2 completes, ALL 3 user stories can start in parallel** (if team capacity allows)

## Sprint 2 Task Summary

**Total Tasks**: 44

**Task Count per Phase**:
- Phase 1 (Setup): 6 tasks
- Phase 2 (Foundational): 14 tasks
- Phase 3 (Scenario 1): 3 tasks
- Phase 4 (Scenario 2): 3 tasks
- Phase 5 (Scenario 3): 7 tasks
- Phase 6 (CI Gates): 5 tasks
- Phase 7 (Testing & Documentation): 6 tasks

**Parallelizable Tasks**: 34 out of 44 (77%)

**Parallel Opportunities**:
- Phase 1: 6 parallelizable tasks (100%)
- Phase 2: 14 parallelizable tasks (100%)
- Phase 3: 3 parallelizable tasks (100%)
- Phase 4: 3 parallelizable tasks (100%)
- Phase 5: 7 parallelizable tasks (100%)
- Phase 6: 5 parallelizable tasks (100%)
- Phase 7: 6 parallelizable tasks (100%)
- **Total Parallelizable Tasks**: 34 out of 44 (77%)

## Independent Test Criteria

- **Setup Complete**: Database schemas exist with PostGIS geometry columns, Redis cache operational with cache key pattern, core data types defined in domain-types
- **Foundational Complete**: Spatial queries return correct results (< 500ms without cache, < 50ms with cache), OSM ingestion pipeline is deterministic and idempotent, ETL pipeline processes staging data with validation and approval
- **Scenario 1 Complete**: Spatial queries return accurate results within performance targets, `/nearby` endpoint returns correct results ordered by distance, cache hit rate > 30%
- **Scenario 2 Complete**: Map renders correctly with station markers, clustering groups dense markers (>50 markers), marker popups show accurate station information, app handles network failures gracefully
- **Scenario 3 Complete**: OSM ingestion produces identical output for identical input, uses idempotency keys, staging and curated tables match, spatial queries work correctly on curated data
- **CI Gates Complete**: All 5 CI gates implemented and passing (GIS ownership, spatial query safety, Redis access, OSM reproducibility, map API contract)
- **Testing & Documentation Complete**: All integration tests passing, SYSTEM_STATE.md updated with GIS sections, sprint review documented

## MVP Scope (Recommended First Sprint)

**MVP (Phase 1-2)**:
- PH1: GIS schemas, Redis cache, core data types (6 tasks)
- PH2: Spatial queries, endpoints, OSM ingestion, ETL pipeline (14 tasks)
- **Total MVP**: 20 tasks, ~3-4 days

**Post-MVP** (Phase 3-7):
- PH3: Redis cache integration for spatial queries (3 tasks)
- PH4: Map rendering optimization (3 tasks)
- PH5: Ingestion and ETL completion (7 tasks)
- PH6: CI security gates (5 tasks)
- PH7: Testing & documentation (6 tasks)
- **Total Post-MVP**: 24 tasks, ~4-5 days

## Exit Criteria

**Phase 1 Complete**: Database schemas exist with PostGIS geometry columns, Redis cache operational with cache key pattern, core data types defined in domain-types

**Phase 2 Complete**: Spatial queries return correct results (< 500ms without cache, < 50ms with cache), OSM ingestion is deterministic and idempotent, ETL pipeline processes staging data with validation and approval

**Phase 3 Complete**: Spatial queries return accurate results within performance targets, `/nearby` endpoint returns correct results ordered by distance, cache hit rate > 30%

**Phase 4 Complete**: Map renders correctly with station markers, clustering groups dense markers (>50 markers), marker popups show accurate station information, app handles network failures gracefully

**Phase 5 Complete**: OSM ingestion produces identical output for identical input, uses idempotency keys, staging and curated tables match, spatial queries work correctly on curated data

**Phase 6 Complete**: All 5 CI gates implemented and passing (GIS ownership, spatial query safety, Redis access, OSM reproducibility, map API contract)

**Phase 7 Complete**: All integration tests passing, SYSTEM_STATE.md updated with GIS sections, sprint review documented

**Sprint 2 Complete**:
- [ ] OSM ingestion works deterministically (batch, idempotent)
- [ ] PostGIS queries return correct spatial results
- [ ] `/nearby` endpoint fully functional with contract enforced
- [ ] Redis caching operational and isolated to driver-service
- [ ] GIS ownership gate passes
- [ ] Spatial query safety enforced
- [ ] Ingestion reproducibility verified
