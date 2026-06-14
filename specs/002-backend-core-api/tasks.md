---

description: "Task list for MVP-1 Sprint 1 — Backend Core API (driver-service)"
---

# Tasks: MVP-1 Sprint 1 — Backend Core API

**Input**: Design documents from `specs/002-backend-core-api/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api.md

**Tests**: Integration tests are included per story to validate Independent Test criteria. Unit tests (mockall) included for service layer.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

- Project root: `source/services/driver-service/`
- Source: `source/services/driver-service/src/`
- Integration tests: `source/services/driver-service/tests/`

---

## Phase 1: Setup

**Purpose**: Create project skeleton and dependency configuration

- [ ] T001 Create directory structure for driver-service: `source/services/driver-service/src/lib/handlers/`, `src/lib/models/`, `src/lib/services/`, `src/lib/repositories/`, `tests/integration/`, `tests/fixtures/`
- [ ] T002 [P] Create `source/services/driver-service/Cargo.toml` with dependencies: tokio 1.52, axum 0.8, sea-orm 2.x (features: sqlx-postgres, runtime-tokio-native-tls, macros), serde 1.0 (derive), serde_json 1.0, tracing 0.1, tracing-subscriber 0.3 (env-filter), thiserror 2.0, dotenvy 0.15, utoipa 5.5 (axum), utoipa-swagger-ui 9.0 (axum), tower 0.5; dev-deps: mockall 0.14, tower 0.5
- [ ] T003 [P] Create `source/services/driver-service/rustfmt.toml` with max_width=120 and configure `.cargo/config.toml` for nightly clippy if needed

---

## Phase 2: Foundational — Database Connection & Shared Infrastructure (US4)

**Purpose**: Blocking prerequisites that ALL user stories depend on: Station model, error types, config, DB pool, test infrastructure, health endpoint

**⚠ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 [P] Create Station domain model in `source/services/driver-service/src/lib/models/station.rs` with fields: id (String), name (String), status (String), latitude (Option<f64>), longitude (Option<f64>), distance (f64). Derive Serialize, Deserialize, Debug, Clone. Add utoipa::ToSchema derive. Create models/mod.rs re-export.
- [ ] T005 [P] Create error types in `source/services/driver-service/src/error.rs`: DomainError enum with variants NotFound(String), BadRequest(String), ServiceUnavailable(String), Internal(String). Derive thiserror::Error. Implement IntoResponse for axum response mapping (404/400/503/500). Add error response JSON shape per contracts/api.md.
- [ ] T006 [P] Create Config struct in `source/services/driver-service/src/config.rs` with fields: database_url, host, port, db_pool_size, db_connect_retries, db_retry_base_delay_ms, rust_log. Implement Config::from_env() using dotenvy + std::env with defaults per research.md.
- [ ] T007 [P] Create repository trait in `source/services/driver-service/src/lib/repositories/mod.rs` with StationRepository trait (Send + Sync) defining methods: find_all, find_by_id, find_nearby. Add #[cfg_attr(test, mockall::automock)] for mock generation. Create StationRepositoryImpl struct holding DbConn. Create repositories/mod.rs re-export.
- [ ] T008 Implement `source/services/driver-service/src/main.rs`: Config::from_env(), tracing_subscriber init with EnvFilter, DB connection pool with exponential backoff retry (default 3 retries, 1s base), crash on exhaustion. Stub Router with health endpoint placeholder.
- [ ] T009 [P] Create test fixtures in `source/services/driver-service/tests/fixtures/stations.sql` with the 4 seeded stations from Sprint 0 OSM import (STA-00001 through STA-00004) matching inventory.station schema.
- [ ] T010 [US4] Implement health handler in `source/services/driver-service/src/lib/handlers/health.rs`: GET /api/v1/health returns 200 with {"status":"ok","database":"connected"} when DB pool is healthy, 503 with {"status":"error","database":"disconnected"} when DB is unreachable. Create handlers/mod.rs with module declaration.
- [ ] T011 [US4] Wire health route into Router in main.rs and create integration test in `source/services/driver-service/tests/integration/health_test.rs` using tower::oneshot to verify 200 + JSON body shape + DB connectivity field.

**Checkpoint**: Service starts, connects to platform_db, serves health endpoint. All foundation ready for user story implementation.

---

## Phase 3: User Story 1 — Station List Endpoint (Priority: P1) 🎯 MVP

**Goal**: Implement `GET /api/v1/stations` returning all stations as JSON array

**Independent Test**: `curl http://localhost:3000/api/v1/stations` returns JSON array of stations with id, name, status, latitude, longitude, distance fields. A 4-station database returns exactly 4 objects. Unavailable DB returns 503.

- [ ] T012 [US1] Implement `StationRepository::find_all` in `source/services/driver-service/src/lib/repositories/mod.rs` using SeaORM to query inventory.station. Return Vec<Station>. Map NULL name to empty string. Filter out stations with NULL latitude/longitude.
- [ ] T013 [US1] Implement StationService in `source/services/driver-service/src/lib/services/station_service.rs`: struct wrapping generic StationRepository trait (for testability via mockall). Method list_stations() calling repo.find_all(). Create services/mod.rs re-export. Add unit test using MockStationRepository.
- [ ] T014 [US1] Implement list handler in `source/services/driver-service/src/lib/handlers/stations.rs`: GET /api/v1/stations handler calling service.list_stations(). Return 200 with JSON array. Return 503 on DB error. Update handlers/mod.rs with stations module.
- [ ] T015 [US1] Wire list route into Router in main.rs. Create integration test in `source/services/driver-service/tests/integration/stations_test.rs` using tower::oneshot: verify 4 stations returned, correct field names, 503 when DB unavailable (simulated via misconfigured DB URL in test).

**Checkpoint**: Stations list endpoint functional and independently testable.

---

## Phase 4: User Story 2 — Station Detail Endpoint (Priority: P1)

**Goal**: Implement `GET /api/v1/stations/{id}` returning single station by ID

**Independent Test**: `curl http://localhost:3000/api/v1/stations/STA-00001` returns station object. `curl http://localhost:3000/api/v1/stations/NONEXISTENT` returns 404.

- [ ] T016 [US2] Implement `StationRepository::find_by_id` in `source/services/driver-service/src/lib/repositories/mod.rs` using SeaORM find_by_id on inventory.station. Return Option<Station> (None → 404). Map NULL name to empty string.
- [ ] T017 [US2] Add get_station method to StationService in `source/services/driver-service/src/lib/services/station_service.rs`: calls repo.find_by_id(id), maps None to DomainError::NotFound. Add unit test for found/not-found paths using MockStationRepository.
- [ ] T018 [US2] Add get-by-id handler in `source/services/driver-service/src/lib/handlers/stations.rs`: GET /api/v1/stations/{id} handler with axum Path param. Return 200 with station JSON, 404 for not found, 400 for invalid ID format.
- [ ] T019 [US2] Wire detail route into Router in main.rs. Add integration tests in `source/services/driver-service/tests/integration/stations_test.rs`: verify existing ID returns correct station, non-existent ID returns 404 JSON body, ID format validation.

**Checkpoint**: Station list AND detail endpoints functional. Both independently testable.

---

## Phase 5: User Story 3 — Nearby Search Endpoint (Priority: P1)

**Goal**: Implement `GET /api/v1/stations/nearby?lat={lat}&lng={lng}&radius={meters}` with PostGIS proximity search

**Independent Test**: `curl "http://localhost:3000/api/v1/stations/nearby?lat=36.8&lng=10.2&radius=50000"` returns stations within 50km of Tunis ordered by distance ascending, under 200ms. Missing params return 400.

- [ ] T020 [US3] Implement `StationRepository::find_nearby` in `source/services/driver-service/src/lib/repositories/mod.rs` using SeaORM raw SQL or query builder executing PostGIS query: `SELECT ... FROM inventory.station WHERE status = 'active' AND ST_DWithin(location, ST_SetSRID(ST_MakePoint($lng, $lat), 4326)::geography, $radius) ORDER BY ST_Distance(...) ASC`. Return Vec<Station> with computed distance field. Filter NULL coordinates.
- [ ] T021 [US3] Add find_nearby method to StationService in `source/services/driver-service/src/lib/services/station_service.rs`: validate lat (-90 to 90), lng (-180 to 180), radius (>0, default 5000). Call repo.find_nearby. Add unit tests for validation rules using MockStationRepository.
- [ ] T022 [US3] Implement nearby handler in `source/services/driver-service/src/lib/handlers/nearby.rs`: GET /api/v1/stations/nearby with axum Query params (lat, lng, optional radius). Extract and forward to service. Return 200 with array (empty if none found), 400 for invalid params. Create handlers/nearby.rs and update handlers/mod.rs.
- [ ] T023 [US3] Wire nearby route into Router in main.rs. Create integration test in `source/services/driver-service/tests/integration/nearby_test.rs`: verify proximity ordering by distance ascending, empty result for far-away query, 400 for missing lat/lng, 400 for out-of-range values, response under 200ms.

**Checkpoint**: All three data endpoints functional. Nearby search returns correct PostGIS results with proper validation and performance.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Docker packaging, API documentation, performance verification

- [ ] T024 Create `source/services/driver-service/Dockerfile` (multi-stage: rust:1.81-slim-bookworm builder → debian:bookworm-slim runtime) with HEALTHCHECK pointing at GET /api/v1/health
- [ ] T025 [P] Add utoipa OpenAPI documentation: annotate Station model, all handler functions, and Query params with #[utoipa::path(...)] attributes. Mount Swagger UI at /api/v1/docs in main.rs using utoipa-swagger-ui.
- [ ] T026 [P] Performance test: start service with platform_db, run `hyperfine --warmup 3 "curl ..."` for each endpoint. Verify <200ms response time. Document results.
- [ ] T027 Run quickstart.md validation: verify all curl commands work, all tests pass (`cargo test`), and Docker build succeeds.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 List (Phase 3)**: Depends on Foundational completion — no dependency on other stories
- **US2 Detail (Phase 4)**: Depends on Foundational completion — shares Station model and repo trait but independently implementable
- **US3 Nearby (Phase 5)**: Depends on Foundational completion — independently implementable
- **Polish (Phase 6)**: Depends on all user stories complete

### User Story Dependencies

- **US4 (DB Connection)**: Foundational — built in Phase 2
- **US1 (List) P1**: Can start after Phase 2 — No story dependencies
- **US2 (Detail) P1**: Can start after Phase 2 — No story dependencies
- **US3 (Nearby) P1**: Can start after Phase 2 — No story dependencies

All three data stories share Station model and repo trait (built in Phase 2) but add their own repository methods independently. They can be implemented in any order.

### Within Each User Story

- Models before services
- Services before handlers
- Handlers before route wiring
- Integration test validates the full stack

### Parallel Opportunities

- T002, T003: Parallel (Cargo.toml vs tooling config)
- T004, T005, T006, T007, T009: All parallel (model, errors, config, repo trait, fixtures — different files)
- T010, T011: Sequential (handler before test)
- T012-T015: Sequential within US1
- T016-T019: Sequential within US2
- T020-T023: Sequential within US3
- T025, T026: Parallel (docs vs performance tests)

---

## Parallel Example: User Story 1

```bash
# Launch model + repo trait + errors + config + fixtures in parallel:
Task: "Create Station model in src/lib/models/station.rs"
Task: "Create error types in src/error.rs"
Task: "Create Config struct in src/config.rs"
Task: "Create repository trait in src/lib/repositories/mod.rs"
Task: "Create test fixtures in tests/fixtures/stations.sql"
```

```bash
# Then implement US1 sequentially:
Task: "Implement list stations in repository"
Task: "Implement list service in station_service.rs"
Task: "Implement list handler in handlers/stations.rs"
Task: "Wire route and add integration test"
```

---

## Implementation Strategy

### MVP First (Phase 3 is MVP)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1 (List endpoint)
4. **STOP and VALIDATE**: Test US1 independently via curl
5. Deploy/demo minimal API

### Incremental Delivery

1. Setup + Foundational → Service starts, health check works
2. Add US1 (List) → Basic data API ready (MVP!)
3. Add US2 (Detail) → Station detail functional
4. Add US3 (Nearby) → Full geospatial capability
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. All complete Phase 1 + Phase 2 together
2. Once Foundational is done:
   - Developer A: US1 List endpoint
   - Developer B: US2 Detail endpoint
   - Developer C: US3 Nearby endpoint
3. All three stories complete independently (no shared source conflicts)

---

## Notes

- [P] tasks = different files, no dependencies → safe to parallelize
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Integration tests use tower::oneshot + sqlx::test (no Docker needed for tests)
- Station model shared across stories → built in Phase 2 Foundational
- Repository trait with mockall allows service unit tests without real DB
- Stop at any checkpoint to validate story independently
- Total tasks: 27 (3 setup + 8 foundational + 4 per story x3 + 4 polish)
