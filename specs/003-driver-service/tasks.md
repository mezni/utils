# Tasks: Driver Service

**Input**: Design documents from `/specs/003-driver-service/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are required per spec (FR-009, FR-009, US3, SC-007, SC-008)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- Driver service: `services/driver-service/`
- Tests: `services/driver-service/tests/`
- Migrations: `services/driver-service/migrations/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Create Rust project structure in services/driver-service/
- [X] T002 [P] Create Cargo.toml with Rust 1.70+, Actix-web 4.0, serde, serde_json, ev-core, ev-db dependencies
- [X] T003 [P] Create Dockerfile with multi-stage Rust build (stage: builder, stage: runner)
- [X] T004 [P] Create Dockerfile.dev with hot reload support for development
- [X] T005 [P] Create .gitignore for Rust project (target/, debug/, release/, .env*, *.rs.bk, *.rlib, *.prof*)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T006 Create src/main.rs with Actix-web server setup, database connection via ev-db
- [X] T007 [P] Create src/lib.rs with library exports and public API
- [X] T008 [P] Create src/config.rs with PostgresUrl configuration struct and validation
- [X] T009 [P] Create src/db.rs with PgPool connection pool using ev-db crate
- [X] T010 [P] Create src/error.rs with error types: HealthCheckError, ValidationError, DatabaseError
- [X] T011 [P] Create src/routes.rs with route definitions for health and nearby endpoints
- [X] T012 [P] Create src/handlers.rs with health_check and stations_nearby handler functions

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Health Check Endpoint (Priority: P1) 🎯 MVP

**Goal**: Provide health check endpoint that returns service and database status

**Independent Test**: Call `GET /api/v1/health` and verify it returns `{"status":"ok","service":"driver-service","db":"ok"}`

### Implementation for User Story 1

- [X] T013 [US1] Implement health_check handler in src/handlers.rs with database connection check
- [X] T014 [US1] Register health check route in src/routes.rs at GET /api/v1/health
- [X] T015 [US1] Add parameter validation and error handling for health endpoint (503 when service not running)
- [X] T016 [US1] Add logging for health check requests in src/main.rs

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Stations Nearby Endpoint (Priority: P1)

**Goal**: Provide stations nearby endpoint that queries inventory.station using spatial query and returns stations sorted by distance

**Independent Test**: Call `GET /api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=5` and verify it returns station objects with coordinates and distance

### Implementation for User Story 2

- [X] T017 [US2] Create src/models.rs with request/response structs: NearbyStationsRequest, NearbyStationsResponse, StationResponse
- [X] T018 [US2] Implement parameter validation in src/handlers.rs (lat: -90 to 90, lng: -180 to 180, radius_km: 0.1 to 100)
- [X] T019 [US2] Implement database query in src/handlers.rs using ST_DWithin(gis.station_locations.geom, point, radius*1000)
- [X] T020 [US2] Calculate distance in kilometers and return stations sorted by distance ascending in src/handlers.rs
- [X] T021 [US2] Register nearby endpoint route in src/routes.rs at GET /api/v1/stations/nearby
- [X] T022 [US2] Add validation and error handling for invalid parameters (400 Bad Request)

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Integration Testing (Priority: P2)

**Goal**: Run integration tests to verify the driver service API endpoints work correctly with the database

**Independent Test**: Run the integration tests suite and verify both endpoints pass with seeded data and fail appropriately with invalid parameters

### Tests for User Story 3

- [X] T023 [P] [US3] Create integration test file tests/integration_test.rs for health and nearby endpoints
- [X] T024 [P] [US3] Create test fixtures SQL file tests/sql/test_stations_nearby.sql for seeding test data
- [X] T025 [US3] Write test for health endpoint returns 200 with correct JSON in tests/integration_test.rs
- [X] T026 [US3] Write test for health endpoint returns 500 when database connection fails in tests/integration_test.rs
- [X] T027 [US3] Write test for nearby endpoint with valid coordinates returns stations array in tests/integration_test.rs
- [X] T028 [US3] Write test for nearby endpoint with no stations returns empty array in tests/integration_test.rs
- [X] T029 [US3] Write test for nearby endpoint with invalid parameters returns 400 Bad Request in tests/integration_test.rs
- [X] T030 [US3] Write test for nearby endpoint with database connection failure returns 500 in tests/integration_test.rs

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [X] T031 [P] Add integration test setup and teardown for PostgreSQL test container in tests/integration_test.rs
- [X] T032 [P] Run cargo test to verify all tests pass in services/driver-service/
- [X] T033 [P] Update README.md with quickstart commands and environment variables
- [X] T034 Run quickstart.md validation - verify health and nearby endpoints work from documentation
- [X] T035 [P] Performance testing - verify health check < 50ms and nearby query < 200ms with 15 stations

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (Phase 4)**: Depends on Foundational (Phase 2) - Can start after Phase 2, integrates with US1 components
- **User Story 3 (Phase 5)**: Depends on Foundational (Phase 2) - Tests both US1 and US2 endpoints
- **Polish (Phase 6)**: Depends on all user stories (US1, US2, US3) being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational (Phase 2) - Shares db.rs with US1, but independently testable
- **User Story 3 (P2)**: Depends on US1 and US2 being implemented - Tests both endpoints

### Within Each User Story

- T013-T016 for US1: Can implement in any order (different files)
- T017-T022 for US2: T017 (models) must complete before T019 (query), T018 (validation) before T022 (error handling)
- T023-T030 for US3: T023-T024 can run in parallel, T025-T030 sequential tests

### Parallel Opportunities

- All Setup tasks (T001-T005) marked [P] can run in parallel
- All Foundational tasks (T007-T012) marked [P] can run in parallel (within Phase 2)
- T023 and T024 for US3 can run in parallel (different files)
- T031 and T032 for Polish can run in parallel
- T033 and T035 for Polish can run in parallel (documentation and performance)

---

## Parallel Example: User Story 1

```bash
# Implementation can be done in any order within US1:
Task: "Create src/main.rs with Actix-web server setup, database connection via ev-db"
Task: "Create src/handlers.rs with health_check handler function"
Task: "Create src/routes.rs with route definitions for health endpoint"
```

---

## Parallel Example: User Story 2

```bash
# Models and handlers can be developed together:
Task: "Create src/models.rs with request/response structs"
Task: "Create src/handlers.rs with stations_nearby handler function"
# Then:
Task: "Implement parameter validation in src/handlers.rs"
Task: "Implement database query using ST_DWithin"
```

---

## Parallel Example: User Story 3

```bash
# Test setup and fixtures can be created together:
Task: "Create integration test file tests/integration_test.rs"
Task: "Create test fixtures SQL file tests/sql/test_stations_nearby.sql"
```

---

## Implementation Strategy

### MVP First (User Stories 1 + 2)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1 (Health Check)
4. Complete Phase 4: User Story 2 (Stations Nearby)
5. **STOP and VALIDATE**: Test both endpoints independently with seeded database
6. Run integration tests to verify both endpoints work correctly

### Full Feature (US1 + US2 + US3)

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Health endpoint functional
3. Add User Story 2 → Test independently → Stations endpoint functional
4. Add User Story 3 → Test independently → Integration tests passing
5. Polish → Performance testing, documentation, CI pipeline

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together (T001-T012)
2. Once Foundational is done:
   - Developer A: User Story 1 (T013-T016)
   - Developer B: User Story 2 (T017-T022)
3. Stories complete and integrate independently
4. Team collaborates on User Story 3 (T023-T030)
5. Polish together (T031-T035)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Integration tests are required (FR-009, US3, SC-007, SC-008)
- All SQL uses bind parameters — no string interpolation (constitution rule)
- Health check endpoint must connect to database (constitution rule: "Every service exposes GET /api/v1/health with database check")
- Migrations must be applied on startup (constitution rule: "Every service runs migrations on startup before accepting requests")
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence