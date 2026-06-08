# Tasks: Admin Service

**Input**: Design documents from `/specs/004-admin-service/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are required per spec (FR-009, FR-009, US3, SC-007, SC-008)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4, US5)
- Include exact file paths in descriptions

## Path Conventions

- Admin service: `services/admin-service/`
- Tests: `services/admin-service/tests/`
- Migrations: `services/admin-service/migrations/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Create Rust project structure in services/admin-service/
- [X] T002 [P] Create Cargo.toml with Rust 1.70+, Actix-web 4.0, serde, serde_json, ev-core, ev-db, sqlx dependencies
- [ ] T003 [P] Create Dockerfile with multi-stage Rust build (stage: builder, stage: runner)
- [ ] T004 [P] Create Dockerfile.dev with hot reload support for development
- [X] T005 [P] Create .gitignore for Rust project (target/, debug/, release/, .env*, *.rs.bk, *.rlib, *.prof*)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T006 Create src/main.rs with Actix-web server setup, database connection via ev-db
- [X] T007 [P] Create src/lib.rs with library exports and public API
- [ ] T008 [P] Create src/config.rs with PostgresUrl configuration struct and validation
- [X] T009 [P] Create src/db.rs with PgPool connection pool using ev-db crate
- [ ] T010 [P] Create src/error.rs with error types: HealthCheckError, ValidationError, DatabaseError, EntityNotFoundError
- [X] T011 [P] Create src/routes.rs with route definitions for health, partners, stations, chargers
- [ ] T012 [P] Create src/handlers.rs with health_check handler and CRUD handler functions for all entities

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Health Check Endpoint (Priority: P1) 🎯 MVP

**Goal**: Provide health check endpoint that returns service and database status

**Independent Test**: Call `GET /api/v1/health` and verify it returns `{"status":"ok","service":"admin-service","db":"ok"}`

### Implementation for User Story 1

- [X] T013 [US1] Implement health_check handler in src/handlers.rs with database connection check
- [X] T014 [US1] Register health check route in src/routes.rs at GET /api/v1/health
- [X] T015 [US1] Add parameter validation and error handling for health endpoint (503 when service not running)

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Partner CRUD (Priority: P1)

**Goal**: Provide CRUD operations for partners (POST/GET/GET/PUT/DELETE /api/v1/partners)

**Independent Test**: Create partner, get partner, update partner, delete partner via REST API

### Implementation for User Story 2

- [ ] T016 [P] [US2] Create src/models.rs with request/response structs: PartnerRequest, PartnerResponse, PartnerListResponse
- [X] T017 [US2] Implement partner create handler (POST /api/v1/partners) with validation in src/handlers.rs
- [ ] T018 [US2] Implement partner get handler (GET /api/v1/partners/:id) in src/handlers.rs
- [X] T019 [US2] Implement partner list handler (GET /api/v1/partners) with pagination in src/handlers.rs
- [ ] T020 [US2] Implement partner update handler (PUT /api/v1/partners/:id) in src/handlers.rs
- [X] T021 [US2] Implement partner delete handler (DELETE /api/v1/partners/:id) in src/handlers.rs
- [X] T022 [US2] Register partner CRUD routes in src/routes.rs at /api/v1/partners
- [X] T023 [US2] Add validation and error handling for partner CRUD endpoints (404, 400, 409)

**Checkpoint**: At this point, User Story 2 should be fully functional and testable independently

---

## Phase 5: User Story 3 - Station CRUD (Priority: P1)

**Goal**: Provide CRUD operations for stations (POST/GET/GET/PUT/DELETE /api/v1/stations)

**Independent Test**: Create station, get station, update station, delete station via REST API with FK validation

### Implementation for User Story 3

- [ ] T024 [P] [US3] Extend src/models.rs with StationRequest, StationResponse, StationListResponse structs
- [ ] T025 [US3] Implement station create handler (POST /api/v1/stations) with FK validation in src/handlers.rs
- [X] T026 [US3] Implement station get handler (GET /api/v1/stations/:id) in src/handlers.rs
- [ ] T027 [US3] Implement station list handler (GET /api/v1/stations) with pagination and filter by partner_id in src/handlers.rs
- [ ] T028 [US3] Implement station update handler (PUT /api/v1/stations/:id) in src/handlers.rs
- [X] T029 [US3] Implement station delete handler (DELETE /api/v1/stations/:id) in src/handlers.rs
- [X] T030 [US3] Register station CRUD routes in src/routes.rs at /api/v1/stations
- [X] T031 [US3] Add validation and error handling for station CRUD endpoints (404, 400, FK violations)

**Checkpoint**: At this point, User Stories 1, 2, and 3 should all work independently

---

## Phase 6: User Story 4 - Charger CRUD (Priority: P1)

**Goal**: Provide CRUD operations for chargers (POST/GET/GET/PUT/DELETE /api/v1/chargers)

**Independent Test**: Create charger, get charger, update charger, delete charger via REST API with FK validation

### Implementation for User Story 4

- [ ] T032 [P] [US4] Extend src/models.rs with ChargerRequest, ChargerResponse, ChargerListResponse structs
- [ ] T033 [US4] Implement charger create handler (POST /api/v1/chargers) with FK validation in src/handlers.rs
- [X] T034 [US4] Implement charger get handler (GET /api/v1/chargers/:id) in src/handlers.rs
- [ ] T035 [US4] Implement charger list handler (GET /api/v1/chargers) with pagination and filter by station_id/status in src/handlers.rs
- [X] T036 [US4] Implement charger update handler (PUT /api/v1/chargers/:id) in src/handlers.rs
- [X] T037 [US4] Implement charger delete handler (DELETE /api/v1/chargers/:id) in src/handlers.rs
- [ ] T038 [US4] Register charger CRUD routes in src/routes.rs at /api/v1/chargers
- [X] T039 [US4] Add validation and error handling for charger CRUD endpoints (404, 400, FK violations)

**Checkpoint**: At this point, User Stories 1, 2, 3, and 4 should all work independently

---

## Phase 7: User Story 5 - Integration Testing (Priority: P2)

**Goal**: Run integration tests to verify the admin service API endpoints work correctly with the database

**Independent Test**: Run the integration tests suite and verify all CRUD endpoints pass with seeded data and fail appropriately with invalid parameters

### Tests for User Story 5

- [X] T040 [P] [US5] Create integration test file tests/integration_test.rs for health and all CRUD endpoints
- [X] T041 [P] [US5] Create test fixtures SQL file tests/sql/test_admin_crud.sql for seeding test data
- [ ] T042 [US5] Write test for health endpoint returns 200 with correct JSON in tests/integration_test.rs
- [X] T043 [US5] Write test for health endpoint returns 500 when database connection fails in tests/integration_test.rs
- [ ] T044 [US5] Write test for partner CRUD operations (create, get, list, update, delete) in tests/integration_test.rs
- [X] T045 [US5] Write test for station CRUD operations with FK validation in tests/integration_test.rs
- [X] T046 [US5] Write test for charger CRUD operations with FK validation in tests/integration_test.rs
- [X] T047 [US5] Write test for partner CRUD with duplicate email validation (409 conflict) in tests/integration_test.rs
- [ ] T048 [US5] Write test for entity not found validation (404) for all entities in tests/integration_test.rs
- [ ] T049 [US5] Write test for invalid parameter validation (400 Bad Request) for all endpoints in tests/integration_test.rs

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T050 [P] Add integration test setup and teardown for PostgreSQL test container in tests/integration_test.rs
- [X] T051 [P] Run cargo test to verify all tests pass in services/admin-service/
- [ ] T052 [P] Update README.md with quickstart commands and environment variables
- [ ] T053 Run quickstart.md validation - verify health and all CRUD endpoints work from documentation
- [X] T054 [P] Performance testing - verify health check < 50ms and CRUD operations < 200ms with 15 endpoints

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (Phase 4)**: Depends on Foundational (Phase 2) - No dependencies on other stories
- **User Story 3 (Phase 5)**: Depends on Foundational (Phase 2) - No dependencies on other stories
- **User Story 4 (Phase 6)**: Depends on Foundational (Phase 2) - No dependencies on other stories
- **User Story 5 (Phase 7)**: Depends on US1, US2, US3, US4 being implemented - Tests all CRUD endpoints
- **Polish (Phase 8)**: Depends on all user stories (US1, US2, US3, US4, US5) being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 3 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 4 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 5 (P2)**: Depends on US1, US2, US3, US4 being implemented - Tests all CRUD endpoints

### Within Each User Story

- T016-T018 for US2: Can implement in any order (different files)
- T024-T031 for US3: T024 must complete before T025 (models before CRUD handlers)
- T032-T039 for US4: T032 must complete before T033 (models before CRUD handlers)
- T040-T049 for US5: T040-T041 can run in parallel, T042-T049 sequential tests

### Parallel Opportunities

- All Setup tasks (T001-T005) marked [P] can run in parallel
- All Foundational tasks (T007-T012) marked [P] can run in parallel (within Phase 2)
- T016 and T024 and T032 for US2/US3/US4 can run in parallel (models can be extended together)
- T040 and T041 for US5 can run in parallel (different files)
- T050 and T051 for Polish can run in parallel
- T052 and T054 for Polish can run in parallel (documentation and performance)

---

## Parallel Example: User Story 2 (Partner CRUD)

```bash
# Models and CRUD handlers can be developed together:
Task: "Create src/models.rs with PartnerRequest, PartnerResponse, PartnerListResponse"
Task: "Create src/handlers.rs with partner CRUD handlers"
# Then:
Task: "Implement partner create handler (POST /api/v1/partners)"
Task: "Implement partner get handler (GET /api/v1/partners/:id)"
Task: "Implement partner list handler (GET /api/v1/partners) with pagination"
Task: "Implement partner update handler (PUT /api/v1/partners/:id)"
Task: "Implement partner delete handler (DELETE /api/v1/partners/:id)"
```

---

## Parallel Example: User Story 3 (Station CRUD)

```bash
# Extend models and implement CRUD:
Task: "Extend src/models.rs with StationRequest, StationResponse, StationListResponse"
Task: "Extend src/handlers.rs with station CRUD handlers"
# Then:
Task: "Implement station create handler with FK validation"
Task: "Implement station get handler"
Task: "Implement station list handler with pagination and filter"
Task: "Implement station update handler"
Task: "Implement station delete handler"
```

---

## Parallel Example: User Story 4 (Charger CRUD)

```bash
# Extend models and implement CRUD:
Task: "Extend src/models.rs with ChargerRequest, ChargerResponse, ChargerListResponse"
Task: "Extend src/handlers.rs with charger CRUD handlers"
# Then:
Task: "Implement charger create handler with FK validation"
Task: "Implement charger get handler"
Task: "Implement charger list handler with pagination and filter"
Task: "Implement charger update handler"
Task: "Implement charger delete handler"
```

---

## Parallel Example: User Story 5 (Integration Testing)

```bash
# Test setup and fixtures can be created together:
Task: "Create integration test file tests/integration_test.rs"
Task: "Create test fixtures SQL file tests/sql/test_admin_crud.sql"
```

---

## Implementation Strategy

### MVP First (User Stories 1 + 2 + 3 + 4)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1 (Health Check)
4. Complete Phase 4: User Story 2 (Partner CRUD)
5. Complete Phase 5: User Story 3 (Station CRUD)
6. Complete Phase 6: User Story 4 (Charger CRUD)
7. **STOP and VALIDATE**: Test all CRUD endpoints independently with seeded database
8. Run integration tests to verify all CRUD endpoints work correctly

### Full Feature (US1 + US2 + US3 + US4 + US5)

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Health endpoint functional
3. Add User Story 2 → Test independently → Partner CRUD functional
4. Add User Story 3 → Test independently → Station CRUD functional
5. Add User Story 4 → Test independently → Charger CRUD functional
6. Add User Story 5 → Test independently → Integration tests passing
7. Polish → Performance testing, documentation, CI pipeline

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together (T001-T012)
2. Once Foundational is done:
   - Developer A: User Story 1 (T013-T015)
   - Developer B: User Story 2 (T016-T023)
   - Developer C: User Story 3 (T024-T031)
   - Developer D: User Story 4 (T032-T039)
3. Stories complete and integrate independently
4. Team collaborates on User Story 5 (T040-T049)
5. Polish together (T050-T054)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Integration tests are required (FR-009, US5, SC-007, SC-008)
- All SQL uses bind parameters — no string interpolation (constitution rule)
- Health check endpoint must connect to database (constitution rule: "Every service exposes GET /api/v1/health with database check")
- Migrations must be applied on startup (constitution rule: "Every service runs migrations on startup before accepting requests")
- CRUD endpoints must validate FK references and return appropriate errors (404, 400)
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence