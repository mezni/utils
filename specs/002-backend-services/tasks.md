# Tasks: Backend Services

**Input**: Design documents from `specs/002-backend-services/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Cargo workspace and project initialization

- [ ] T001 Create Cargo workspace at `source/services/Cargo.toml` with resolver = "2" and members [shared/ev-core, shared/ev-db, shared/ev-auth, driver-service, admin-service]
- [ ] T002 [P] Create `source/services/rust-toolchain.toml` pinning channel = "1.80"
- [ ] T003 [P] Create `source/services/shared/ev-core/Cargo.toml` and `src/lib.rs` with nanoid, serde, chrono, thiserror dependencies
- [ ] T004 [P] Create `source/services/shared/ev-db/Cargo.toml` and `src/lib.rs` with sqlx, ev-core path dependency
- [ ] T005 [P] Create `source/services/shared/ev-auth/Cargo.toml` and `src/lib.rs` as stub crate
- [ ] T006 [P] Create `source/services/driver-service/Cargo.toml` with workspace dep inheritance
- [ ] T007 [P] Create `source/services/admin-service/Cargo.toml` with workspace dep inheritance

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared domain types, error handling, DB helpers, and infrastructure that ALL three user stories depend on

- [ ] T008 [P] Implement ev-core domain types in `source/services/shared/ev-core/src/station.rs`, `charger.rs`, `partner.rs`, `event.rs` with serde Serialize/Deserialize
- [ ] T009 [P] Implement `AppError` enum with `ResponseError` impl in `source/services/shared/ev-core/src/error.rs` — maps to consistent JSON error shape with code, message, details
- [ ] T010 [P] Implement ID generation in `source/services/shared/ev-core/src/id.rs` with `EntityPrefix` enum (STA, CHR, PRT) and `generate_entity_id()` using nanoid 0.4
- [ ] T011 [P] Implement DB pool creation helper in `source/services/shared/ev-db/src/pool.rs` with PgPoolOptions config (max_connections=20, acquire_timeout=5s)
- [ ] T012 [P] Implement test database setup helper in `source/services/shared/ev-db/src/test_db.rs` — drop/create/migrate/seed for contract tests
- [ ] T013 [P] Write contract test for `/health` endpoint (both services) in `source/services/driver-service/tests/contract_health.rs` and `source/services/admin-service/tests/contract_health.rs`
- [ ] T014 [P] Implement RequestId middleware in `source/services/driver-service/src/middleware/mod.rs` and `source/services/admin-service/src/middleware/mod.rs`
- [ ] T015 [P] Create multi-stage Dockerfiles at `infra/docker/driver-service.Dockerfile` and `infra/docker/admin-service.Dockerfile` with cargo-chef, SQLX_OFFLINE=true, lld linker
- [ ] T016 Update `infra/docker-compose.yml` with driver-service and admin-service service definitions, depends_on DB health checks

---

## Phase 3: User Story 1 - Station Discovery via Driver Service (Priority: P1) 🎯 MVP

**Goal**: Mobile app users can browse stations (list, nearby search, detail) via driver-service API

**Independent Test**: Start driver-service, call `GET /api/v1/stations/nearby?lat=36.8&lng=10.18&radius=10` and verify it returns stations sorted by distance with coordinates and chargers

### Tests for User Story 1

- [ ] T017 [P] [US1] Write contract test for `GET /api/v1/stations` paginated list in `source/services/driver-service/tests/contract_stations.rs` — validate status 200, data array, total, page fields
- [ ] T018 [P] [US1] Write contract test for `GET /api/v1/stations/nearby` in `source/services/driver-service/tests/contract_stations.rs` — validate status 200, data sorted by distance_km, total field
- [ ] T019 [P] [US1] Write contract test for `GET /api/v1/stations/{id}` in `source/services/driver-service/tests/contract_stations.rs` — validate status 200, full station with chargers
- [ ] T020 [P] [US1] Write contract test for stations 404 in `source/services/driver-service/tests/contract_stations.rs` — validate error.code = "NOT_FOUND"
- [ ] T021 [P] [US1] Write contract test for nearby invalid lat/lng in `source/services/driver-service/tests/contract_stations.rs` — validate 400 with VALIDATION_ERROR

### Implementation for User Story 1

- [ ] T022 [P] [US1] Implement station query helpers in `source/services/shared/ev-db/src/queries/stations.rs` with list_all, find_by_id, find_nearby (ST_DWithin + ST_Distance with geography cast)
- [ ] T023 [US1] Implement `list_stations` handler in `source/services/driver-service/src/routes/stations.rs` with pagination (page, per_page params)
- [ ] T024 [US1] Implement `nearby_stations` handler in `source/services/driver-service/src/routes/stations.rs` with lat/lng/radius validation and distance ordering
- [ ] T025 [US1] Implement `get_station` handler in `source/services/driver-service/src/routes/stations.rs` with charger array nesting
- [ ] T026 [US1] Wire route configuration in `source/services/driver-service/src/routes/mod.rs` using web::scope("/api/v1") with correct route ordering (nearby before {id})
- [ ] T027 [US1] Implement `main.rs` in `source/services/driver-service/src/main.rs` with AppState (db pool, config, startup time), middleware, graceful shutdown

**Checkpoint**: Driver-service fully functional, all 5 discovery contract tests pass

---

## Phase 4: User Story 2 - Station Management via Admin Service (Priority: P1)

**Goal**: Platform operators can create, update, and soft-delete stations via admin-service API

**Independent Test**: Start admin-service, create station via `POST /api/v1/stations`, fetch it via `GET /api/v1/stations/{id}` on driver-service, update via PUT, delete via DELETE, verify it disappears from discovery

### Tests for User Story 2

- [ ] T028 [P] [US2] Write contract test for `POST /api/v1/stations` in `source/services/admin-service/tests/contract_stations.rs` — validate 201 with generated id and chargers
- [ ] T029 [P] [US2] Write contract test for `PUT /api/v1/stations/{id}` in `source/services/admin-service/tests/contract_stations.rs` — validate 200 with updated fields only
- [ ] T030 [P] [US2] Write contract test for `DELETE /api/v1/stations/{id}` in `source/services/admin-service/tests/contract_stations.rs` — validate 204, then verify 404 on subsequent GET
- [ ] T031 [P] [US2] Write contract test for station create validation errors in `source/services/admin-service/tests/contract_stations.rs` — validate 400 with field-level details
- [ ] T032 [P] [US2] Write contract test for station create with unknown partner_id in `source/services/admin-service/tests/contract_stations.rs` — validate 400 BAD_REQUEST

### Implementation for User Story 2

- [ ] T033 [P] [US2] Implement station write query helpers in `source/services/shared/ev-db/src/queries/stations.rs` — insert_station_with_chargers (transactional), update_station (partial), soft_delete_station
- [ ] T034 [US2] Implement `create_station` handler in `source/services/admin-service/src/routes/stations.rs` with station + chargers in single request, partner_id validation, nanoid generation
- [ ] T035 [US2] Implement `update_station` handler in `source/services/admin-service/src/routes/stations.rs` with partial update (only provided fields)
- [ ] T036 [US2] Implement `delete_station` handler in `source/services/admin-service/src/routes/stations.rs` — sets deleted_at timestamp
- [ ] T037 [US2] Wire admin-service route configuration in `source/services/admin-service/src/routes/mod.rs` with web::scope("/api/v1")
- [ ] T038 [US2] Implement `main.rs` in `source/services/admin-service/src/main.rs` with AppState (platform_db pool, analytics_db pool), middleware, graceful shutdown

**Checkpoint**: Admin-service station CRUD fully functional, all 5 management contract tests pass. Cross-service test: create via admin, discover via driver.

---

## Phase 5: User Story 3 - Event Ingestion via Admin Service (Priority: P2)

**Goal**: Product analysts can capture user interactions via single and batch event endpoints

**Independent Test**: Send events via `POST /api/v1/events` and `POST /api/v1/events/batch`, then query analytics_db directly to verify persistence

### Tests for User Story 3

- [ ] T039 [P] [US3] Write contract test for `POST /api/v1/events` in `source/services/admin-service/tests/contract_events.rs` — validate 201 with event id and required fields
- [ ] T040 [P] [US3] Write contract test for `POST /api/v1/events/batch` (valid batch) in `source/services/admin-service/tests/contract_events.rs` — validate 201 with ingested count
- [ ] T041 [P] [US3] Write contract test for batch exceeding 100 events in `source/services/admin-service/tests/contract_events.rs` — validate 400 with batch size message
- [ ] T042 [P] [US3] Write contract test for all-or-nothing batch rejection in `source/services/admin-service/tests/contract_events.rs` — one invalid event among 100 → 0 persisted
- [ ] T043 [P] [US3] Write contract test for event missing required fields in `source/services/admin-service/tests/contract_events.rs` — validate 400 VALIDATION_ERROR

### Implementation for User Story 3

- [ ] T044 [P] [US3] Implement event write helpers in `source/services/shared/ev-db/src/queries/events.rs` — insert_single_event, insert_batch_events (transactional), validate_event helper
- [ ] T045 [US3] Implement `ingest_event` handler in `source/services/admin-service/src/routes/events.rs` with event_type/session_id/occurred_at validation
- [ ] T046 [US3] Implement `ingest_batch` handler in `source/services/admin-service/src/routes/events.rs` with 100-event limit check and all-or-nothing validation

**Checkpoint**: Event ingestion fully functional, all 5 event contract tests pass. Analytics DB append-only rules verified.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Performance validation, documentation, and final verification

- [ ] T047 Add unit tests across shared crates and services to meet 80%+ coverage target — `cargo tarpaulin --workspace`
- [ ] T048 Write integration test in `source/services/driver-service/tests/e2e_contract_driver.rs` and `source/services/admin-service/tests/e2e_contract_admin.rs` (reqwest, #[ignore] by default) verifying cross-service contract: admin creates station → driver discovers it
- [ ] T049 Update `AGENTS.md` — add tasks.md reference and any new conventions
- [ ] T050 Performance validation: run quickstart.md end-to-end, verify SC-001 (100ms p95), SC-002 (200ms create), SC-004 (500ms batch)
- [ ] T051 Run `cargo clippy --workspace` and fix all warnings

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — can run in parallel with Phase 4 and Phase 5
- **User Story 2 (Phase 4)**: Depends on Foundational — shares ev-db query module with US1 (cooperative, not blocking)
- **User Story 3 (Phase 5)**: Depends on Foundational — independent of US1 and US2 (different DB, different query module)
- **Polish (Phase 6)**: Depends on all user stories complete

### User Story Dependencies

- **US1 (Station Discovery - P1)**: No dependencies on other stories. Complete standalone.
- **US2 (Station Management - P1)**: Shares `stations.rs` query module with US1 but reads and writes are separate functions. No blocking dependencies.
- **US3 (Event Ingestion - P2)**: Completely independent — uses analytics_db, not platform_db.

### Parallel Opportunities

- All Phase 1 [P] tasks can run concurrently (T002-T007)
- All Phase 2 [P] tasks can run concurrently (T008-T015)
- Once Phase 2 is done, US1, US2, and US3 implementation can run in parallel across different services
- All test tasks within a user story marked [P] can run in parallel

### Parallel Example: User Story 1

```bash
# Launch all contract tests together (they fail initially):
cargo test -p driver-service --test contract_stations -- contract_all

# Launch all model + query helpers together:
Task: "Implement station query helpers in ev-db/src/queries/stations.rs"
```

### Parallel Example: All Three Stories

```bash
# Once Phase 2 is complete:
# Agent A: Phase 3 (US1 — driver-service discovery)
# Agent B: Phase 4 (US2 — admin-service management, platform_db)
# Agent C: Phase 5 (US3 — admin-service events, analytics_db)
```

---

## Implementation Strategy

### MVP First (US1 + US2 — Both P1)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks everything)
3. Complete Phase 3 (US1): Driver-service discovery API
4. Complete Phase 4 (US2): Admin-service station CRUD
5. **STOP and VALIDATE**: Creation → discovery lifecycle test
6. Complete Phase 5 (US3): Event ingestion
7. Complete Phase 6: Polish

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add US1 (station discovery) → test independently → deploy/show (driver-service live)
3. Add US2 (station management) → test cross-service → deploy/show (admin-service live)
4. Add US3 (event ingestion) → test data pipeline → deploy/show (analytics working)
5. Phase 6 polish → performance validation → release

---

## Notes

- [P] tasks = different files, no dependencies
- [US1/2/3] label = which user story
- Tests MUST be written and FAIL before implementing (Constitution III TDD)
- Spec explicitly includes FR-018 (80%+ unit), FR-019 (100% contract), FR-020 (integration)
- SC-007/008/009 validate test requirements
- Each user story independently testable via its "Independent Test" criteria
