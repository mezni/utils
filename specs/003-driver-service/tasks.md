# Tasks: Driver Service

**Input**: Design documents from `/specs/003-driver-service/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Service root**: `source/services/driver-service/`
- **Source**: `source/services/driver-service/src/`
- **Workspace**: `source/services/` (Cargo workspace root)
- **Existing lib**: `source/services/libs/borne-data/` (Sprint 1.1)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Register the driver-service as a workspace member, create crate scaffolding, add dependencies

- [ ] T001 Register `driver-service` as a workspace member in `source/services/Cargo.toml` members list
- [ ] T002 [P] Create `source/services/driver-service/Cargo.toml` with Actix-web, serde, tokio, tracing, borne-data dependencies
- [ ] T003 [P] Create module scaffolding at `source/services/driver-service/src/` with `main.rs`, `api/`, `handlers/`, `dto/`, `config/`, `errors/`, `telemetry/` directories and mod files

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented — response envelope, error types, config, validation, logging middleware

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 [P] Implement `AppError` enum in `source/services/driver-service/src/errors/app_error.rs` with NotFound, ValidationError, InternalError, ServiceUnavailable variants, each mapping to an HTTP status code and error code string
- [ ] T005 [P] Implement JSON response envelope structs (`ApiResponse<T>`, `ErrorResponse`, `ValidationErrorDetail`) in `source/services/driver-service/src/dto/error_response.rs` with `data`, `error`, `meta` fields per data-model.md
- [ ] T006 [P] Implement query parameter DTO structs with serde `Deserialize` and custom `validate()` methods for `lat`, `lng`, `radius_m` in `source/services/driver-service/src/dto/nearby_query.rs`
- [ ] T007 [P] Implement `Settings` struct in `source/services/driver-service/src/config/settings.rs` reading environment variables (DB_HOST, DB_PORT, SERVER_PORT, etc.) with defaults
- [ ] T008 [P] Implement logging middleware in `source/services/driver-service/src/telemetry/middleware.rs` using `tracing` to log method, path, status, and duration for every request (FR-011)
- [ ] T009 Implement `GET /api/v1/health` endpoint in `source/services/driver-service/src/api/v1/health.rs` and `source/services/driver-service/src/handlers/health_handler.rs` that checks DB connectivity and returns health status per contracts/rest-api.md (FR-009)
- [ ] T010 Wire up Actix-web app in `source/services/driver-service/src/main.rs` with config loading, pool creation via `borne_data::create_pool()`, route registration, and middleware stack

**Checkpoint**: Foundation ready — service boots, health endpoint responds, logging works, all DTOs and error types in place. User stories can now begin.

---

## Phase 3: User Story 1 — List All Stations (Priority: P1) 🎯 MVP

**Goal**: Mobile app can fetch a lightweight list of all stations for map markers (FR-003)

**Independent Test**: Start the service, call `GET /api/v1/stations`, verify response contains stations with id, name, address, latitude, longitude — all wrapped in consistent JSON envelope. Empty DB returns empty array.

### Implementation for User Story 1

- [ ] T011 [P] [US1] Create station response DTO in `source/services/driver-service/src/dto/station_response.rs` with id, name, address, latitude, longitude fields
- [ ] T012 [US1] Implement station list handler in `source/services/driver-service/src/handlers/station_handler.rs` calling `borne_data::list_all()` and mapping results to station response DTOs with 100-result limit
- [ ] T013 [US1] Implement API route registration for `GET /api/v1/stations` in `source/services/driver-service/src/api/v1/stations.rs` wiring to the station handler
- [ ] T014 [US1] Register stations route in `source/services/driver-service/src/api/v1/mod.rs` and integrate into main.rs app setup

**Checkpoint**: At this point, US1 should be fully functional — `GET /api/v1/stations` returns stations in consistent envelope. Testable independently of US2/US3.

---

## Phase 4: User Story 2 — Find Nearby Stations (Priority: P1)

**Goal**: App can find stations within a geographic radius, ordered by distance (FR-004, FR-005)

**Independent Test**: Start the service, call `GET /api/v1/stations/nearby?lat=36.8065&lng=10.1815&radius_m=50000`, verify stations returned ordered by distance. Call with invalid params and verify field-level validation errors.

### Implementation for User Story 2

- [ ] T015 [US2] Implement nearby handler in `source/services/driver-service/src/handlers/nearby_handler.rs` extracting and validating `NearbyQuery` params, calling `borne_data::find_nearby()`, mapping results to station response DTOs
- [ ] T016 [US2] Implement API route registration for `GET /api/v1/stations/nearby` in `source/services/driver-service/src/api/v1/nearby.rs` — registered BEFORE the parameterized `{id}` route to avoid conflicts per research.md
- [ ] T017 [US2] Register nearby route in `source/services/driver-service/src/api/v1/mod.rs`

**Checkpoint**: US2 independently testable — nearby spatial queries return ordered results, validation errors report field-level details.

---

## Phase 5: User Story 3 — View Station Details (Priority: P2)

**Goal**: App can fetch station detail with chargers and partner info (FR-006)

**Independent Test**: Start the service, call `GET /api/v1/stations/{id}` with a known station ID, verify response includes station, chargers array (with connector_type, power_kw, status), and partner. Non-existent ID returns 404.

### Implementation for User Story 3

- [ ] T018 [P] [US3] Create station detail response DTOs in `source/services/driver-service/src/dto/station_detail_response.rs` with nested ChargerResponse and PartnerResponse structs per data-model.md
- [ ] T019 [US3] Implement station detail handler in `source/services/driver-service/src/handlers/station_handler.rs` calling `borne_data::find_by_id()` and mapping to detail response DTOs, handling NotFound error
- [ ] T020 [US3] Implement API route for `GET /api/v1/stations/{id}` in `source/services/driver-service/src/api/v1/station_detail.rs`
- [ ] T021 [US3] Register station detail route in `source/services/driver-service/src/api/v1/mod.rs`

**Checkpoint**: All three discovery endpoints working independently.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Linting, documentation, and validation

- [ ] T022 [P] Add `[lints]` section to `source/services/driver-service/Cargo.toml` inheriting workspace lints and run `cargo clippy --fix`
- [ ] T023 Add Rust documentation comments to all public functions and types in `source/services/driver-service/src/`
- [ ] T024 Update `source/services/driver-service/src/main.rs` with graceful shutdown handling (SIGTERM, Ctrl+C)
- [ ] T025 Run quickstart validation: clean compile, boot service with `cargo run`, verify all 4 endpoints respond correctly
- [ ] T026 Run `cargo fmt` across `source/services/driver-service/` for consistent formatting
- [ ] T027 Add concurrent request smoke test in `source/services/driver-service/tests/integration/mod.rs` spawning 100 simultaneous `/api/v1/stations` requests and verifying zero failures (SC-005)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Foundational — no dependency on US2 or US3
- **US2 (Phase 4)**: Depends on Foundational — no dependency on US1 or US3
- **US3 (Phase 5)**: Depends on Foundational — no dependency on US1 or US2
- **Polish (Phase 6)**: Depends on all user stories

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — No dependencies on other stories
- **US2 (P1)**: Can start after Foundational — No dependencies on other stories (parallel with US1)
- **US3 (P2)**: Can start after Foundational — No dependencies on other stories (parallel with US1/US2)

### Within Each User Story

- DTOs before handlers
- Handlers before route registration
- Route registration before integration testing
- Story complete before moving to next

### Parallel Opportunities

- T002, T003 (Setup) can run in parallel
- T004-T008 (Foundational DTOs, errors, config, telemetry) can run in parallel
- US1 and US2 (handlers are different files) can run in parallel
- T011 (DTO) and T018 (Detail DTO) can run in parallel
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Launch DTO first (required by handler):
Task: "Create StationResponse DTO in source/services/driver-service/src/dto/station_response.rs"

# Then launch handler (depends on DTO + borne_data):
Task: "Implement station list handler in source/services/driver-service/src/handlers/station_handler.rs"

# Route registration is sequential:
Task: "Wire up stations route in source/services/driver-service/src/api/v1/stations.rs"
```

---

## Implementation Strategy

### MVP First (Phase 1 + 2 + US1)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (list stations)
4. **STOP and VALIDATE**: `GET /api/v1/stations` works, returns consistent JSON envelope
5. Proceed to remaining stories

### Incremental Delivery

1. Setup + Foundational → service skeleton boots, health endpoint responds
2. Add US1 → stations list works → deployable MVP
3. Add US2 (parallel with US1) → nearby search works
4. Add US3 → station details work
5. Polish → production-ready

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- US1 and US2 are both P1 and can be implemented in parallel
- US3 is P2 — implement after US1/US2 are stable
- All endpoints use `/api/v1/` prefix per project constitution
- The `borne-data` library handles all DB connection and query logic — driver-service is a thin HTTP layer
- Cross-cutting US4 (consistent JSON envelope) is satisfied by Phase 2 response envelope implementation + applies to all endpoints automatically
