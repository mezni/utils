# Tasks: Data Layer

**Input**: Design documents from `/specs/002-data-layer/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Workspace**: `source/services/` — Rust workspace root
- **Library**: `source/services/libs/borne-data/` — shared data layer crate
- **Tests**: `source/services/libs/borne-data/tests/` — integration tests

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and Rust workspace structure

- [ ] T001 Create Rust workspace at `source/services/Cargo.workspace.toml` with members `libs/borne-data`
- [ ] T002 [P] Initialize `borne-data` crate at `source/services/libs/borne-data/Cargo.toml` with `sqlx`, `tokio`, `serde`, `chrono`, `testcontainers` dependencies
- [ ] T003 [P] Create library module structure at `source/services/libs/borne-data/src/` with `lib.rs`, `pool.rs`, `error.rs`, `models/mod.rs`, `queries/mod.rs`, `migration/mod.rs`
- [ ] T004 [P] Configure Rust toolchain at `source/services/rust-toolchain.toml` (edition 2021, version 1.80+)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

- [ ] T005 Implement `DataLayerError` enum in `source/services/libs/borne-data/src/error.rs` with variants: Connection, Query, NotFound, Migration, PoolExhausted, plus `std::error::Error` and `Display` trait implementations
- [ ] T006 [P] Implement `DbConfig` struct and `create_pool`/`create_pool_with_config` functions in `source/services/libs/borne-data/src/pool.rs` with environment variable reading (FR-009)
- [ ] T007 [P] Implement connection retry with exponential backoff (3 retries: 1s, 2s, 4s) in `source/services/libs/borne-data/src/pool.rs` (FR-011)
- [ ] T008 [P] Implement logging configuration in `source/services/libs/borne-data/src/lib.rs` using `tracing` crate (or equivalent) for connection events and query timing (FR-010)

**Checkpoint**: Foundation ready — connection management, error types, and config work. US1 and US2 can begin independently.

---

## Phase 3: User Story 1 — Spatial Queries via Shared Library (Priority: P1) 🎯 MVP

**Goal**: Developer can call `find_nearby`, `find_by_id`, and `list_all` station queries that return typed Rust structs.

**Independent Test**: Run integration test that connects to platform_db, calls `find_nearby(36.8065, 10.1815, 5000)`, and receives stations ordered by distance — all without an HTTP server.

### Implementation for User Story 1

- [ ] T009 [P] [US1] Create `Partner` model struct with `PartnerType` enum in `source/services/libs/borne-data/src/models/partner.rs` (FR-003)
- [ ] T010 [P] [US1] Create `Station` model struct in `source/services/libs/borne-data/src/models/station.rs` (FR-003)
- [ ] T011 [P] [US1] Create `Charger` model struct in `source/services/libs/borne-data/src/models/charger.rs` (FR-003)
- [ ] T012 [US1] Implement `stations::list_all` query function in `source/services/libs/borne-data/src/queries/stations.rs` to fetch all stations
- [ ] T013 [US1] Implement `stations::find_nearby` with PostGIS `ST_DWithin` in `source/services/libs/borne-data/src/queries/stations.rs` — accepts lat, lng, radius_m, returns stations ordered by distance (FR-004)
- [ ] T014 [US1] Implement `stations::find_by_id` in `source/services/libs/borne-data/src/queries/stations.rs` — JOIN across station, charger, partner tables, returns `StationDetail` (FR-005)
- [ ] T015 [US1] Create `StationDetail` struct (station + chargers + partner) in `source/services/libs/borne-data/src/queries/stations.rs`
- [ ] T016 [US1] Wire up re-exports in `source/services/libs/borne-data/src/lib.rs` so all public types are accessible from `borne_data::*`

**Checkpoint**: US1 fully functional — spatial queries work against platform_db.

---

## Phase 4: User Story 2 — Database Migrations (Priority: P1)

**Goal**: Developer can apply pending SQL migrations to platform_db via the shared library.

**Independent Test**: Run migration against fresh platform_db. Verify tracking table records applied migrations. Re-run and confirm no re-execution.

### Implementation for User Story 2

- [ ] T017 [P] [US2] Create initial migration file at `source/services/libs/borne-data/migrations/20260610000001_initial.sql` with a no-op (confirms the migration system works)
- [ ] T018 [US2] Implement migration runner in `source/services/libs/borne-data/src/migration/runner.rs` using SQLx `migrate!` macro — applies pending migrations and logs results (FR-006)
- [ ] T019 [US2] Wire up migration runner in `source/services/libs/borne-data/src/lib.rs` as `borne_data::run_migrations(pool)`

**Checkpoint**: US2 fully functional — migrations apply idempotently.

---

## Phase 5: User Story 3 — Integration Test Suite (Priority: P2)

**Goal**: Developer can verify data layer correctness via a single `cargo test` command against a containerized PostGIS instance.

**Independent Test**: Run `cargo test -p borne-data` — test suite spins up PostGIS container, applies migrations, executes all queries, tears down container.

### Implementation for User Story 3

- [ ] T020 [P] [US3] Create test helper module at `source/services/libs/borne-data/tests/common/mod.rs` — starts `postgis/postgis:16-3.4` via testcontainers, creates connection pool, provides setup/teardown
- [ ] T021 [US3] Write `test_stations_list_all` in `source/services/libs/borne-data/tests/queries_test.rs` — calls `list_all` and asserts seed data count matches (10 stations expected)
- [ ] T022 [US3] Write `test_find_nearby_returns_results` in `source/services/libs/borne-data/tests/queries_test.rs` — calls `find_nearby` at Tunis center with 50km radius, asserts results are ordered by distance
- [ ] T023 [US3] Write `test_find_nearby_empty_radius` in `source/services/libs/borne-data/tests/queries_test.rs` — calls `find_nearby` with 1m radius, asserts empty result set
- [ ] T024 [US3] Write `test_find_by_id_with_chargers` in `source/services/libs/borne-data/tests/queries_test.rs` — calls `find_by_id` on an existing station, asserts chargers and partner are returned
- [ ] T025 [US3] Write `test_find_by_id_not_found` in `source/services/libs/borne-data/tests/queries_test.rs` — calls `find_by_id` on a non-existent ID, asserts `DataLayerError::NotFound`
- [ ] T026 [US3] Write `test_migration_applies_fresh` in `source/services/libs/borne-data/tests/migration_test.rs` — runs migrations on fresh DB, asserts tracking table created with one record
- [ ] T027 [US3] Write `test_migration_idempotent` in `source/services/libs/borne-data/tests/migration_test.rs` — runs migrations twice, asserts no re-execution
- [ ] T028 [US3] Write `test_connection_failure_returns_error` in `source/services/libs/borne-data/tests/queries_test.rs` — creates pool with bad credentials, asserts `DataLayerError::Connection`
- [ ] T029 [US3] Write `test_pool_exhaustion` in `source/services/libs/borne-data/tests/queries_test.rs` — acquires max connections, asserts next acquire returns `PoolExhausted` (or blocks gracefully)

**Checkpoint**: All queries and migrations verified against real PostGIS. Test suite runs single-command.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, linting, and validation

- [ ] T030 [P] Add `cargo clippy` and `cargo fmt` linting configuration to workspace
- [ ] T031 Add Rust documentation comments to all public functions and types in `source/services/libs/borne-data/src/`
- [ ] T032 Create `source/services/libs/borne-data/README.md` with crate documentation and usage examples
- [ ] T033 Update `AGENTS.md` to reference current plan at `specs/002-data-layer/plan.md`
- [ ] T034 Run quickstart validation — clean test run from repo root
- [ ] T035 [P] Add benchmark test asserting `find_nearby` completes under 200ms and full integration suite under 60s in `source/services/libs/borne-data/tests/queries_test.rs` (SC-002, SC-003)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Foundational — no dependency on US2 or US3
- **US2 (Phase 4)**: Depends on Foundational — no dependency on US1 or US3
- **US3 (Phase 5)**: Depends on US1 (queries) and US2 (migrations) — integration tests exercise both
- **Polish (Phase 6)**: Depends on all user stories

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — No dependencies on other stories
- **US2 (P1)**: Can start after Foundational — No dependencies on other stories (parallel with US1)
- **US3 (P2)**: Depends on US1 + US2 complete — integration tests exercise queries and migrations

### Within Each User Story

- Models before queries (US1)
- Migration file before runner (US2)
- Test helpers before test functions (US3)

### Parallel Opportunities

- T002, T003, T004 (Setup) can run in parallel
- T006, T007, T008 (Foundational) can run in parallel
- T009, T010, T011 (models) can run in parallel
- T020 (test helpers) can run in parallel with US1/US2 implementation
- US1 and US2 can run in parallel (different files, same dependency on Foundational)

---

## Parallel Example: User Story 1

```bash
# Launch all models together:
Task: "Create Partner model in source/services/libs/borne-data/src/models/partner.rs"
Task: "Create Station model in source/services/libs/borne-data/src/models/station.rs"
Task: "Create Charger model in source/services/libs/borne-data/src/models/charger.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 + 2)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (spatial queries)
4. Complete Phase 4: User Story 2 (migrations)
5. **STOP and VALIDATE**: Run integration tests against containerized PostGIS
6. Proceed to Phase 5 (US3 — full test suite)

### Incremental Delivery

1. Setup + Foundational → library skeleton ready
2. Add US1 → spatial queries work against platform_db
3. Add US2 → migrations run idempotently
4. Add US3 → full test coverage on real PostGIS
5. Each increment adds value without breaking previous work

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- US1 and US2 are both P1 and can be implemented in parallel
- US3 tests must run against a containerized PostGIS (testcontainers), not the shared dev platform_db
- All connection config comes from environment variables — no hardcoded credentials
- Commit after each task or logical group
