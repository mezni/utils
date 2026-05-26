---

description: "Task list for Spatial Discovery — Nearby API & SLO Validation"

---

# Tasks: Spatial Discovery — Nearby API & SLO Validation

**Input**: Design documents from `specs/003-spatial-discovery-nearby/`

**Prerequisites**: Phase 1 complete — migrations, seed data, domain modules,
auth, and CRUD endpoints implemented and wired. Infrastructure module
(`src/domain/infrastructure/`) scaffolded.

**Tests**: Not requested in specification — test tasks are excluded unless
explicitly requested.

**Organization**: Tasks are grouped by user story to enable independent
implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

## Path Conventions

- Backend: `sources/backend/src/` (Rust), `sources/backend/migrations/` (SQL)
- All paths below use the existing monorepo structure from plan.md

---

## Phase 1: Setup (Verification & Tooling)

**Purpose**: Confirm Phase 1 foundation is ready and benchmarking tool installed

- [x] T001 [P] Confirm GIST index exists on `stations.coordinates` by reviewing
  Phase 1 migration
  `sources/backend/migrations/20260526000005_create_stations.up.sql`
- [x] T002 [P] Install benchmark tool: `cargo install oha` (or use fallback
  `scripts/benchmark-nearby.sh` if oha compilation times out)

---

## Phase 2: Foundational (Blocking Prerequisites)

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

No foundational tasks — this feature builds on Phase 1's complete infrastructure
module. The `sources/backend/src/domain/infrastructure/` directory already
exists with `mod.rs` and `repository.rs`.

---

## Phase 3: User Story 1 — Driver Discovers Nearby Stations (Priority: P1) 🎯 MVP

**Goal**: High-performance nearby discovery endpoint that returns stations
ordered by distance within a radius, capped at 50, with available charger
counts and `is_test` isolation.

**Independent Test**: Issue `GET /api/v1/stations/nearby?longitude=10.1815&latitude=36.8065`
and verify stations are ordered by distance, capped at 50, no test stations by
default. Issue with `include_test=true` and verify test stations appear.

### Implementation for User Story 1

- [x] T003 [P] [US1] Add `NearbyStationResult` struct with Serialize,
  Deserialize, and sqlx::FromRow derives in
  `sources/backend/src/domain/infrastructure/mod.rs`
- [x] T004 [US1] Implement `find_nearby_stations_bounded` repository function
  using `ST_DWithin` + `ST_Distance` + `COUNT(*) FILTER (WHERE status = 'available')`
  + `LIMIT 50` with `is_test` filtering (`AND ($4 = TRUE OR s.is_test = FALSE)`)
  and soft-delete exclusion (`WHERE s.deleted_at IS NULL`) in
  `sources/backend/src/domain/infrastructure/repository.rs`
- [x] T005 [US1] Implement `nearby_stations` Actix-web handler with query param
  extraction (longitude, latitude, optional radius_meters default 20000.0,
  optional include_test default false), coordinate validation (lng: -180 to 180,
  lat: -90 to 90, radius > 0), and RFC 7807 error responses in
  `sources/backend/src/domain/infrastructure/mod.rs`
- [x] T006 [US1] Wire nearby route into the Actix-web App at `/api/v1/stations/nearby`
  in `sources/backend/src/main.rs`

**Checkpoint**: Nearby discovery endpoint returns correct results — ordered by
distance, capped at 50, no test stations by default, available charger counts
present.

---

## Phase 4: User Story 2 — SLO Benchmark (Priority: P2)

**Goal**: Verify the nearby endpoint meets the ≤200ms p95 SLO under concurrent load.

**Independent Test**: Run `oha -n 1000 -c 10 "http://localhost:8080/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065&include_test=true"`
and verify p95 ≤ 200ms.

### Implementation for User Story 2

- [ ] T007 [US2] Run SLO benchmark: `oha -n 1000 -c 10 "http://localhost:8080/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065&include_test=true"`
  and record p50, p95, p99 latencies (or use `scripts/benchmark-nearby.sh`
  fallback if oha not available)
- [ ] T008 [US2] [CONTINGENCY] If SLO fails (p95 > 200ms): run `EXPLAIN ANALYZE`
  on the nearby query via psql, verify GIST index is being used, and add
  missing indexes or optimize query. Re-run benchmark and record results.
- [ ] T009 [US2] Document SLO benchmark results in `docs/performance-baseline.md`,
  including results from two independent runs to confirm reproducibility

**Checkpoint**: SLO benchmark confirms p95 ≤ 200ms. Results documented.

---

## Phase 5: User Story 3 — Mobile App Shows Station Detail (Priority: P3)

**Goal**: Verify existing station detail and charger list endpoints meet mobile
app requirements. These endpoints are already implemented in Phase 1 — this
phase only tests and confirms.

**Independent Test**: Fetch a known seed station's detail and its chargers,
verify all required fields are present and correctly shaped.

### Implementation for User Story 3

- [ ] T010 [US3] Verify station detail endpoint `GET /api/v1/stations/{id}`
  returns all required fields for mobile consumption: id, owner_id, name,
  address, city, longitude, latitude, is_operational, is_test, created_at,
  updated_at — spot-check across 5 seed stations via curl
- [ ] T011 [US3] Verify charger list endpoint
  `GET /api/v1/stations/{station_id}/chargers` returns paginated chargers with
  id, station_id, connector_type_id, power_kw, current_type, status — test on
  at least one known station with chargers
- [ ] T012 [US3] Verify edge cases: soft-deleted station returns 404,
  non-existent station ID returns 404, station with zero chargers returns empty
  `data` array

**Checkpoint**: Station detail and charger list endpoints confirmed working for
mobile app consumption.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Code quality, build verification, and quickstart validation

- [x] T013 [P] Run `cargo clippy --all-targets -- -D warnings` and fix any new
  warnings in `sources/backend/`
- [x] T014 [P] Run `cargo test` and verify all tests pass in `sources/backend/`
- [ ] T015 Run quickstart.md validation steps: nearby endpoint returns correct
  shape, results are ordered by ascending distance_meters, benchmark runs, edge
  cases return expected errors, and zero test stations appear in default mode
  (requires database with seed data to be running)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — verification tasks only
- **Foundational (Phase 2)**: No blocking prerequisites — Phase 1 already complete
- **User Stories (Phase 3+)**: All depend on Phase 1 backend being running
  - US1 and US3 can run in parallel (different code areas)
  - US2 (benchmark) depends on US1 (the endpoint must exist)
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: No dependencies on other stories — can start immediately
- **User Story 2 (P2)**: Depends on US1 (needs the nearby endpoint to benchmark)
- **User Story 3 (P3)**: No dependencies on other stories — verifies existing Phase 1 endpoints

### Within Each User Story

- Models before services
- Services before endpoints
- Core implementation before verification

### Parallel Opportunities

- T001 and T002 (Setup) can run in parallel
- T003 (struct model) is parallelizable within US1
- US1 and US3 can be implemented in parallel by different developers
- T013 and T014 (Polish) can run in parallel

---

## Parallel Example: User Story 1

```bash
# Launch struct model creation:
Task: "Add NearbyStationResult struct in infrastructure/mod.rs"

# Then sequentially:
# Repository function in infrastructure/repository.rs
# Handler in infrastructure/mod.rs
# Route wiring in main.rs
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (verify GIST index, install oha)
2. Complete Phase 3: User Story 1 (nearby endpoint)
3. **STOP and VALIDATE**: `curl` the nearby endpoint, verify shape
4. Deploy/demo if ready — US2 and US3 are validation phases

### Incremental Delivery

1. Complete Setup → Foundation confirmed
2. Add User Story 1 (nearby endpoint) → Test with curl → **MVP!**
3. Add User Story 2 (SLO benchmark) → Record results → Document
4. Add User Story 3 (station detail verification) → Confirm mobile-ready
5. Polish → Verify build quality

### Parallel Team Strategy

With multiple developers:

1. Developer A: User Story 1 (nearby endpoint — the main deliverable)
2. Developer B: User Story 3 (station detail verification — independent, uses existing endpoints)
3. Both complete → Developer A runs User Story 2 (benchmark — needs US1 endpoint)
4. Either: Polish tasks

---

## Notes

- No tests requested in spec — all tasks are implementation or verification
- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Stop at checkpoint after US1 to validate independently
- The benchmark uses `include_test=true` because all seed stations are test records
- GPS coordinates are NOT logged in application logs (per spec clarification)
- Rate limiting is deferred to post-MVP0 (per spec clarification)
- No authentication or partner scoping on nearby endpoint (per spec clarifications)
