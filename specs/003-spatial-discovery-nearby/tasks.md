---

description: "Task list for Spatial Discovery — Nearby API & SLO Validation"

---

# Tasks: Spatial Discovery — Nearby API & SLO Validation

**Input**: Design documents from `specs/003-spatial-discovery-nearby/`

**Prerequisites**: Phase 1 complete — all 5 domain entities, migrations, seed
data, auth module, and CRUD endpoints are implemented and wired.

**Tests**: Not requested in specification — test tasks are excluded unless
explicitly requested.

**Organization**: Tasks are grouped by user story to enable independent
implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

## Path Conventions

- Backend: `sources/backend/src/` (Rust)
- All paths below use the existing monorepo structure from plan.md

---

## Phase 0: Setup & Research (Shared Prerequisites)

**Purpose**: Existing infrastructure module is already scaffolded. No new
dependencies or migrations needed.

- [ ] T001 [P] [Research] Confirm GIST index exists on stations.coordinates
  by running `\d stations` in psql or reviewing Phase 1 migration
  `sources/backend/migrations/20260526000005_create_stations.up.sql`
- [ ] T002 [P] [Research] Install `oha` benchmark tool: `cargo install oha`
- [ ] T003 [P] [Research] Verify seed data is loaded and contains stations near
  Tunis (~36.8°N, 10.2°E) by querying the database

---

## Phase 1: User Story 1 — Driver Discovers Nearby Stations (Priority: P1) 🎯 MVP

**Goal**: High-performance nearby discovery endpoint that returns stations
ordered by distance within a radius, capped at 50, with available charger
counts and `is_test` isolation.

**Independent Test**: Issue `GET /api/v1/stations/nearby?longitude=10.1815&latitude=36.8065`
and verify stations are ordered by distance, capped at 50, and exclude test records.

- [ ] T004 [US1] Add `NearbyStationResult` struct with Serialize, Deserialize,
  and sqlx::FromRow derives in `sources/backend/src/domain/infrastructure/mod.rs`
- [ ] T005 [US1] Implement `find_nearby_stations_bounded` repository function
  using ST_DWithin + ST_Distance + COUNT(*) FILTER + LIMIT 50 with is_test
  filtering in `sources/backend/src/domain/infrastructure/repository.rs`
- [ ] T006 [US1] Implement `nearby_stations` Actix-web handler with query param
  extraction (longitude, latitude, optional radius_meters default 20000.0,
  optional include_test default false), validation (coordinate ranges, radius > 0),
  and RFC 7807 error responses in `sources/backend/src/domain/infrastructure/mod.rs`
- [ ] T007 [US1] Wire nearby route into the `/api/v1/stations` service scope in
  `sources/backend/src/main.rs` (route: `GET /nearby`)

**Checkpoint**: Nearby discovery endpoint returns correct results — ordered by
distance, capped at 50, no test stations by default, available charger counts
present.

---

## Phase 2: User Story 2 — SLO Benchmark (Priority: P2)

**Goal**: Verify the nearby endpoint meets the ≤200ms p95 SLO under concurrent
load.

**Independent Test**: Run `oha -n 1000 -c 10 "http://localhost:8080/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065"`
and verify p95 ≤ 200ms.

- [ ] T008 [US2] Run SLO benchmark: `oha -n 1000 -c 10 "http://localhost:8080/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065"`
  and record p50, p95, p99 latencies in task completion notes
- [ ] T009 [US2] [CONTINGENCY] If SLO fails: run `EXPLAIN ANALYZE` on the
  nearby query, verify GIST index is being used, and add missing indexes or
  optimize query as needed. Re-run benchmark.
- [ ] T010 [US2] Document SLO benchmark results in `docs/performance-baseline.md`

**Checkpoint**: SLO benchmark confirms p95 ≤ 200ms. Results documented.

---

## Phase 3: User Story 3 — Station Detail & Charger List Verification (Priority: P3)

**Goal**: Verify that existing station detail and charger list endpoints meet
mobile app requirements. These endpoints are already implemented in Phase 1 —
this phase only tests and confirms.

**Independent Test**: Fetch a known station's detail and its chargers, verify
all required fields are present and correctly shaped.

- [ ] T011 [US3] Verify station detail endpoint returns all required fields for
  mobile consumption: id, name, address, city, longitude, latitude,
  is_operational by requesting each of the 100 seed stations in a loop or
  spot-check via curl
- [ ] T012 [US3] Verify charger list endpoint returns paginated chargers with
  id, station_id, connector_type_id, power_kw, current_type, status per charger
  for at least one station with chargers
- [ ] T013 [US3] Verify edge cases: soft-deleted station returns 404,
  non-existent station returns 404, station with zero chargers returns empty
  data array

**Checkpoint**: Station detail and charger list endpoints confirmed working
for mobile app consumption.
