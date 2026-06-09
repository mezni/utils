---
description: "Task list for Sprint 2.3 — Driver Service implementation"
---

# Tasks: Driver Service

**Input**: Design documents from `specs/009-driver-service/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/api.md

**Tests**: No separate test tasks generated. Integration tests are built into each user story phase as verification steps.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

All paths are relative to the repository root. Binary at `source/apps/driver-service/`, shared crates at `source/crates/ev-core` and `source/crates/ev-db`.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Initialize the driver-service binary crate and add it to the workspace

- [X] T001 Create `source/apps/driver-service/` directory with `src/main.rs` placeholder
- [X] T002 Create `source/apps/driver-service/Cargo.toml` with dependencies: actix-web, serde, serde_json, tokio, thiserror, ev-core, ev-db, sqlx (with postgres + runtime-tokio features), log, env_logger
- [X] T003 Add `source/apps/driver-service` to workspace members in `source/Cargo.toml`
- [X] T004 Verify `cargo build --package driver-service` compiles with zero warnings

**Checkpoint**: Crate compiles — workspace member registered

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared infrastructure all endpoints depend on — config, error types, AppState, route registration, server bootstrap

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T005 [P] Create `source/apps/driver-service/src/config.rs` — Config struct with DATABASE_URL, HOST, PORT fields parsed from env vars with defaults
- [X] T006 [P] Create `source/apps/driver-service/src/error.rs` — AppError enum (NotFound, BadRequest, InternalError, DbError) implementing actix_web::ResponseError with JSON error body matching contracts
- [X] T007 Create `source/apps/driver-service/src/main.rs` — async main, PgPool init via ev-db, AppState with pool, Actix-web HttpServer bind
- [X] T008 Create `source/apps/driver-service/src/routes/mod.rs` — configure() function registering all route modules
- [X] T009 Create `source/apps/driver-service/src/models/mod.rs` — shared response types: StationSummary, StationNearby, StationDetail, ChargerInfo, HealthResponse, ReviewsStubResponse, ApiError deriving Serialize
- [X] T010 Verify `cargo build --package driver-service` compiles with zero warnings after foundational code

**Checkpoint**: Server binary compiles and can bind to a port (verified by running and checking health endpoint stub)

---

## Phase 3: User Story 1 — Browse Nearby Stations (Priority: P1) 🎯 MVP

**Goal**: Health endpoint + nearby station discovery with partner visibility filter. This is the core driver flow — without it, the app can't show any stations.

**Independent Test**: Start the server with a real database. Hit GET /api/health → 200 with `{"status":"ok"}`. Hit GET /api/stations/nearby?lat=36.8008&lng=10.1815 → JSON array of stations within 10km, sorted by distance, all belonging to verified+live+active partners.

### Implementation for User Story 1

- [X] T011 [US1] Create `source/apps/driver-service/src/routes/health.rs` — GET /api/health handler returning HealthResponse
- [X] T012 [US1] Create `source/apps/driver-service/src/db/mod.rs` — db module declaration
- [X] T013 [US1] Create `source/apps/driver-service/src/db/nearby.rs` — nearby_stations() function: ST_DWithin query with partner JOIN, distance sort, pagination (lat, lng, radius, limit, offset params)
- [X] T014 [US1] Create `source/apps/driver-service/src/routes/nearby.rs` — GET /api/stations/nearby handler validating query params and calling db::nearby
- [X] T015 [US1] Verify endpoint against real database — nearby returns stations within radius, partner visibility enforced, empty radius returns empty list

**Checkpoint**: Health and nearby endpoints work against real DB — MVP functional

---

## Phase 4: User Story 2 — View Station Detail (Priority: P1)

**Goal**: Station detail endpoint returns station info + full charger list. Same priority as US1 because drivers need charger-level detail to decide.

**Independent Test**: Hit GET /api/stations/STN001 → JSON with station fields and chargers array. Each charger has connector_type, power_kw, status. Invalid ID returns 404.

### Implementation for User Story 2

- [X] T016 [US2] Create `source/apps/driver-service/src/db/detail.rs` — get_station() returning station + chargers in two queries, partner visibility JOIN on station query
- [X] T017 [US2] Create `source/apps/driver-service/src/routes/detail.rs` — GET /api/stations/{id} handler calling db::detail
- [X] T018 [US2] Verify endpoint — valid station returns 200 with chargers, invalid returns 404

**Checkpoint**: Station detail endpoint works — both P1 stories complete

---

## Phase 5: User Story 3 — Search Stations (Priority: P2)

**Goal**: Text search on station name/address with optional connector type filter.

**Independent Test**: Hit GET /api/stations/search?q=Tunis → matching stations. Add `&connector_type=ccs` → filtered to stations with CCS chargers. Short query returns 400.

### Implementation for User Story 3

- [X] T019 [US3] Create `source/apps/driver-service/src/db/search.rs` — search_stations() with ILIKE on name/address, optional connector_type filter via charger JOIN, partner visibility, pagination
- [X] T020 [US3] Create `source/apps/driver-service/src/routes/search.rs` — GET /api/stations/search handler with query param validation (q min 2 chars, optional connector_type)
- [X] T021 [US3] Verify endpoint — text search returns matches, connector filter works, short query rejected

**Checkpoint**: Search endpoint works — stations discoverable by name and connector type

---

## Phase 6: User Story 4 — View Map Markers (Priority: P2)

**Goal**: Bounding box query for map viewport rendering, returning station name and availability status.

**Independent Test**: Hit GET /api/stations/markers?south=36.7&west=10.0&north=36.9&east=10.3 → stations in bbox with name and availability_status. Empty bbox returns empty list.

### Implementation for User Story 4

- [X] T022 [US4] Create `source/apps/driver-service/src/db/markers.rs` — markers_in_bbox() using ST_MakeEnvelope && overlap operator, partner visibility, latest availability via LATERAL subquery
- [X] T023 [US4] Create `source/apps/driver-service/src/routes/markers.rs` — GET /api/stations/markers handler with bbox param validation
- [X] T024 [US4] Verify endpoint — bbox returns stations, empty bbox returns empty list

**Checkpoint**: Markers endpoint works — map viewport populated with station pins

---

## Phase 7: User Story 5 — View Reviews Stub (Priority: P3)

**Goal**: Placeholder endpoint returns "coming soon" message. Prevents frontend from crashing when navigating to reviews section.

**Independent Test**: Hit GET /api/stations/STN001/reviews → 200 with `{"station_id":"STN001","message":"Reviews are coming soon"}`. Invalid station returns 404.

### Implementation for User Story 5

- [X] T025 [US5] Create `source/apps/driver-service/src/routes/reviews.rs` — GET /api/stations/{id}/reviews handler returning stub JSON (validates station exists, returns placeholder if found, 404 if not)
- [X] T026 [US5] Verify endpoint — valid station returns placeholder, invalid station returns 404

**Checkpoint**: Reviews stub works — frontend can navigate to reviews section safely

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Dockerfile, full integration tests, lint verification

- [X] T027 Create `source/apps/driver-service/Dockerfile` — multi-stage build (rust:1.85-slim-bookworm builder, debian:bookworm-slim runtime), compile workspace, copy only driver-service binary
- [X] T028 Run `cargo build --all` — verify entire workspace compiles with zero warnings including the new crate
- [X] T029 Run `cargo clippy --package driver-service` — verify zero clippy warnings
- [X] T030 Run all 5 endpoint verifications end-to-end against a real database — health, nearby, detail, search, markers all return correct responses

**Checkpoint**: All 3 user stories complete (5 endpoints). Docker image builds. Lint clean.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies
- **Foundational (Phase 2)**: Depends on Setup
- **User Story 1 (Phase 3)**: Depends on Foundational — BLOCKS all other stories
- **User Story 2 (Phase 4)**: Depends on Foundational — Can run in parallel with US1 (different files)
- **User Story 3 (Phase 5)**: Depends on Foundational — Can run in parallel with US1, US2
- **User Story 4 (Phase 6)**: Depends on Foundational — Can run in parallel with US1–US3
- **User Story 5 (Phase 7)**: Depends on Foundational — Can run in parallel with US1–US4
- **Polish (Phase 8)**: Depends on all user stories complete

### User Story Dependencies

- **User Story 1 (P1)**: No dependency on other stories — first user story to implement
- **User Story 2 (P1)**: No dependency on other stories — can be implemented in parallel with US1
- **User Story 3 (P2)**: No dependency on other stories — independent endpoint
- **User Story 4 (P2)**: No dependency on other stories — independent endpoint
- **User Story 5 (P3)**: No dependency on other stories — independent endpoint

### Within Each User Story

- Create db query file → Create route handler → Register route in routes/mod.rs → Verify with curl

### Parallel Opportunities

- T005, T006 (config.rs and error.rs) — different files, no dependencies
- User Stories 1–5 can all be implemented in parallel after Foundational (different files per endpoint)
- T011, T012 (health.rs and db/mod.rs) — can run in parallel
- All db/ files are independent of each other
- All route handler files are independent of each other

---

## Parallel Example: All 5 Endpoints

```bash
# All endpoints are independent — can be developed in parallel:
Task: "User Story 1 (US1) — Nearby stations: db/nearby.rs + routes/nearby.rs"
Task: "User Story 2 (US2) — Station detail: db/detail.rs + routes/detail.rs"
Task: "User Story 3 (US3) — Search: db/search.rs + routes/search.rs"
Task: "User Story 4 (US4) — Markers: db/markers.rs + routes/markers.rs"
Task: "User Story 5 (US5) — Reviews stub: routes/reviews.rs"
```

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup → driver-service crate compiles
2. Complete Phase 2: Foundational → server boots with AppState
3. Complete Phase 3: User Story 1 → health + nearby endpoints work
4. **STOP and VALIDATE**: Test nearby with real database, confirm partner visibility
5. This is the MVP — drivers can discover nearby stations, all future stories are additive

### Incremental Delivery

1. Setup + Foundational → server binary boots
2. User Story 1 → health + nearby (MVP!)
3. User Story 2 → station detail
4. User Story 3 → search
5. User Story 4 → map markers
6. User Story 5 → reviews stub
7. Polish → Dockerfile + lint

### Parallel Team Strategy

With multiple developers:
1. Team completes Setup + Foundational together
2. Split across endpoints (all independent):
   - Developer A: US1 (nearby)
   - Developer B: US2 (detail) + US5 (reviews)
   - Developer C: US3 (search)
   - Developer D: US4 (markers)
3. Final: Polish (Dockerfile, lint, end-to-end verify)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story can be independently verified
- All endpoints are read-only — no write operations in Sprint 2.3
- PostgreSQL 17 + PostGIS 3 with Sprint 2.2 migrations must be available for verification
- Partner visibility filter is applied server-side via SQL JOIN — no auth layer needed
- Passwords/auth not in scope — MVP-3 adds Keycloak + JWT middleware
