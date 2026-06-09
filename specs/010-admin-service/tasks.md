# Tasks: Admin Service

**Input**: Design documents from `specs/010-admin-service/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api.md

**Tests**: Not requested in spec — no test tasks included.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

All paths are relative to workspace root `source/`. Admin service lives at `source/services/admin-service/`.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Add admin-service to workspace and initialize crate

- [X] T001 Add `"apps/admin-service"` to workspace members in `source/Cargo.toml`
- [X] T002 [P] Create `source/apps/admin-service/Cargo.toml` with actix-web 4, sqlx 0.8, serde, thiserror, ev-core, ev-db, log, env_logger, chrono dependencies

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T003 Create `source/apps/admin-service/src/config.rs` with Config struct reading DATABASE_URL, HOST (default `0.0.0.0`), PORT (default `8081`), RUST_LOG (default `info`), plus `bind_address()` method and `x_partner_id()` helper extracting X-Partner-Id header value
- [X] T004 Create `source/apps/admin-service/src/error.rs` with AppError enum (NotFound, ValidationError, BadRequest, Conflict, InternalError, DbError) implementing `ResponseError` returning JSON `{"error": {"code": "...", "message": "..."}}`
- [X] T005 Create `source/apps/admin-service/src/models/mod.rs` with all request/response structs: CreatePartnerRequest, UpdatePartnerRequest, PartnerResponse, CreateStationRequest, UpdateStationRequest, StationResponse, CreateChargerRequest, UpdateChargerRequest, ChargerResponse, CreateAvailabilityRequest, AvailabilityResponse, HealthResponse, PaginationParams
- [X] T006 Create `source/apps/admin-service/src/db/mod.rs` re-exporting partners, stations, chargers, availability modules
- [X] T007 Create `source/apps/admin-service/src/routes/mod.rs` with `configure()` function registering all route handlers, plus `GET /api/health` handler
- [X] T008 Create `source/apps/admin-service/src/main.rs` with AppState (PgPool), `#[actix_web::main]` entrypoint loading config, initializing pool, binding HttpServer on `config.bind_address()`, calling `routes::configure`

**Checkpoint**: Foundation ready — `cargo build --package admin-service` compiles. Server starts and `/api/health` returns 200.

---

## Phase 3: User Story 1 — Manage Partners (Priority: P1) 🎯 MVP

**Goal**: Full CRUD for partner entities (create, read, update, soft-delete) with flag management

**Independent Test**: Create partner via `POST /api/partners` → verify 201 with all fields → update flags via `PUT /api/partners/{id}` → verify changes → soft-delete via `DELETE /api/partners/{id}` → verify `is_active=false` via `GET /api/partners/{id}` → list via `GET /api/partners` and confirm pagination

### Implementation

- [X] T009 [US1] Create `source/apps/admin-service/src/db/partners.rs` with CRUD queries: `create_partner()` (INSERT with ID generated via ev_core), `get_partner()` (SELECT by id), `list_partners()` (SELECT with pagination), `update_partner()` (partial UPDATE with COALESCE, RETURNING *), `delete_partner()` (soft-delete: UPDATE is_active=false)
- [X] T010 [US1] Create `source/apps/admin-service/src/routes/partners.rs` with handlers: `POST /api/partners` (create, 201), `GET /api/partners` (list, paginated, 200), `GET /api/partners/{id}` (get by id, 200/404), `PUT /api/partners/{id}` (partial update, 200/400/404), `DELETE /api/partners/{id}` (soft-delete, 200/404). Extract X-Partner-Id for audit fields.
- [X] T011 [US1] Wire partner routes into `source/apps/admin-service/src/routes/mod.rs`

**Checkpoint**: Partner CRUD fully functional. Can create, read, update flags, and soft-delete partners.

---

## Phase 4: User Story 2 — Manage Stations (Priority: P1)

**Goal**: Full CRUD for station entities with spatial coordinate validation

**Independent Test**: Create partner → create station for that partner via `POST /api/stations` → verify 201 → update address/name via `PUT /api/stations/{id}` → verify changes → delete via `DELETE /api/stations/{id}` → verify 404 on subsequent GET → list via `GET /api/stations?partner_id=X` and verify scoping

### Implementation

- [X] T012 [US2] Create `source/apps/admin-service/src/db/stations.rs` with CRUD queries: `create_station()` (INSERT with partner FK check), `get_station()` (SELECT by id), `list_stations()` (SELECT with optional partner_id filter, paginated), `update_station()` (partial UPDATE, RETURNING *), `delete_station()` (hard DELETE, CASCADE)
- [X] T013 [US2] Create `source/apps/admin-service/src/routes/stations.rs` with handlers: `POST /api/stations` (create, 201), `GET /api/stations` (list, optional partner_id filter, paginated, 200), `GET /api/stations/{id}` (get by id, 200/404), `PUT /api/stations/{id}` (partial update, 200/400/404), `DELETE /api/stations/{id}` (hard delete, 200/404). Validate lat (-90..90) and lng (-180..180) ranges.
- [X] T014 [US2] Wire station routes into `source/apps/admin-service/src/routes/mod.rs`

**Checkpoint**: Station CRUD fully functional. Partners can manage their stations.

---

## Phase 5: User Story 3 — Manage Chargers (Priority: P1)

**Goal**: Full CRUD for charger entities with connector type validation

**Independent Test**: Create partner → create station → create charger at that station via `POST /api/chargers` → verify 201 with status=offline → update status via `PUT /api/chargers/{id}` → verify → delete → verify 404 → list via `GET /api/chargers?station_id=X`

### Implementation

- [X] T015 [US3] Create `source/apps/admin-service/src/db/chargers.rs` with CRUD queries: `create_charger()` (INSERT with station FK check, default status=offline), `get_charger()` (SELECT by id), `list_chargers()` (SELECT with optional station_id filter, paginated), `update_charger()` (partial UPDATE, RETURNING *), `delete_charger()` (hard DELETE)
- [X] T016 [US3] Create `source/apps/admin-service/src/routes/chargers.rs` with handlers: `POST /api/chargers` (create, 201), `GET /api/chargers` (list, optional station_id filter, paginated, 200), `GET /api/chargers/{id}` (get by id, 200/404), `PUT /api/chargers/{id}` (partial update, 200/400/404), `DELETE /api/chargers/{id}` (hard delete, 200/404). Validate connector_type enum and power_kw > 0.
- [X] T017 [US3] Wire charger routes into `source/apps/admin-service/src/routes/mod.rs`

**Checkpoint**: Charger CRUD fully functional. Stations have chargers with connector types and status.

---

## Phase 6: User Story 4 — Update Station Availability (Priority: P2)

**Goal**: Append-only availability updates for stations

**Independent Test**: Create partner → create station → POST availability with status `available` → verify 201 → POST again with `unavailable` → verify new record created → POST a third time with `partial` → verify latest status is `partial`

### Implementation

- [X] T018 [US4] Create `source/apps/admin-service/src/db/availability.rs` with `create_availability()` (INSERT into station_availability, RETURNING *)
- [X] T019 [US4] Create `source/apps/admin-service/src/routes/availability.rs` with handler: `POST /api/stations/{id}/availability` (create availability record, 201/400/404). Validate status enum.
- [X] T020 [US4] Wire availability routes into `source/apps/admin-service/src/routes/mod.rs`

**Checkpoint**: Station availability updates work and are append-only. Each POST creates a new record.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Docker, build verification, documentation alignment

- [X] T021 [P] Create `source/apps/admin-service/Dockerfile` matching Driver Service multi-stage pattern (rust:1.85-slim-bookworm → debian:bookworm-slim), expose port 8081
- [X] T022 Run `cargo build --all` and fix any warnings or errors
- [X] T023 [P] Verify against `specs/010-admin-service/quickstart.md` — confirm env vars, endpoints, and Docker instructions work

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **US1 Partners (Phase 3)**: Depends on Foundational — No dependencies on other stories
- **US2 Stations (Phase 4)**: Depends on Foundational + US1 (needs partner FK to exist)
- **US3 Chargers (Phase 5)**: Depends on Foundational + US1 + US2 (needs station FK via partner)
- **US4 Availability (Phase 6)**: Depends on Foundational + US1 + US2 (needs station FK)
- **Polish (Phase 7)**: Depends on all phases

### User Story Dependencies

| Story | Depends On | Blocks |
|-------|-----------|--------|
| US1 — Partners | Foundational | US2, US3, US4 |
| US2 — Stations | Foundational, US1 | US3, US4 |
| US3 — Chargers | Foundational, US1, US2 | — |
| US4 — Availability | Foundational, US1, US2 | — |

### Within Each User Story

- Models (data-model) before db queries
- db queries before route handlers
- Route handlers before wiring into routes/mod.rs
- Story complete before moving to next priority

### Parallel Opportunities

- T001 and T002 can run in parallel (Setup Phase)
- T003 through T008 are sequential (Foundational — each depends on previous)
- Within US phases: db + routes within a story are sequential (routes depend on db)
- No cross-story [P] opportunities due to sequential FK dependency chain

---

## Parallel Example: Setup Phase

```bash
# Launch both setup tasks together:
Task: "T001 Add admin-service to workspace members in source/Cargo.toml"
Task: "T002 Create source/apps/admin-service/Cargo.toml with dependencies"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: US1 — Partners (CRUD working independently)
4. **STOP and VALIDATE**: `cargo build --all`, test partner CRUD via curl
5. Deploy/demo if partner management alone is sufficient

### Incremental Delivery

1. Setup + Foundational → Server starts, health check works
2. Add US1 (Partners) → Partner CRUD operational → Deploy/Demo
3. Add US2 (Stations) → Station CRUD operational → Deploy/Demo
4. Add US3 (Chargers) → Charger CRUD operational → Deploy/Demo
5. Add US4 (Availability) → Full admin functionality → Deploy/Demo

### Sequential Delivery Required

User stories MUST be delivered in order (US1 → US2 → US3 → US4) because each story builds on the FK chain (Partner → Station → Charger/Availability).

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- `cargo build --package admin-service` must succeed after each phase
