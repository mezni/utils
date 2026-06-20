---

description: "Task list for Sprint 1.1 — Admin Service Setup"

---

# Tasks: Admin Service Setup (Sprint 1.1)

**Input**: Design documents from `/specs/001-admin-service-setup/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are OPTIONAL - only include them if explicitly requested in the feature specification.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Backend**: `services/admin-service/src/`, `services/admin-service/migrations/`
- **Frontend**: `apps/dashboard/src/`
- **CI**: `speckit/speckit-lint/src/`
- **API**: `api/openapi/`
- **Infrastructure**: `infrastructure/docker/`, `infrastructure/postgres/init/`, `infrastructure/traefik/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create Rust workspace root `Cargo.toml` at repo root with admin-service member
- [ ] T002 [P] Create `services/admin-service/Cargo.toml` with actix-web, sqlx, tokio, serde, nanoid, chrono dependencies
- [ ] T003 [P] Create `apps/dashboard/package.json` with React 18, shadcn/ui, Tailwind CSS, react-router dependencies
- [ ] T004 [P] Create `speckit/speckit-lint/Cargo.toml` with clap, regex dependencies
- [ ] T005 [P] Create `infrastructure/docker/docker-compose.dev.yml` with postgres:16-alpine + postgis, admin-service build
- [ ] T006 [P] Create `infrastructure/docker/env/.env.dev` with DATABASE_URL, port configs
- [ ] T007 [P] Create `infrastructure/traefik/traefik.yml` with entrypoints, providers
- [ ] T008 [P] Create `infrastructure/traefik/dynamic/routers.yml` with admin-service route rules
- [ ] T009 [P] Create `infrastructure/traefik/dynamic/middlewares.yml` with CORS and rate-limiting stubs (auth middleware deferred to Auth Service sprint)
- [ ] T010 [P] Create `infrastructure/postgres/init/01-platform.sql` with CREATE EXTENSION postgis, CREATE SCHEMA inventory

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T011 Create `api/openapi/admin.yaml` with OpenAPI 3.0 spec: /health, /partners, /stations, /chargers endpoints, DTOs, error schemas
- [ ] T012 [P] Create `services/admin-service/migrations/001_inventory_schema.sql` with inventory schema creation
- [ ] T013 [P] Create `services/admin-service/migrations/002_lookup_tables.sql` with access_types, data_sources, connector_types, current_types, connector_statuses
- [ ] T014 [P] Create `services/admin-service/migrations/006_seed_data.sql` with lookup table seed values
- [ ] T015 [P] Create `services/admin-service/migrations/003_partners.sql` with partners table (OPR-* nanoid, CHECK constraint, deleted_at, timestamps)
- [ ] T016 [P] Create `services/admin-service/migrations/004_stations.sql` with stations table (STA-* nanoid, GEOGRAPHY Point 4326, GIST index, FK to partners, deleted_at)
- [ ] T017 [P] Create `services/admin-service/migrations/005_chargers.sql` with chargers table (CHG-* nanoid, FK to stations ON DELETE CASCADE, unique constraint, deleted_at)
- [ ] T018 Create `services/admin-service/src/main.rs` with Actix-web server setup, config loading, route mounting
- [ ] T019 [P] Create `services/admin-service/src/config.rs` with environment configuration struct
- [ ] T020 [P] Create `services/admin-service/src/error.rs` with AppError enum, HTTP response mapping
- [ ] T020b [P] Extend `services/admin-service/src/error.rs` with database-connection error → HTTP 503 mapping, middleware for graceful pool exhaustion
- [ ] T021 Create `services/admin-service/src/models/mod.rs` with model module re-exports
- [ ] T022 Create `services/admin-service/src/db/mod.rs` with database pool initialization
- [ ] T023 [P] Create `speckit/speckit-lint/src/main.rs` with CLI scaffolding, rule registry, file walking
- [ ] T024 [P] Create `speckit/speckit-lint/src/rules/mod.rs` with rule trait and module registry
- [ ] T025 [P] Create `speckit/speckit-lint/src/rules/service_topology.rs` with service topology validation
- [ ] T026 [P] Create `speckit/speckit-lint/src/rules/schema_isolation.rs` with schema isolation validation
- [ ] T027 [P] Create `speckit/speckit-lint/src/rules/naming.rs` with nanoid format validation
- [ ] T028 [P] Create `speckit/speckit-lint/src/rules/openapi_first.rs` with OpenAPI-first enforcement
- [ ] T029 [P] Create `speckit/speckit-lint/src/rules/sqlx_safety.rs` with raw SQL detection
- [ ] T030 [P] Create `speckit/speckit-lint/src/rules/frontend_boundary.rs` with frontend API client enforcement
- [ ] T031 [P] Create `speckit/speckit-lint/src/rules/migration_validation.rs` with migration integrity checks

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 4 - System Health Check (Priority: P4) 🎯 MVP

**Goal**: Operators can verify admin-service responsiveness via health check endpoint

**Independent Test**: Send GET /health and receive `{"status":"healthy","service":"admin-service","version":"1.0.0"}` with HTTP 200

### Implementation for User Story 4

- [ ] T032 [P] [US4] Create `services/admin-service/src/routes/mod.rs` with route module re-exports
- [ ] T033 [P] [US4] Create `services/admin-service/src/routes/health.rs` with GET /health handler returning service status JSON
- [ ] T034 [US4] Mount health route in `services/admin-service/src/main.rs` with Actix-web app configuration

**Checkpoint**: Health endpoint responds with correct JSON — administrative monitoring works

---

## Phase 4: User Story 1 - Admin Creates a Partner Account (Priority: P1)

**Goal**: Admin can create, view, update, and soft-delete partner/operator records

**Independent Test**: POST /partners with valid data returns OPR-* ID; GET /partners lists all partners; PATCH /partners/{id} updates fields; DELETE /partners/{id} soft-deletes

### Implementation for User Story 1

- [ ] T035 [P] [US1] Create `services/admin-service/src/models/partner.rs` with Partner struct, CreatePartnerRequest, UpdatePartnerRequest, PartnerResponse DTOs
- [ ] T036 [P] [US1] Create `services/admin-service/src/db/partners.rs` with SQLx compile-time queries: insert, select_all, select_by_id, update, soft_delete
- [ ] T037 [P] [US1] Create `services/admin-service/src/routes/partners.rs` with POST /partners, GET /partners, GET /partners/{id}, PATCH /partners/{id}, DELETE /partners/{id} handlers
- [ ] T038 [US1] Mount partner routes in `services/admin-service/src/main.rs`
- [ ] T039 [US1] Add partner request validation in `services/admin-service/src/models/partner.rs` (name required, network_type enum, OPR-* ID format)
- [ ] T040 [P] [US1] Create `apps/dashboard/src/lib/api-client.ts` with generated OpenAPI client for partner endpoints
- [ ] T041 [P] [US1] Create `apps/dashboard/src/pages/Partners.tsx` with partner list view, create/edit form, delete confirmation
- [ ] T042 [P] [US1] Create `apps/dashboard/src/components/partners/PartnerTable.tsx` with shadcn/ui table component
- [ ] T043 [P] [US1] Create `apps/dashboard/src/components/partners/PartnerForm.tsx` with shadcn/ui form for create/edit

**Checkpoint**: Partner CRUD works end-to-end — backend API + dashboard UI functional

---

## Phase 5: User Story 2 - Admin Creates a Station (Priority: P2)

**Goal**: Admin can create, view, update, and soft-delete charging stations with spatial data linked to partners

**Independent Test**: POST /stations with valid data and partner ID returns STA-* ID; GET /stations lists stations; station soft-delete propagates deleted_at to chargers

### Implementation for User Story 2

- [ ] T044 [P] [US2] Create `services/admin-service/src/models/station.rs` with Station struct, CreateStationRequest, UpdateStationRequest, StationResponse DTOs (location as lat/lon pair)
- [ ] T045 [P] [US2] Create `services/admin-service/src/db/stations.rs` with SQLx compile-time queries: insert with ST_GeogFromText, select_all, select_by_id, update, soft_delete
- [ ] T046 [P] [US2] Create `services/admin-service/src/routes/stations.rs` with POST /stations, GET /stations, GET /stations/{id}, PATCH /stations/{id}, DELETE /stations/{id} handlers
- [ ] T047 [US2] Mount station routes in `services/admin-service/src/main.rs`
- [ ] T048 [US2] Add station spatial validation in `services/admin-service/src/models/station.rs` (lat -90 to 90, lon -180 to 180, partner_id OPR-* format check)
- [ ] T049 [P] [US2] Create `apps/dashboard/src/pages/Stations.tsx` with station list view, create/edit form
- [ ] T050 [P] [US2] Create `apps/dashboard/src/components/stations/StationTable.tsx` with shadcn/ui table
- [ ] T051 [P] [US2] Create `apps/dashboard/src/components/stations/StationForm.tsx` with lat/lon coordinate inputs

**Checkpoint**: Station CRUD works end-to-end — spatial data validated, partner FK enforced

---

## Phase 6: User Story 3 - Admin Manages Chargers at a Station (Priority: P3)

**Goal**: Admin can create, view, update, and soft-delete chargers linked to stations

**Independent Test**: POST /chargers with valid data and station ID returns CHG-* ID; deleting a station cascades soft-delete to all its chargers; duplicate connector_type at same station is rejected

### Implementation for User Story 3

- [ ] T052 [P] [US3] Create `services/admin-service/src/models/charger.rs` with Charger struct, CreateChargerRequest, UpdateChargerRequest, ChargerResponse DTOs
- [ ] T053 [P] [US3] Create `services/admin-service/src/db/chargers.rs` with SQLx compile-time queries: insert, select_all (with station_id filter), select_by_id, update, soft_delete
- [ ] T054 [P] [US3] Create `services/admin-service/src/routes/chargers.rs` with POST /chargers, GET /chargers, GET /chargers/{id}, PATCH /chargers/{id}, DELETE /chargers/{id} handlers
- [ ] T055 [US3] Mount charger routes in `services/admin-service/src/main.rs`
- [ ] T056 [US3] Add charger validation: station_id STA-* format check, connector_type/current_type FK validation, unique constraint enforcement
- [ ] T057 [P] [US3] Create `apps/dashboard/src/pages/Chargers.tsx` with charger list view, create/edit form
- [ ] T058 [P] [US3] Create `apps/dashboard/src/components/chargers/ChargerTable.tsx` with shadcn/ui table
- [ ] T059 [P] [US3] Create `apps/dashboard/src/components/chargers/ChargerForm.tsx` with connector type/power inputs

**Checkpoint**: Charger CRUD works end-to-end — cascade soft-delete from station confirmed

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories, testing, and sprint documentation

- [ ] T060 Implement unit tests for partner model validation in `services/admin-service/src/models/partner.rs`
- [ ] T061 [P] Implement unit tests for station spatial validation in `services/admin-service/src/models/station.rs`
- [ ] T062 [P] Implement unit tests for charger uniqueness constraint in `services/admin-service/src/models/charger.rs`
- [ ] T063 [P] Implement integration tests in `services/admin-service/tests/` for partner CRUD flow
- [ ] T064 [P] Implement integration tests in `services/admin-service/tests/` for station spatial CRUD flow
- [ ] T065 [P] Implement integration tests in `services/admin-service/tests/` for charger CRUD with cascade
- [ ] T066 [P] Create `apps/dashboard/src/App.tsx` with React Router setup, page routing, layout shell
- [ ] T067 [P] Create `apps/dashboard/src/main.tsx` with React DOM entry point
- [ ] T068 Create `apps/dashboard/tailwind.config.ts` with shadcn/ui theme configuration
- [ ] T069 Run speckit-lint validation against project: `cargo run -- --path ../../ --verbose`
- [ ] T070 Generate docs/SYSTEM_STATE.md with sprint 1.1 architecture state
- [ ] T071 Generate docs/roadmap_status.md with sprint 1.1 progress
- [ ] T072 Generate docs/sprint_backlog.md with remaining items and deferred work

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-6)**: All depend on Foundational phase completion
  - User Story 4 (Health) can run independently - no dependencies on other stories
  - User Story 1 (Partners) can run independently - no dependencies on other stories
  - User Story 2 (Stations) depends on US1 partners existing (partner_id FK)
  - User Story 3 (Chargers) depends on US2 stations existing (station_id FK)
- **Polish (Phase 7)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 4 (P4)**: Can start after Foundational - No dependencies on other stories
- **User Story 1 (P1)**: Can start after Foundational - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational - Depends on US1 for partner FK
- **User Story 3 (P3)**: Can start after Foundational - Depends on US2 for station FK

### Within Each User Story

- Models before services
- Services before endpoints
- Core implementation before UI components
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel (T002-T010)
- All Foundational tasks marked [P] can run in parallel (T012-T031)
- US4 (Health) can run in parallel with US1 (Partners)
- Models within a story marked [P] can run in parallel
- UI components within a story marked [P] can run in parallel
- T060-T065 (tests) can run in parallel with T066-T068 (dashboard shell)
- US2 (Stations) must wait for US1 (Partners) — partner FK dependency
- US3 (Chargers) must wait for US2 (Stations) — station FK dependency

---

## Parallel Example: User Story 1

```bash
# Launch all models + DB queries + routes together for US1:
Task: "Create Partner model/DTOs in services/admin-service/src/models/partner.rs"
Task: "Create SQLx partner queries in services/admin-service/src/db/partners.rs"
Task: "Create partner route handlers in services/admin-service/src/routes/partners.rs"
```

---

## Implementation Strategy

### MVP First (User Story 4 + User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 4 (Health - fastest, validates service running)
4. Complete Phase 4: User Story 1 (Partners - core entity)
5. **STOP and VALIDATE**: Test Partner CRUD independently
6. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 4 (Health) → Service verified running
3. Add User Story 1 (Partners) → Core entity operational (MVP!)
4. Add User Story 2 (Stations) → Spatial entities added
5. Add User Story 3 (Chargers) → Full hierarchy complete
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Phase 1 + Phase 2 together
2. Once Foundation is done:
   - Developer A: User Story 4 + User Story 1
   - Developer B: Create dashboard shell (Phase 7 polish tasks)
   - Developer C: speckit-lint validation + testing
3. Stories complete and integrate independently
4. After US1: Developer A continues to US2 (Stations)
5. After US2: Same developer continues to US3 (Chargers)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Models before services, services before endpoints
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
- All IDs use nanoid(12) format: OPR-*/STA-*/CHG-* with DB CHECK constraint
- All deletes are soft deletes (deleted_at timestamp) — no restore endpoint
- All updates use PATCH semantics — partial field updates only
