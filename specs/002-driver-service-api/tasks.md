# Tasks: Driver Service & Spatial API

**Input**: Design documents from `/specs/002-driver-service-api/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Service**: `source/services/driver-service/src/`
- **Infra**: `source/infra/`
- **Tests**: `source/services/driver-service/tests/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create `source/services/driver-service/` directory tree (src/, src/api/, src/db/, src/models/, tests/)
- [ ] T002 [P] Create `source/services/driver-service/Cargo.toml` with dependencies: actix-web, sqlx (postgres + runtime-tokio + tls-native-tls + macros), serde, serde_json, tracing, tracing-subscriber, config, dotenvy, chrono, uuid
- [ ] T003 [P] Create `source/services/driver-service/.env.template` with documented env vars (LISTEN_ADDR, DATABASE_URL, DB_POOL_MIN, DB_POOL_MAX, CORS_ORIGINS, RUST_LOG)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 Implement Config struct in `source/services/driver-service/src/config.rs` — load from env/config crate with sensible defaults
- [ ] T005 [P] Implement JSON logging setup in `source/services/driver-service/src/logging.rs` — tracing-subscriber JSON fmt to stdout
- [ ] T006 [P] Implement database pool module in `source/services/driver-service/src/db/pool.rs` — sqlx::PgPool builder with configurable min/max
- [ ] T007 Create NearbyStation response struct in `source/services/driver-service/src/models/station.rs` — maps to gis.get_nearby_stations return columns
- [ ] T008 Create models module file in `source/services/driver-service/src/models/mod.rs` — re-exports station

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Nearby Station Lookup (Priority: P1) 🎯 MVP

**Goal**: A driver can query nearby charging stations via `GET /api/v1/nearby?lat=&lng=&radius=` and get results sorted by distance.

**Independent Test**: `curl "http://localhost:3001/api/v1/nearby?lat=36.8&lng=10.18&radius=10000"` returns a JSON array of stations near Tunis with correct distance ordering.

### Implementation for User Story 1

- [ ] T009 [P] [US1] Implement API module root in `source/services/driver-service/src/api/mod.rs` — empty mod declarations for nearby and health
- [ ] T010 [P] [US1] Implement nearby handler in `source/services/driver-service/src/api/nearby.rs` — extract lat/lng/radius query params, validate bounds, call gis.get_nearby_stations via sqlx, return JSON array
- [ ] T011 [US1] Implement main.rs in `source/services/driver-service/src/main.rs` — bootstrap config, logging, pool, configure CORS middleware, register routes (nearby only), start Actix-web server

**Checkpoint**: At this point, `GET /api/v1/nearby` works independently. MVP deliverable.

---

## Phase 4: User Story 2 - Service Health (Priority: P2)

**Goal**: Traefik and operators can check service health via `GET /health`.

**Independent Test**: `curl "http://localhost:3001/health"` returns `{"status": "ok"}` when DB is reachable, `{"status": "degraded"}` (503) when DB is down.

### Implementation for User Story 2

- [ ] T012 [P] [US2] Implement health handler in `source/services/driver-service/src/api/health.rs` — try pool acquire with 500ms timeout, return 200/503 with status JSON
- [ ] T013 [US2] Register /health route in `source/services/driver-service/src/api/mod.rs` and add to main.rs

**Checkpoint**: Health endpoint functional. Both US1 and US2 independently testable.

---

## Phase 5: User Story 3 - Gateway Routing (Priority: P3)

**Goal**: Traefik reverse proxy routes `/api/v1/*` requests to the driver-service.

**Independent Test**: `curl "http://localhost/api/v1/nearby?lat=36.8&lng=10.18&radius=10000"` (via Traefik) returns the same response as hitting port 3001 directly.

### Implementation for User Story 3

- [ ] T014 [P] [US3] Create Traefik dynamic config in `source/infra/traefik/dynamic.yml` — router rule PathPrefix(/api/v1/) → driver-service:3001, health check on /health
- [ ] T015 [US3] Update `source/infra/docker-compose.yml` — add driver-service service (build context, env vars, port 3001), add traefik service (image, ports, volumes, dynamic config mount), add healthcheck on driver-service

**Checkpoint**: Traefik routes requests to driver-service correctly.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories, plus docs and validation.

- [ ] T016 [P] Create integration tests in `source/services/driver-service/tests/api_tests.rs` — test nearby with valid params, empty result, invalid params, health check
- [ ] T017 [P] Update `source/docs/system_state.md` and `source/docs/roadmap_status.md` with Sprint 1.2 completion
- [ ] T018 Update `source/docs/sprint_backlog.md` — mark Sprint 1.2 tasks complete
- [ ] T019 Run quickstart.md validation — verify all curl commands produce expected output

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3-5)**: All depend on Foundational phase completion
  - US1 (P1) → US2 (P2) → US3 (P3) sequential (main.rs must be updated each time)
- **Polish (Phase 6)**: Depends on all user stories complete

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational. No dependencies on other stories.
- **US2 (P2)**: Independent handler — only needs route registration in main.rs (which US1 created)
- **US3 (P3)**: Independent — only modifies infra files, no code changes in driver-service

### Within Each User Story

- Models before services
- Services/handlers before route registration
- Core implementation before integration

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel
- US1 handler (T010) and API module (T009) can run in parallel
- US2 handler (T012) and US3 files (T014) can run in parallel
- Polish tasks marked [P] can run in parallel

---

## Parallel Example: User Story 1

```bash
# Launch all [P] tasks for User Story 1 together:
Task: "T009 [P] [US1] Create API module root in source/services/driver-service/src/api/mod.rs"
Task: "T010 [P] [US1] Implement nearby handler in source/services/driver-service/src/api/nearby.rs"

# After both complete:
Task: "T011 [US1] Implement main.rs bootstrap in source/services/driver-service/src/main.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: US1 (nearby endpoint)
4. **STOP and VALIDATE**: `curl "http://localhost:3001/api/v1/nearby?lat=36.8&lng=10.18&radius=10000"` returns stations
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add US1 (nearby endpoint) → Test independently → **MVP!**
3. Add US2 (health endpoint) → Test independently
4. Add US3 (Traefik routing) → Test independently
5. Polish (tests, docs, validation)

### Parallel Team Strategy

With multiple developers:
1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: US1 (nearby handler + main.rs)
   - Developer B: US2 (health handler) + US3 (Traefik + docker-compose)
3. Stories integrate without conflicts (different files)
