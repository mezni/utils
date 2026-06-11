# Tasks: Clickstream Service — Event Ingestion

**Input**: Design documents from `/specs/004-clickstream-service/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Not explicitly requested — manual verification per user story's "Independent Test" criteria.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- Workspace root: `source/Cargo.toml`
- Service: `source/services/clickstream-service/`
- Paths shown are relative to `source/services/clickstream-service/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Create clickstream-service crate at source/services/clickstream-service/Cargo.toml with dependencies on actix-web 4, serde 1, serde_json 1, sqlx 0.8 (postgres, runtime-tokio-native-tls), tokio 1, tracing 0.1, tracing-actix-web 0.7, tracing-subscriber 0.3 (json), actix-governor 0.6, nanoid 0.4
- [X] T002 Create directory structure at source/services/clickstream-service/ (src/routes/, src/models/, src/db/, src/middleware/, migrations/)
- [X] T003 Add `clickstream-service` to workspace members in source/Cargo.toml

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T004 Create migration SQL for raw_events table in source/services/clickstream-service/migrations/001_create_raw_events.sql with columns: id (BIGSERIAL PK), batch_id (VARCHAR(21) NOT NULL), event_name (VARCHAR(50) NOT NULL), user_id (VARCHAR(255)), session_id (VARCHAR(255) NOT NULL), payload (JSONB), client_ts (TIMESTAMPTZ NOT NULL), server_ts (TIMESTAMPTZ NOT NULL DEFAULT NOW()), ip_address (VARCHAR(45)); plus indexes on event_name, server_ts, session_id
- [X] T005 [P] Implement AppError enum in source/services/clickstream-service/src/errors.rs with variants for all error codes (InvalidEventName, MissingSessionId, InvalidTimestamp, InvalidPayload, PayloadTooLarge, BatchSizeExceeded, BatchTooLarge, InvalidJson, UnsupportedMediaType, DbDisconnected, RateLimited); implement actix_web::ResponseError to serialize into the envelope format with appropriate HTTP status codes
- [X] T006 [P] Implement generic ApiResponse<T> envelope in source/services/clickstream-service/src/response.rs with fields data: Option<T>, error: Option<ApiError>, meta: Meta; implement Serialize and helper constructors (success, error)
- [X] T007 [P] Implement Event struct in source/services/clickstream-service/src/models/event.rs with fields event_name, user_id (Option), session_id, client_ts (DateTime<Utc>), payload (Option<Value>); implement Deserialize with custom validation to enforce MVP-1 taxonomy, non-empty event_name/session_id, valid ISO 8601 client_ts, and 64KB max size
- [X] T008 [P] Implement per-IP token bucket rate limiter middleware in source/services/clickstream-service/src/middleware/rate_limiter.rs using actix-governor with configurable burst size (default 100)
- [X] T009 [P] Implement AnalyticsDbRepo in source/services/clickstream-service/src/db/repository.rs with methods: new(pool) -> Self, insert_event(event, batch_id, ip) -> Result, insert_batch(events, batch_id, ip) -> Result (Vec<InsertResult>), health_check() -> Result<bool>; pool sourced from DATABASE_URL_ANALYTICS env var
- [X] T010 Configure application state (AppState with AnalyticsDbRepo), tracing subscriber (JSON output, RUST_LOG), and server bootstrap in source/services/clickstream-service/src/main.rs with bind address from CLICKSTREAM_BIND_ADDR env var (default 0.0.0.0:8082); run embedded sqlx migrations on startup

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 — Ingest Single Event (Priority: P1) 🎯 MVP

**Goal**: Accept POST /api/v1/events with a single event JSON body, validate, store in analytics_db, return 202 within 100ms

**Independent Test**: Send `curl -X POST http://localhost:8082/api/v1/events -H "Content-Type: application/json" -d '{"event_name":"map_open","session_id":"sess_1","client_ts":"2026-06-11T12:00:00Z"}'` and verify 202 response; send invalid event_name and verify 422 with field-level error

### Implementation for User Story 1

- [X] T011 [P] [US1] Create routes module at source/services/clickstream-service/src/routes/mod.rs re-exporting ingest and health route modules
- [X] T012 [P] [US1] Implement POST /api/v1/events handler in source/services/clickstream-service/src/routes/ingest.rs that deserializes request body into Event, validates, spawns a tokio task to call repo.insert_event(), and immediately returns 202 with ApiResponse containing the batch_id
- [X] T013 [US1] Wire routes, rate limiter middleware, and AppState into the Actix-web HttpServer in source/services/clickstream-service/src/main.rs

**Checkpoint**: User Story 1 should be fully functional — single event ingestion works end-to-end

---

## Phase 4: User Story 2 — Ingest Batch Events (Priority: P1)

**Goal**: Accept POST /api/v1/events/batch with a JSON array of events (1-100), validate each, store valid ones, return 202 with partial success info

**Independent Test**: Send `curl -X POST http://localhost:8082/api/v1/events/batch -H "Content-Type: application/json" -d '[{"event_name":"map_open","session_id":"sess_1","client_ts":"2026-06-11T12:00:00Z"}]'` and verify 202 with accepted=1; include one invalid event and verify accepted+failed breakdown

### Implementation for User Story 2

- [X] T014 [P] [US2] Add insert_batch method to AnalyticsDbRepo in source/services/clickstream-service/src/db/repository.rs that accepts Vec<Event> + batch_id + ip, validates each event individually, inserts only valid ones, and returns Vec<InsertResult> with per-event status
- [X] T015 [US2] Implement POST /api/v1/events/batch handler in source/services/clickstream-service/src/routes/ingest.rs that validates structural constraints (1-100 events, total ≤512KB), validates each event individually, calls repo.insert_batch() in a spawned task, and returns 202 with { batch_id, accepted, failed }
- [X] T016 [US2] Register batch route in source/services/clickstream-service/src/routes/mod.rs

**Checkpoint**: User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 — Health Check (Priority: P3)

**Goal**: Return service status and database connectivity via GET /api/v1/health

**Independent Test**: Call `curl http://localhost:8082/api/v1/health` and verify 200 with database=connected; stop analytics_db and verify 503 with database=disconnected

### Implementation for User Story 3

- [X] T017 [P] [US3] Implement GET /api/v1/health handler in source/services/clickstream-service/src/routes/health.rs that calls repo.health_check() and returns 200 with status=ok/database=connected or 503 with status=degraded/database=disconnected
- [X] T018 [US3] Register health route in source/services/clickstream-service/src/routes/mod.rs

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [X] T019 [P] Add structured tracing events (event_name, server_ts, result) for every ingested event in source/services/clickstream-service/src/routes/ingest.rs per FR-011
- [X] T020 [P] Add env configuration module at source/services/clickstream-service/src/config.rs to read DATABASE_URL_ANALYTICS, CLICKSTREAM_BIND_ADDR, RATE_LIMIT_BURST_SIZE with sensible defaults
- [X] T021 Run `cargo build -p clickstream-service` and fix all compilation errors
- [X] T022 Run quickstart.md end-to-end: start analytics_db via Docker, migrate, run service, verify all 3 endpoints respond correctly
- [X] T023 Add load test script at tests/load/clickstream-load.sh using oha to verify SC-004 (500 concurrent requests, p95 <200ms)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3-5)**: All depend on Foundational phase completion
  - US1 (Phase 3) → US2 (Phase 4) builds on US1's route structure
  - US3 (Phase 5) depends on Foundational only, independent of US1/US2
- **Polish (Phase 6)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational — no dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational — shares ingest route module from US1 but is independently testable
- **User Story 3 (P3)**: Can start after Foundational — no dependencies on US1 or US2

### Within Each User Story

- Models before services
- Services before routes
- Routes before wiring into server
- Story complete before moving to next priority

### Parallel Opportunities

- T005, T006, T007, T008, T009 marked [P] can run in parallel (Phase 2)
- T011, T012 marked [P] can run in parallel (Phase 3)
- T017 marked [P] can run independently (Phase 5)
- T019, T020 marked [P] can run in parallel (Phase 6)

---

## Parallel Example: User Story 1

```bash
# Launch all [P] tasks for User Story 1 together:
Task: "Create routes module in src/routes/mod.rs"
Task: "Implement POST /api/v1/events handler in src/routes/ingest.rs"

# Then wire them:
Task: "Wire routes and state in src/main.rs"
```

## Parallel Example: User Story 2

```bash
# Dependent on US1's ingest.rs — sequential within US2:
Task: "Add batch insert to AnalyticsDbRepo in src/db/repository.rs"
Task: "Implement batch handler in src/routes/ingest.rs"
Task: "Register batch route in src/routes/mod.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1 (single event ingestion)
4. **STOP and VALIDATE**: Test User Story 1 independently (curl POST /api/v1/events)
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 (single event) → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 (batch events) → Test independently → Deploy/Demo
4. Add User Story 3 (health check) → Test independently → Deploy/Demo
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (single event)
   - Developer B: User Story 3 (health check)
   - Developer A continues: User Story 2 (batch events)
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- No automated tests requested in spec — manual verification is sufficient for MVP-1
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
