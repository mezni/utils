# Tasks: Admin Service Core Operations

**Input**: Design documents from `specs/003-admin-flow/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are OPTIONAL - only include them if explicitly requested in the feature specification.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- Paths shown below assume `source/` monorepo structure:
  - `source/services/admin-service/` for the core microservice
  - `source/crates/db-models/` for shared database DTOs
  - `source/crates/validation/` for shared validation rules

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create `admin-service` directory structure in `source/services/admin-service/`
- [ ] T002 Initialize Rust project for `admin-service` with `actix-web`, `sqlx`, `serde`, `chrono`, `uuid`, `reqwest`, `tracing`, `redis` dependencies in `source/services/admin-service/Cargo.toml`
- [ ] T003 Create `db-models` crate in `source/crates/db-models/` with `Cargo.toml`
- [ ] T004 Create `validation` crate in `source/crates/validation/` with `Cargo.toml`
- [ ] T005 [P] Configure `admin-service` Dockerfile for multi-stage build (`rust:1.88-slim-bullseye` as builder, `debian:bookworm-slim` as runtime) in `source/services/admin-service/Dockerfile`
- [ ] T006 [P] Update `source/infra/docker-compose.yml` to include `admin-service` and its environment variables (`DATABASE_URL`, `REDIS_URL`, `KEYCLOAK_URL`, `KEYCLOAK_CLIENT_ID`, `PORT`, `RUST_LOG`)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T007 Implement base error types and `ResponseError` trait in `source/services/admin-service/src/error.rs`
- [ ] T008 Configure application logging with `tracing` and `EnvFilter` in `source/services/admin-service/src/main.rs`
- [ ] T009 Implement generic helper for `NanoID` entity ID generation in `source/services/admin-service/src/utils/id_generator.rs`
- [ ] T010 Implement Redis client initialization and connection pooling in `source/services/admin-service/src/redis.rs`
- [ ] T011 Implement `UserContext` extraction from Traefik headers (`X-User-Id`, `X-User-Roles`) in `source/services/admin-service/src/middleware/auth.rs`
- [ ] T012 Configure basic Actix-web server and JSON payload limits in `source/services/admin-service/src/main.rs`

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Partner Management (Priority: P1) 🎯 MVP

**Goal**: A partner administrator can create, retrieve, update, and soft-delete partner entities. Partners are assigned unique `OPR-` identifiers, and all mutations are transactional and logged to the audit trail.

**Independent Test**: Fully functional CRUD for partners, including database persistence, audit logging, and correct HTTP responses. Can be tested independently via API calls.

### Implementation for User Story 1

- [ ] T013 [P] [US1] Create `Partner` model (for `inventory.partners`) in `source/crates/db-models/src/partner.rs`
- [ ] T014 [P] [US1] Create `CreatePartnerRequest` and `UpdatePartnerRequest` DTOs in `source/services/admin-service/src/models/partner.rs`
- [ ] T015 [P] [USOR-1] Create `inventory` repository methods for partners in `source/services/admin-service/src/repositories/inventory.rs`
- [ ] T016 [US1] Implement `AdminOrchestrator::create_partner` to handle transactional writes, audit logging, and `NanoID` generation in `source/services/admin-service/src/services/admin_orchestrator.rs`
- [ ] T017 [US1] Implement `create_partner` route handler (`POST /api/v1/admin/partner`) in `source/services/admin-service/src/routes/partner.rs`
- [ ] T018 [US1] Implement `get_partner` route handler (`GET /api/v1/admin/partner/:id`) in `source/services/admin-service/src/routes/partner.rs`
- [ ] T019 [US1] Implement `update_partner` route handler (`PUT /api/v1/admin/partner/:id`) in `source/services/admin-service/src/routes/partner.rs`
- [ ] T020 [US1] Implement `delete_partner_soft` route handler (`DELETE /api/v1/admin/partner/:id`) in `source/services/admin-service/src/routes/partner.rs`
- [ ] T021 [US1] Add partner management routes to `source/services/admin-service/src/routes/mod.rs`
- [ ] T022 [US1] Add unit tests for partner database operations in `source/services/admin-service/tests/unit/repositories_test.rs`
- [ ] T023 [US1] Add integration tests for Partner CRUD (create, get, update, delete) in `source/services/admin-service/tests/integration/partner_test.rs`

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Station Management (Priority: P1)

**Goal**: A partner administrator can create, retrieve, update, and soft-delete station entities. Stations are assigned unique `STA-` identifiers, linked to a partner, geolocated, and all mutations are transactional and trigger MV refresh and Redis cache bust.

**Independent Test**: Fully functional CRUD for stations, including database persistence, audit logging, correct HTTP responses, successful MV refresh, and Redis cache bust. Can be tested via API calls.

### Implementation for User Story 2

- [ ] T024 [P] [US2] Create `Station` model (for `inventory.stations`) in `source/crates/db-models/src/station.rs`
- [ ] T025 [P] [US2] Create `CreateStationRequest` and `UpdateStationRequest` DTOs in `source/services/admin-service/src/models/station.rs`
- [ ] T026 [P] [US2] Implement `StationRepository` (create, get, update, soft-delete) in `source/services/admin-service/src/repositories/station.rs`
- [ ] T027 [US2] Implement `AdminOrchestrator::create_station` to handle transactional writes, audit logging, `NanoID` generation, MV refresh, and Redis cache bust in `source/services/admin-service/src/services/admin_orchestrator.rs`
- [ ] T028 [US2] Implement `create_station` route handler (`POST /api/v1/admin/station`) in `source/services/admin-service/src/routes/station.rs`
- [ ] T029 [US2] Implement `get_station` route handler (`GET /api/v1/admin/station/:id`) in `source/services/admin-service/src/routes/station.rs`
- [ ] T030 [US2] Implement `update_station` route handler (`PUT /api/v1/admin/station/:id`) in `source/services/admin-service/src/routes/station.rs`
- [ ] T031 [US2] Implement `delete_station_soft` route handler (`DELETE /api/v1/admin/station/:id`) in `source/services/admin-service/src/routes/station.rs`
- [ ] T032 [US2] Add station management routes to `source/services/admin-service/src/routes/mod.rs`
- [ ] T033 [US2] Add unit tests for `StationRepository` in `source/services/admin-service/tests/unit/repositories_test.rs`
- [ ] T034 [US2] Add integration tests for Station CRUD, MV refresh, and Redis cache bust in `source/services/admin-service/tests/integration/station_test.rs`

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Charger Management (Priority: P1)

**Goal**: A partner administrator can create, retrieve, update, and soft-delete charger entities. Chargers are assigned unique `CHG-` identifiers, linked to a station, have technical specifications, and all mutations are transactional and trigger MV refresh and Redis cache bust.

**Independent Test**: Fully functional CRUD for chargers, including database persistence, audit logging, correct HTTP responses, successful MV refresh, and Redis cache bust. Can be tested via API calls.

### Implementation for User Story 3

- [ ] T035 [P] [US3] Create `Charger` model (for `inventory.chargers`) in `source/crates/db-models/src/charger.rs`
- [ ] T036 [P] [US3] Create `CreateChargerRequest` and `UpdateChargerRequest` DTOs in `source/services/admin-service/src/models/charger.rs`
- [ ] T037 [P] [US3] Implement `ChargerRepository` (create, get, update, soft-delete) in `source/services/admin-service/src/repositories/charger.rs`
- [ ] T038 [US3] Implement `AdminOrchestrator::create_charger` to handle transactional writes, audit logging, `NanoID` generation, MV refresh, and Redis cache bust in `source/services/admin-service/src/services/admin_orchestrator.rs`
- [ ] T039 [US3] Implement `create_charger` route handler (`POST /api/v1/admin/charger`) in `source/services/admin-service/src/routes/charger.rs`
- [ ] T040 [US3] Implement `get_charger` route handler (`GET /api/v1/admin/charger/:id`) in `source/services/admin-service/src/routes/charger.rs`
- [ ] T041 [US3] Implement `update_charger` route handler (`PUT /api/v1/admin/charger/:id`) in `source/services/admin-service/src/routes/charger.rs`
- [ ] T042 [US3] Implement `delete_charger_soft` route handler (`DELETE /api/v1/admin/charger/:id`) in `source/services/admin-service/src/routes/charger.rs`
- [ ] T043 [US3] Add charger management routes to `source/services/admin-service/src/routes/mod.rs`
- [ ] T044 [US3] Add unit tests for `ChargerRepository` in `source/services/admin-service/tests/unit/repositories_test.rs`
- [ ] T045 [US3] Add integration tests for Charger CRUD, MV refresh, and Redis cache bust in `source/services/admin-service/tests/integration/charger_test.rs`

**Checkpoint**: At this point, User Stories 1, 2, and 3 should work independently

---

## Phase 6: User Story 4 - Idempotent Operations (Priority: P2)

**Goal**: A partner administrator submits the same POST request multiple times (e.g., due to network retry). The system detects the duplicate request using an idempotency key and returns the original response without re-executing the mutation.

**Why this priority**: Prevents duplicate partner, station, or charger creation from network retries. Critical for data integrity but not as foundational as the CRUD operations themselves.

**Independent Test**: Can be fully tested by making two identical POST requests with the same idempotency key within 24 hours and verifying the second request returns the original response with `Idempotency-Replayed: true` header.

### Implementation for User Story 4

- [ ] T046 [P] [US4] Implement `IdempotencyMiddleware` to check for `Idempotency-Key` header and handle duplicate requests in `source/services/admin-service/src/middleware/idempotency.rs`
- [ ] T047 [US4] Integrate `IdempotencyMiddleware` into Actix-web application in `source/services/admin-service/src/main.rs`
- [ ] T048 [US4] Implement `Redis` client for idempotency key storage and retrieval (`idempotency:{key}` namespace with 24h TTL) in `source/services/admin-service/src/redis.rs`
- [ ] T049 [US4] Add unit tests for `IdempotencyMiddleware` in `source/services/admin-service/tests/unit/middleware_test.rs`
- [ ] T050 [US4] Add integration tests for idempotent POST requests in `source/services/admin-service/tests/integration/idempotency_test.rs`

---

## Phase 7: User Story 5 - Transactional Consistency (Priority: P2)

**Goal**: All database operations within a single request complete successfully before returning the response, maintaining ACID properties.

**Independent Test**: Make a mutation that involves multiple database operations and verify either all operations commit or none do, with appropriate error handling.

### Implementation for User Story 5

- [ ] T051 [US5] Ensure all multi-table data modifications are wrapped in a single `sqlx::Transaction` within `AdminOrchestrator` methods in `source/services/admin-service/src/services/admin_orchestrator.rs`
- [ ] T052 [US5] Implement robust error handling for transaction failures (rollback and return 500 Internal Server Error or 409 Conflict) in `source/services/admin-service/src/services/admin_orchestrator.rs`
- [ ] T053 [US5] Add integration tests for transactional consistency, including scenarios where one operation fails and the transaction rolls back in `source/services/admin-service/tests/integration/transaction_test.rs`

---

## Phase 8: User Story 6 - Audit Trail (Priority: P2)

**Goal**: Every mutation performed by authenticated users is logged to an audit log with comprehensive information about what changed, when it changed, and who made the change, including before and after snapshots.

**Independent Test**: Make a mutation and query the audit log to verify it contains the correct actor, action, target information, and before/after snapshots.

### Implementation for User Story 6

- [ ] T054 [P] [US6] Create `AuditLog` model (for `analytics_db.audit_log`) in `source/crates/db-models/src/audit_log.rs`
- [ ] T055 [P] [US6] Implement `AuditRepository` for inserting audit log entries into `analytics_db` in `source/services/admin-service/src/repositories/audit.rs`
- [ ] T056 [US6] Implement `AdminOrchestrator::log_audit_event` to compute BEFORE/AFTER snapshots and insert into audit log in `source/services/admin-service/src/services/admin_orchestrator.rs`
- [ ] T057 [US6] Integrate `AdminOrchestrator::log_audit_event` into all `create`, `update`, and `delete_soft` methods in `source/services/admin-service/src/services/admin_orchestrator.rs`
- [ ] T058 [US6] Ensure repository layer is audit-unaware (no audit logic in `partner.rs`, `station.rs`, `charger.rs` repositories)
- [ ] T059 [US6] Add unit tests for `AuditRepository` in `source/services/admin-service/tests/unit/repositories_test.rs`
- [ ] T060 [US6] Add integration tests for audit logging with before/after snapshots in `source/services/admin-service/tests/integration/audit_log_test.rs`

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T061 [P] Ensure all validation rules from `data-model.md` and `api-contracts.md` are implemented in `source/crates/validation/` and used in route handlers.
- [ ] T062 Review and refine all error messages and responses to match `error-contracts.md` in `source/services/admin-service/src/error.rs` and route handlers.
- [ ] T063 Add comprehensive unit tests for all DTOs and models in `source/services/admin-service/tests/unit/models_test.rs`.
- [ ] T064 Run `cargo clippy -- -D warnings` and fix all warnings in `source/services/admin-service/`.
- [ ] T065 Update `quickstart.md` with additional testing scenarios and troubleshooting tips.
- [ ] T066 Update documentation (`docs/roadmap_status.md`, `docs/sprint_backlog.md`, `docs/SYSTEM_STATE.md`) to reflect Admin Service Sprint 2 completion.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-8)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Phase 9)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1 - Partner Management)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P1 - Station Management)**: Can start after Foundational (Phase 2) - Depends on US1 (partners must exist to create stations)
- **User Story 3 (P1 - Charger Management)**: Can start after Foundational (Phase 2) - Depends on US2 (stations must exist to create chargers)
- **User Story 4 (P2 - Idempotent Operations)**: Can start after Foundational (Phase 2) - Can be implemented in parallel with P1 stories, but needs Redis.
- **User Story 5 (P2 - Transactional Consistency)**: Can start after Foundational (Phase 2) - Applies to all transactional writes in P1 stories.
- **User Story 6 (P2 - Audit Trail)**: Can start after Foundational (Phase 2) - Applies to all mutations in P1 stories.

### Within Each User Story

- Models before repositories/services
- Repositories before orchestrators
- Orchestrators before route handlers
- Unit tests before integration tests
- Core implementation before cross-cutting concerns within the story

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes:
  - US1, US4, US5, US6 can start in parallel (US1 is foundational for US2/US3, but the others are cross-cutting)
  - US2 can start after US1 is complete
  - US3 can start after US2 is complete
- All tests for a user story marked [P] can run in parallel
- Models within a story marked [P] can run in parallel
- `source/crates/db-models` and `source/crates/validation` can be developed in parallel to the `admin-service` core logic.

---

## Parallel Example: User Story 1 (P1 - Partner Management)

```bash
# Foundational tasks completed.
# Developer A starts US1: Partner Management
Task: "Create Partner model (for inventory.partners) in source/crates/db-models/src/partner.rs"
Task: "Create CreatePartnerRequest and UpdatePartnerRequest DTOs in source/services/admin-service/src/models/partner.rs"
Task: "Implement PartnerRepository (create, get, update, soft-delete) in source/services/admin-service/src/repositories/partner.rs"

# Once these are done, Developer A continues with orchestrator and routes for US1:
Task: "Implement AdminOrchestrator::create_partner to handle transactional writes, audit logging, NanoID generation in source/services/admin-service/src/services/admin_orchestrator.rs"
Task: "Implement create_partner route handler (POST /api/v1/admin/partner) in source/services/admin-service/src/routes/partner.rs"
Task: "Add partner management routes to source/services/admin-service/src/routes/mod.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1 (Partner Management)
4. **STOP and VALIDATE**: Test User Story 1 independently (full CRUD for partners)
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 (Partner Management) → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 (Station Management) → Test independently → Deploy/Demo
4. Add User Story 3 (Charger Management) → Test independently → Deploy/Demo
5. Add User Story 4 (Idempotent Operations) → Test independently → Deploy/Demo
6. Add User Story 5 (Transactional Consistency) → Test independently → Deploy/Demo
7. Add User Story 6 (Audit Trail) → Test independently → Deploy/Demo
8. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - **Developer A**: User Story 1 (Partner Management)
   - **Developer B**: User Story 4 (Idempotent Operations), User Story 5 (Transactional Consistency), User Story 6 (Audit Trail) - These can be integrated into US1 later or as features become available.
   - **Developer C**: User Story 2 (Station Management, once US1 is done)
   - **Developer D**: User Story 3 (Charger Management, once US2 is done)
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
