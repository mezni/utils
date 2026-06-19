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
- [ ] T013 Implement `IdempotencyMiddleware` to check for `Idempotency-Key` header and handle duplicate requests in `source/services/admin-service/src/middleware/idempotency.rs`
- [ ] T014 Implement Redis client for idempotency key storage and retrieval (`idempotency:{key}` namespace with 24h TTL) in `source/services/admin-service/src/redis.rs`
- [ ] T015 Implement `TraefikHeaderValidationMiddleware` to validate required headers (`X-User-Id`, `X-User-Roles`) in `source/services/admin-service/src/middleware/traefik_validation.rs`
- [ ] T016 Implement `RoleEnforcementMiddleware` to allow `role:admin`, `role:partner` and reject 403 forbidden in `source/services/admin-service/src/middleware/role_enforcement.rs`

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: Transaction Orchestrator (Blocking Prerequisites)

**Purpose**: Core transaction orchestrator that enforces post-commit steps (audit, cache bust, MV refresh)

**⚠️ CRITICAL**: Must be complete before all CRUD operations

- [ ] T017 Implement `AdminOrchestrator` with transaction workflow: BEGIN TX → WRITE ENTITY → COMMIT → REFRESH MATERIALIZED VIEW → REDIS INVALIDATION → AUDIT LOG WRITE in `source/services/admin-service/src/services/admin_orchestrator.rs`
- [ ] T018 Implement `AuditDiffService` to capture BEFORE snapshot, capture AFTER snapshot, build JSON diff payload, insert analytics_db.audit_log in `source/services/admin-service/src/services/audit_service.rs` and `source/services/admin-service/src/models/audit.rs`
- [ ] T019 Implement `CacheBustService` to invalidate Redis caches (stations:tile:*, stations:near:*) with warning-only on failure and add X-Cache-Bust-Failed header in `source/services/admin-service/src/services/cache_service.rs`
- [ ] T020 Implement `MVRefreshService` to execute REFRESH MATERIALIZED VIEW CONCURRENTLY inventory.mv_stations_geo and inventory.mv_stations_summary with warning on failure in `source/services/admin-service/src/services/materialized_view_service.rs`

**Checkpoint**: Transaction orchestrator ready with audit, cache bust, and MV refresh

---

## Phase 4: User Story 1 - Partner Management (Priority: P1) 🎯 MVP

**Goal**: A partner administrator can create, retrieve, update, and soft-delete partner entities. Partners are assigned unique `OPR-` identifiers, and all mutations are transactional and logged to the audit trail.

**Independent Test**: Fully functional CRUD for partners, including database persistence, audit logging, and correct HTTP responses. Can be tested independently via API calls.

### Implementation for User Story 1

- [ ] T021 [P] [US1] Create `Partner` model (for `inventory.partners`) in `source/crates/db-models/src/partner.rs`
- [ ] T022 [P] [US1] Create `CreatePartnerRequest` and `UpdatePartnerRequest` DTOs in `source/services/admin-service/src/models/partner.rs`
- [ ] T023 [P] [USOR-1] Create `inventory` repository methods for partners in `source/services/admin-service/src/repositories/partner_repository.rs`
- [ ] T024 [US1] Implement `create_partner` endpoint (`POST /api/v1/admin/partner`) with transaction and audit in `source/services/admin-service/src/routes/partner/create.rs`
- [ ] T025 [US1] Implement `update_partner` endpoint (`PUT /api/v1/admin/partner/:id`) with BEFORE snapshot capture in `source/services/admin-service/src/routes/partner/update.rs`
- [ ] T026 [US1] Implement `get_partner` endpoint (`GET /api/v1/admin/partner/:id`) in `source/services/admin-service/src/routes/partner/get.rs`
- [ ] T027 [US1] Implement `delete_partner_soft` endpoint (`DELETE /api/v1/admin/partner/:id`) with audit logging in `source/services/admin-service/src/routes/partner/delete.rs`
- [ ] T028 [US1] Add partner management routes to `source/services/admin-service/src/routes/partner/mod.rs`
- [ ] T029 [US1] Add partner management module to `source/services/admin-service/src/routes/mod.rs`
- [ ] T030 [US1] Add unit tests for partner repository in `source/services/admin-service/tests/unit/partner_repository_test.rs`
- [ ] T031 [US1] Add integration tests for Partner CRUD in `source/services/admin-service/tests/integration/partner_crud_test.rs`

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 5: User Story 2 - Station Management (Priority: P1)

**Goal**: A partner administrator can create, retrieve, update, and soft-delete station entities. Stations are assigned unique `STA-` identifiers, linked to a partner, geolocated, and all mutations are transactional and trigger MV refresh and Redis cache bust.

**Independent Test**: Fully functional CRUD for stations, including database persistence, audit logging, correct HTTP responses, successful MV refresh, and Redis cache bust. Can be tested via API calls.

### Implementation for User Story 2

- [ ] T032 [P] [US2] Create `Station` model (for `inventory.stations`) in `source/crates/db-models/src/station.rs`
- [ ] T033 [P] [US2] Create `CreateStationRequest` and `UpdateStationRequest` DTOs in `source/services/admin-service/src/models/station.rs`
- [ ] T034 [P] [US2] Implement `StationRepository` (create, get, update, soft-delete) in `source/services/admin-service/src/repositories/station_repository.rs`
- [ ] T035 [US2] Implement `create_station` endpoint (`POST /api/v1/admin/station`) with transaction, audit, MV refresh, and cache bust in `source/services/admin-service/src/routes/station/create.rs`
- [ ] T036 [US2] Implement `update_station` endpoint (`PUT /api/v1/admin/station/:id`) with BEFORE snapshot capture, MV refresh, and cache bust in `source/services/admin-service/src/routes/station/update.rs`
- [ ] T037 [US2] Implement `get_station` endpoint (`GET /api/v1/admin/station/:id`) in `source/services/admin-service/src/routes/station/get.rs`
- [ ] T038 [US2] Implement `delete_station_soft` endpoint (`DELETE /api/v1/admin/station/:id`) with audit, MV refresh, and cache bust in `source/services/admin-service/src/routes/station/delete.rs`
- [ ] T039 [US2] Add station management routes to `source/services/admin-service/src/routes/station/mod.rs`
- [ ] T040 [US2] Add station management module to `source/services/admin-service/src/routes/mod.rs`
- [ ] T041 [US2] Add unit tests for station repository in `source/services/admin-service/tests/unit/station_repository_test.rs`
- [ ] T042 [US2] Add integration tests for Station CRUD, MV refresh, and cache bust in `source/services/admin-service/tests/integration/station_crud_test.rs`

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 6: User Story 3 - Charger Management (Priority: P1)

**Goal**: A partner administrator can create, retrieve, update, and soft-delete charger entities. Chargers are assigned unique `CHG-` identifiers, linked to a station, have technical specifications, and all mutations are transactional and trigger MV refresh and Redis cache bust.

**Independent Test**: Fully functional CRUD for chargers, including database persistence, audit logging, correct HTTP responses, successful MV refresh, and Redis cache bust. Can be tested via API calls.

### Implementation for User Story 3

- [ ] T043 [P] [US3] Create `Charger` model (for `inventory.chargers`) in `source/crates/db-models/src/charger.rs`
- [ ] T044 [P] [US3] Create `CreateChargerRequest` and `UpdateChargerRequest` DTOs in `source/services/admin-service/src/models/charger.rs`
- [ ] T045 [P] [US3] Implement `ChargerRepository` (create, get, update, soft-delete) in `source/services/admin-service/src/repositories/charger_repository.rs`
- [ ] T046 [US3] Implement `create_charger` endpoint (`POST /api/v1/admin/charger`) with transaction, audit, MV refresh, and cache bust in `source/services/admin-service/src/routes/charger/create.rs`
- [ ] T047 [US3] Implement `update_charger` endpoint (`PUT /api/v1/admin/charger/:id`) with BEFORE snapshot capture, MV refresh, and cache bust in `source/services/admin-service/src/routes/charger/update.rs`
- [ ] T048 [US3] Implement `get_charger` endpoint (`GET /api/v1/admin/charger/:id`) in `source/services/admin-service/src/routes/charger/get.rs`
- [ ] T049 [US3] Implement `delete_charger_soft` endpoint (`DELETE /api/v1/admin/charger/:id`) with audit, MV refresh, and cache bust in `source/services/admin-service/src/routes/charger/delete.rs`
- [ ] T050 [US3] Add charger management routes to `source/services/admin-service/src/routes/charger/mod.rs`
- [ ] T051 [US3] Add charger management module to `source/services/admin-service/src/routes/mod.rs`
- [ ] T052 [US3] Add unit tests for charger repository in `source/services/admin-service/tests/unit/charger_repository_test.rs`
- [ ] T053 [US3] Add integration tests for Charger CRUD, MV refresh, and cache bust in `source/services/admin-service/tests/integration/charger_crud_test.rs`

**Checkpoint**: At this point, User Stories 1, 2, and 3 should work independently

---

## Phase 7: Testing & Validation

**Purpose**: Verify all acceptance criteria and operational requirements

- [ ] T054 [P] Add transaction failure tests in `source/services/admin-service/tests/integration/transaction_failure_test.rs` — verify rollback behavior
- [ ] T055 [P] Add idempotency replay tests in `source/services/admin-service/tests/integration/idempotency_replay_test.rs` — verify same key + same response + no duplicate rows
- [ ] T056 [P] Add DB role enforcement tests in `source/services/admin-service/tests/integration/db_permissions_test.rs` — verify admin_service_role can write inventory, driver_service_role cannot write inventory, auth_service_role cannot access inventory
- [ ] T057 Add integration tests for Partner, Station, and Charger all endpoints in `source/services/admin-service/tests/integration/full_crud_test.rs`
- [ ] T058 Add performance/load tests with 100 concurrent requests in `source/services/admin-service/tests/load/admin_load_test.py` — verify no degradation
- [ ] T059 Add OpenAPI contract tests for all endpoints in `source/services/admin-service/tests/contracts/api_contract_test.rs`
- [ ] T060 Add failure scenario tests (malformed JSON, missing fields, Keycloak unavailable, Traefik header missing) in `source/services/admin-service/tests/integration/failure_scenarios_test.rs`

---

## Phase 8: Polish & Documentation

**Purpose**: Production deployment and documentation

- [ ] T061 [P] Create production Dockerfile at `source/services/admin-service/Dockerfile` (multi-stage, distroless runtime)
- [ ] T062 Add environment configuration module in `src/config.rs` loading from environment variables (Database URL, Redis URL, Keycloak URL, Keycloak Client ID, listen port)
- [ ] T063 Update `docs/SYSTEM_STATE.md` to reflect Admin Service deployment
- [ ] T064 Update `docs/sprint_backlog.md` to reflect Sprint 2 completion

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **Transaction Orchestrator (Phase 3)**: Depends on Foundational - BLOCKS all CRUD operations
- **User Stories (Phase 4-6)**: All depend on Transaction Orchestrator completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Testing & Validation (Phase 7)**: Depends on all CRUD operations being complete
- **Polish & Documentation (Phase 8)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1 - Partner Management)**: Can start after Transaction Orchestrator - No dependencies on other stories
- **User Story 2 (P1 - Station Management)**: Can start after Transaction Orchestrator - No dependencies on US1 (independent repositories)
- **User Story 3 (P1 - Charger Management)**: Can start after Transaction Orchestrator - No dependencies on US1/US2 (independent repositories)
- **User Stories 1, 2, 3 can proceed in parallel** once Transaction Orchestrator is complete

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes:
  - T013, T014, T015, T016 (middleware) can run in parallel
  - User Stories 1, 2, 3 can all start in parallel (independent repositories)
- Within each user story:
  - Models (T021, T032, T043) can run in parallel
  - DTOs (T022, T033, T044) can run in parallel
  - Repositories (T023, T034, T045) can run in parallel
  - Core routes (create/update/get/delete) can run in parallel
- All tests for a user story marked [P] can run in parallel
- `source/crates/db-models` and `source/crates/validation` can be developed in parallel to the `admin-service` core logic.

---

## Parallel Example: User Story 1 (P1 - Partner Management)

```bash
# Transaction Orchestrator completed.
# Developer A starts US1: Partner Management
Task: "Create Partner model (for inventory.partners) in source/crates/db-models/src/partner.rs"
Task: "Create CreatePartnerRequest and UpdatePartnerRequest DTOs in source/services/admin-service/src/models/partner.rs"
Task: "Implement PartnerRepository (create, get, update, soft-delete) in source/services/admin-service/src/repositories/partner_repository.rs"

# Once these are done, Developer A continues with routes for US1:
Task: "Implement create_partner endpoint (POST /api/v1/admin/partner) with transaction and audit in source/services/admin-service/src/routes/partner/create.rs"
Task: "Implement update_partner endpoint (PUT /api/v1/admin/partner/:id) with BEFORE snapshot capture in source/services/admin-service/src/routes/partner/update.rs"
Task: "Implement get_partner endpoint (GET /api/v1/admin/partner/:id) in source/services/admin-service/src/routes/partner/get.rs"
Task: "Implement delete_partner_soft endpoint (DELETE /api/v1/admin/partner/:id) with audit logging in source/services/admin-service/src/routes/partner/delete.rs"
```

---

## Parallel Example: User Stories 2 & 3 (after Transaction Orchestrator)

```bash
# All middleware and orchestrator done.
# With 3 developers available:
Developer A: User Story 1 (Partner Management)
Developer B: User Story 2 (Station Management)
Developer C: User Story 3 (Charger Management)

# Within each story:
Developer A (US1):
  - Create Partner model
  - Create DTOs
  - Implement PartnerRepository
  - Implement create_partner route
  - Implement update_partner route
  - Implement get_partner route
  - Implement delete_partner route
  - Write unit tests
  - Write integration tests

# Parallel work:
Developer B (US2) starts after US1 models are done:
  - Create Station model (depends on Partner)
  - Create DTOs (can run in parallel)
  - Implement StationRepository (can run in parallel)
  - Implement station routes (can run in parallel)
  - Write tests (can run in parallel)

Developer C (US3) starts after US2 models are done:
  - Create Charger model
  - Create DTOs (can run in parallel)
  - Implement ChargerRepository (can run in parallel)
  - Implement charger routes (can run in parallel)
  - Write tests (can run in parallel)
```

---

## Implementation Strategy

### MVP First (User Stories 1-3 All Complete)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: Transaction Orchestrator
4. Complete Phase 4-6: User Stories 1, 2, 3 (all P1 stories in parallel if staffed)
5. **STOP and VALIDATE**: Test all CRUD operations independently (partners, stations, chargers)
6. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational + Transaction Orchestrator → Foundation ready
2. Add User Story 1 (Partner Management) → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 (Station Management) → Test independently → Deploy/Demo
4. Add User Story 3 (Charger Management) → Test independently → Deploy/Demo
5. Add Phase 7: Testing & Validation → Test independently → Deploy/Demo
6. Add Phase 8: Polish & Documentation → Test independently → Deploy/Demo
7. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - **Developer A**: Phase 3 (Transaction Orchestrator)
   - **Developer B**: User Story 1 (Partner Management)
   - **Developer C**: User Story 2 (Station Management) - starts when Partner model is done
   - **Developer D**: User Story 3 (Charger Management) - starts when Station model is done
3. Once Transaction Orchestrator completes:
   - **Developer A**: Integrates audit, cache bust, MV refresh into all CRUD operations
   - **Developer B**: User Story 1 routes and tests
   - **Developer C**: User Story 2 routes and tests
   - **Developer D**: User Story 3 routes and tests
4. Stories complete and integrate independently

---

## Sprint Exit Criteria

Sprint 2 should not close until all of the following are verified:

- ✅ Partner CRUD operational (create, update, get, soft-delete)
- ✅ Station CRUD operational (create, update, get, soft-delete)
- ✅ Charger CRUD operational (create, update, get, soft-delete)
- ✅ sqlx transactions used everywhere
- ✅ BEFORE/AFTER audit logs stored in analytics_db.audit_log
- ✅ Redis invalidation after commit (stations:tile:*, stations:near:*)
- ✅ MV refresh after commit (inventory.mv_stations_geo, inventory.mv_stations_summary)
- ✅ Idempotency-Key support working (IdempotencyMiddleware, Redis storage)
- ✅ Traefik header validation working (X-User-Id, X-User-Roles)
- ✅ Role enforcement working (role:admin, role:partner)
- ✅ DB role isolation verified (admin_service_role, driver_service_role, auth_service_role)
- ✅ Integration tests passing (transaction failure, idempotency replay, DB permissions, full CRUD, failure scenarios, OpenAPI contracts)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
- Idempotency-Key must be validated (UUID v4) and stored with 24h TTL
- BEFORE/AFTER audit snapshots required for all UPDATE and DELETE operations
- Redis invalidation must be warning-only on failure (X-Cache-Bust-Failed header)
- MV refresh must use CONCURRENTLY to avoid table locks
- Transaction orchestrator must catch errors at each step and rollback appropriately
- Traefik headers (X-User-Id, X-User-Roles) are required for all endpoints
- Role enforcement must allow admin and partner roles, reject all others
