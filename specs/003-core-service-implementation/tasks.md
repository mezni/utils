# Tasks: Core Service Implementation

**Input**: Design documents from `/specs/003-core-service-implementation/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are included as required by Principle VII (Quality & Testing Discipline) from the constitution.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Phase 1: Setup (Project Structure)

**Purpose**: Initialize Rust project with core-service structure

- [ ] T001 Create core-service directory structure per implementation plan
- [ ] T002 Initialize Rust project with Cargo.toml and dependencies (Actix Web 4.x, SQLx 0.7, jsonwebtoken 8.x, etc.)
- [ ] T003 [P] Configure Rust toolchain (fmt, clippy, test) in .rust-toolchain and Cargo.toml

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 Setup database connection pooling with SQLx in services/core-service/src/utils/database.rs
- [ ] T005 [P] Implement JWT validation middleware in services/core-service/src/middleware/auth.rs
- [ ] T006 [P] Setup Actix Web routing structure in services/core-service/src/main.rs
- [ ] T007 Create error handling infrastructure with thiserror in services/core-service/src/middleware/error.rs
- [ ] T008 Configure environment management with config crate in services/core-service/src/config.rs
- [ ] T009 Setup logging with tracing in services/core-service/src/utils/logging.rs
- [ ] T010 Create base models with common fields (created_at, updated_at, deleted_at) in services/core-service/src/models/mod.rs
- [ ] T011 Setup SQLx migrations structure in services/core-service/migrations/
- [ ] T011a Create outbox table migration in services/core-service/migrations/XXXX_create_outbox_table.sql
- [ ] T011b [P] Create OutboxEvent model in services/core-service/src/models/outbox.rs
- [ ] T011c Setup RabbitMQ connection configuration in services/core-service/src/utils/rabbitmq.rs
- [ ] T010a Add soft-delete fields to infrastructure entity models in services/core-service/src/models/
- [ ] T004a [P] Implement database connection resilience with retry logic in services/core-service/src/utils/database.rs
- [ ] T004b [P] Add database health check in services/core-service/src/handlers/health_handler.rs

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Basic Core Service Operations (Priority: P1) 🎯 MVP

**Goal**: Provide fundamental data operations so that the application has a working backend for basic functionality

**Independent Test**: Can be fully tested by verifying that the core-service starts successfully, responds to health checks, and provides basic CRUD operations for Company, Station, and Charger entities

### Tests for User Story 1

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T012 [P] [US1] Unit test for Company model in services/core-service/tests/unit/models/company_test.rs
- [ ] T013 [P] [US1] Unit test for CompanyRepository in services/core-service/tests/unit/repositories/company_repository_test.rs
- [ ] T014 [P] [US1] Integration test for health endpoint in services/core-service/tests/integration/health_test.rs
- [ ] T015 [P] [US1] Integration test for Company CRUD operations in services/core-service/tests/integration/company_api_test.rs

### Implementation for User Story 1

- [ ] T016 [P] [US1] Create Company model in services/core-service/src/models/company.rs
- [ ] T017 [P] [US1] Create Station model in services/core-service/src/models/station.rs
- [ ] T018 [P] [US1] Create Charger model in services/core-service/src/models/charger.rs
- [ ] T019 [US1] Implement CompanyRepository in services/core-service/src/repositories/company_repository.rs (depends on T016)
- [ ] T020 [US1] Implement StationRepository in services/core-service/src/repositories/station_repository.rs (depends on T017)
- [ ] T021 [US1] Implement ChargerRepository in services/core-service/src/repositories/charger_repository.rs (depends on T018)
- [ ] T022 [US1] Implement CompanyService in services/core-service/src/services/company_service.rs (depends on T019)
- [ ] T023 [US1] Implement StationService in services/core-service/src/services/station_service.rs (depends on T020)
- [ ] T024 [US1] Implement ChargerService in services/core-service/src/services/charger_service.rs (depends on T021)
- [ ] T025 [US1] Implement Company handlers in services/core-service/src/handlers/company_handler.rs (depends on T022)
- [ ] T026 [US1] Implement Station handlers in services/core-service/src/handlers/station_handler.rs (depends on T023)
- [ ] T027 [US1] Implement Charger handlers in services/core-service/src/handlers/charger_handler.rs (depends on T024)
- [ ] T028 [US1] Add Company routes to services/core-service/src/main.rs (depends on T025)
- [ ] T029 [US1] Add Station routes to services/core-service/src/main.rs (depends on T026)
- [ ] T030 [US1] Add Charger routes to services/core-service/src/main.rs (depends on T027)
- [ ] T031 [US1] Implement health check endpoint in services/core-service/src/handlers/health_handler.rs
- [ ] T032 [US1] Implement metrics endpoint in services/core-service/src/handlers/metrics_handler.rs
- [ ] T033 [US1] Add validation and error handling for all entities in services/core-service/src/utils/validation.rs
- [ ] T034 [US1] Add optimistic concurrency control for all entities in services/core-service/src/utils/concurrency.rs
- [ ] T034a [US1] Implement soft-delete queries in repositories in services/core-service/src/repositories/
- [ ] T034b [US1] Add soft-delete tests in services/core-service/tests/integration/soft_delete_test.rs

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Service Integration (Priority: P2)

**Goal**: Properly integrate with other services (auth, geo, analytics) so that the complete system can function as a unified application

**Independent Test**: Can be fully tested by verifying that the core-service can communicate with the authentication service for user validation and with the database for data persistence

### Tests for User Story 2

- [ ] T035 [P] [US2] Integration test for JWT validation in services/core-service/tests/integration/auth_test.rs
- [ ] T036 [P] [US2] Integration test for database connection resilience in services/core-service/tests/integration/database_resilience_test.rs
- [ ] T037 [P] [US2] E2E test for full request flow with auth in services/core-service/tests/e2e/auth_flow_test.rs

### Implementation for User Story 2

- [ ] T038 [P] [US2] Create User model in services/core-service/src/models/user.rs
- [ ] T039 [US2] Implement UserRepository in services/core-service/src/repositories/user_repository.rs (depends on T038)
- [ ] T040 [US2] Implement UserService in services/core-service/src/services/user_service.rs (depends on T039)
- [ ] T041 [US2] Complete JWT validation middleware with Keycloak integration in services/core-service/src/middleware/auth.rs (depends on T040)
- [ ] T042 [US2] Implement role-based access control in services/core-service/src/middleware/rbac.rs
- [ ] T043 [US2] Add graceful database failure handling in services/core-service/src/utils/database.rs
- [ ] T043a [US2] Implement circuit breaker pattern for database failures in services/core-service/src/utils/circuit_breaker.rs
- [ ] T044 [US2] Implement circuit breaker pattern for external dependencies in services/core-service/src/utils/circuit_breaker.rs
- [ ] T045 [US2] Add correlation ID propagation in services/core-service/src/middleware/correlation.rs
- [ ] T046 [US2] Integrate User handlers with auth middleware in services/core-service/src/handlers/user_handler.rs

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - API Documentation (Priority: P3)

**Goal**: Comprehensive API documentation for the core-service so that developers can understand how to integrate with it and use its capabilities effectively

**Independent Test**: Can be fully tested by verifying that the OpenAPI specification is accessible and accurately describes all available endpoints and their behavior

### Tests for User Story 3

- [ ] T047 [P] [US3] Integration test for OpenAPI endpoint in services/core-service/tests/integration/openapi_test.rs
- [ ] T048 [P] [US3] Validation test for OpenAPI specification completeness in services/core-service/tests/integration/openapi_validation_test.rs

### Implementation for User Story 3

- [ ] T049 [P] [US3] Add utoipa attributes to all models in services/core-service/src/models/
- [ ] T050 [P] [US3] Add utoipa attributes to all handlers in services/core-service/src/handlers/
- [ ] T051 [US3] Configure OpenAPI generation in services/core-service/src/lib.rs
- [ ] T052 [US3] Add OpenAPI JSON endpoint in services/core-service/src/handlers/openapi_handler.rs
- [ ] T053 [US3] Add Swagger UI endpoint in services/core-service/src/handlers/swagger_handler.rs
- [ ] T054 [US3] Add API versioning support in services/core-service/src/utils/versioning.rs
- [ ] T055 [US3] Add rate limiting middleware in services/core-service/src/middleware/rate_limit.rs

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T056 [P] Create audit log model in services/core-service/src/models/audit_log.rs
- [ ] T057 [P] Create outbox event model in services/core-service/src/models/outbox.rs (duplicate - remove if T011b complete)
- [ ] T058 Implement event publisher with lapin in services/core-service/src/events/publisher.rs
- [ ] T058a Implement outbox relay worker in services/core-service/src/events/relay.rs
- [ ] T059 Implement audit log middleware in services/core-service/src/middleware/audit.rs
- [ ] T060 [P] Add soft-delete functionality to repositories in services/core-service/src/repositories/
- [ ] T061a Add cascade soft-delete behavior for infrastructure entities in services/core-service/src/services/
- [ ] T062 [P] Add DTOs for API requests/responses in services/core-service/src/dto/
- [ ] T063 Implement Favorite model and handlers in services/core-service/src/models/favorite.rs and services/core-service/src/handlers/favorite_handler.rs
- [ ] T064 Implement Review model and handlers in services/core-service/src/models/review.rs and services/core-service/src/handlers/review_handler.rs
- [ ] T065 Add comprehensive unit tests for all services in services/core-service/tests/unit/services/
- [ ] T066 Add transaction tests for business-mutation + outbox writes in services/core-service/tests/integration/transaction_test.rs
- [ ] T067 Add outbox tests for relay-worker delivery in services/core-service/tests/integration/outbox_test.rs
- [ ] T068 Add audit-log tests in services/core-service/tests/integration/audit_test.rs
- [ ] T069 Add soft-delete tests in services/core-service/tests/integration/soft_delete_test.rs
- [ ] T073a Add database resilience integration test in services/core-service/tests/integration/database_resilience_test.rs
- [ ] T070 Create Dockerfile for core-service in services/core-service/Dockerfile
- [ ] T071 Create .dockerignore for core-service in services/core-service/.dockerignore
- [ ] T072 Update docker-compose.yml to include core-service
- [ ] T073 Run quickstart.md validation

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Phase 6)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - May integrate with US1 but should be independently testable
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - May integrate with US1/US2 but should be independently testable

### Within Each User Story

- Tests MUST be written and FAIL before implementation
- Models before services
- Services before handlers
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- All tests for a user story marked [P] can run in parallel
- Models within a story marked [P] can run in parallel
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Launch all tests for User Story 1 together:
Task: "Unit test for Company model in services/core-service/tests/unit/models/company_test.rs"
Task: "Unit test for CompanyRepository in services/core-service/tests/unit/repositories/company_repository_test.rs"
Task: "Integration test for health endpoint in services/core-service/tests/integration/health_test.rs"
Task: "Integration test for Company CRUD operations in services/core-service/tests/integration/company_api_test.rs"

# Launch all models for User Story 1 together:
Task: "Create Company model in services/core-service/src/models/company.rs"
Task: "Create Station model in services/core-service/src/models/station.rs"
Task: "Create Charger model in services/core-service/src/models/charger.rs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test User Story 1 independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 → Test independently → Deploy/Demo
4. Add User Story 3 → Test independently → Deploy/Demo
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1
   - Developer B: User Story 2
   - Developer C: User Story 3
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