---
description: "Task list for Sprint 0: System Bootstrap & Enforcement Kernel"
---

# Tasks: System Bootstrap & Enforcement Kernel

**Input**: Design documents from `/specs/001-system-bootstrap/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Tests are NOT explicitly requested in Sprint 0. Sprint 0 focuses on infrastructure setup and enforcement kernel.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Monorepo**: `apps/packages/`, `services/`, `tools/`, `infrastructure/` at repository root
- Paths shown below assume monorepo structure

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Monorepo initialization and basic structure

- [X] T001 Create workspace Cargo.toml with 6 crates (ui-kit, domain-types, client-core, auth-service, driver-service, admin-service)
- [X] T002 [P] Create apps/packages/ui-kit/Cargo.toml with UI-only dependencies
- [X] T003 [P] Create apps/packages/domain-types/Cargo.toml with contracts-only dependencies
- [X] T004 [P] Create apps/packages/client-core/Cargo.toml with transport-only dependencies
- [X] T005 [P] Create services/auth-service/Cargo.toml with sqlx, actix-web, serde dependencies
- [X] T006 [P] Create services/driver-service/Cargo.toml with sqlx, actix-web, serde, geospatial dependencies
- [X] T007 [P] Create services/admin-service/Cargo.toml with sqlx, actix-web, serde dependencies
- [X] T008 [P] Create .cargo/config.toml with workspace configuration

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T009 Create .specify/memory/constitution.md (v1.0.0 from docs/constitution/constitution.md)
- [X] T010 Create .specify/extensions.yml with SpecKit hooks configuration
- [X] T011 [P] Create .specify/templates/plan-template.md
- [X] T012 [P] Create .specify/templates/spec-template.md
- [X] T013 [P] Create .specify/templates/tasks-template.md
- [X] T014 [P] Create tools/ci_guard.sh with 9-stage CI enforcement
- [X] T015 [P] Create tools/01_validate_identity.sh for UUID vs nanoid validation
- [X] T016 [P] Create tools/02_validate_deps.sh for dependency graph validation
- [X] T017 [P] Create tools/03_validate_analytics_gate.sh for analytics write permission validation
- [X] T018 [P] Create tools/04_validate_schema.sh for database schema validation
- [X] T019 [P] Create tools/05_sqlx_policy_check.sh for SQLx compile-time policy validation
- [X] T020 Create tools/06_ci_guard_final.sh as final CI gate runner
- [X] T021 [P] Create .github/workflows/ci.yml with 9-stage CI pipeline (hard-stop on failure)
- [X] T022 [P] Create docs/constitution/speckit_enforcement.md
- [X] T023 [P] Create infrastructure/docker-compose/local.yml with 3 services + PostgreSQL + Redis
- [X] T024 [P] Create infrastructure/traefik/traefik.toml for reverse proxy configuration

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Monorepo Initialization (Priority: P1) 🎯 MVP

**Goal**: Developers clone the repository and see a complete project structure with all necessary directories, packages, and configuration files in place

**Independent Test**: Navigate to the repository root, verify the directory structure matches the specification, and confirm all expected files exist

### Implementation for User Story 1

- [ ] T025 [P] [US1] Create apps/packages/ui-kit/src/components/ directory
- [ ] T026 [P] [US1] Create apps/packages/ui-kit/src/layouts/ directory
- [ ] T027 [P] [US1] Create apps/packages/ui-kit/src/tokens/ directory
- [ ] T028 [P] [US1] Create apps/packages/ui-kit/src/accessibility/ directory
- [ ] T029 [P] [US1] Create apps/packages/ui-kit/tests/ directory
- [ ] T030 [P] [US1] Create apps/packages/domain-types/src/dto/ directory
- [ ] T031 [P] [US1] Create apps/packages/domain-types/src/events/ directory
- [ ] T032 [P] [US1] Create apps/packages/domain-types/src/ids/ directory
- [ ] T033 [P] [US1] Create apps/packages/domain-types/tests/ directory
- [ ] T034 [P] [US1] Create apps/packages/client-core/src/api/ directory
- [ ] T035 [P] [US1] Create apps/packages/client-core/src/auth/ directory
- [ ] T036 [P] [US1] Create apps/packages/client-core/src/mappers/ directory
- [ ] T037 [P] [US1] Create apps/packages/client-core/tests/ directory
- [ ] T038 [P] [US1] Create services/auth-service/src/models/ directory
- [ ] T039 [P] [US1] Create services/auth-service/src/services/ directory
- [ ] T040 [P] [US1] Create services/auth-service/src/api/ directory
- [ ] T041 [P] [US1] Create services/auth-service/src/db/ directory
- [ ] T042 [P] [US1] Create services/auth-service/tests/ directory
- [ ] T043 [P] [US1] Create services/auth-service/migrations/ directory
- [ ] T044 [P] [US1] Create services/driver-service/src/models/ directory
- [ ] T045 [P] [US1] Create services/driver-service/src/services/ directory
- [ ] T046 [P] [US1] Create services/driver-service/src/api/ directory
- [ ] T047 [P] [US1] Create services/driver-service/src/db/ directory
- [ ] T048 [P] [US1] Create services/driver-service/src/telemetry/ directory
- [ ] T049 [P] [US1] Create services/driver-service/tests/ directory
- [ ] T050 [P] [US1] Create services/driver-service/migrations/ directory
- [ ] T051 [P] [US1] Create services/admin-service/src/models/ directory
- [ ] T052 [P] [US1] Create services/admin-service/src/services/ directory
- [ ] T053 [P] [US1] Create services/admin-service/src/api/ directory
- [ ] T054 [P] [US1] Create services/admin-service/src/db/ directory
- [ ] T055 [P] [US1] Create services/admin-service/tests/ directory
- [ ] T056 [P] [US1] Create services/admin-service/migrations/ directory
- [ ] T057 [P] [US1] Create tools/scripts/ directory
- [ ] T058 [P] [US1] Create infrastructure/scripts/ directory
- [ ] T059 [P] [US1] Create docs/sprints/sprint_00/backlog/ directory
- [ ] T060 [P] [US1] Create docs/sprints/sprint_00/review/ directory
- [ ] T061 [P] [US1] Create docs/spec/ directory

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - CI Enforcement Pipeline (Priority: P1)

**Goal**: The CI pipeline executes 9 mandatory stages with hard-stop on any failure

**Independent Test**: Run `make ci` and verify all 9 stages pass without any failures

### Implementation for User Story 2

- [ ] T062 [P] [US2] Create tools/format_check.sh in tools/ci_guard.sh stage 1
- [ ] T063 [P] [US2] Create tools/type_check.sh in tools/ci_guard.sh stage 2
- [ ] T064 [P] [US2] Create tools/dependency_graph_validation.sh in tools/ci_guard.sh stage 3
- [ ] T065 [P] [US2] Create tools/identity_validation.sh in tools/ci_guard.sh stage 4
- [ ] T066 [P] [US2] Create tools/schema_validation.sh in tools/ci_guard.sh stage 5
- [ ] T067 [P] [US2] Create tools/sqlx_compile_check.sh in tools/ci_guard.sh stage 6
- [ ] T068 [P] [US2] Create tools/analytics_write_gate.sh in tools/ci_guard.sh stage 7
- [ ] T069 [P] [US2] Create tools/integration_tests.sh in tools/ci_guard.sh stage 8
- [ ] T070 [P] [US2] Create tools/build_success.sh in tools/ci_guard.sh stage 9
- [ ] T071 [US2] Integrate all 9 stages into tools/ci_guard.sh with hard-stop enforcement
- [ ] T072 [US2] Create Makefile with ci target that runs tools/ci_guard.sh
- [ ] T073 [US2] Create .github/workflows/ci.yml with all 9 CI stages

**Checkpoint**: At this point, User Story 2 should be fully functional and testable independently

---

## Phase 5: User Story 3 - Database Schemas Bootstrapped (Priority: P1)

**Goal**: All three databases (platform_db, analytics_db, keycloak_db) are initialized with proper schema definitions

**Independent Test**: Connect to each database and verify all tables and constraints exist

### Implementation for User Story 3

#### auth-service Migrations

- [ ] T074 [P] [US3] Create services/auth-service/migrations/0001_init_users.up.sql
- [ ] T075 [P] [US3] Create services/auth-service/migrations/0001_init_users.down.sql (if needed)

#### driver-service Migrations

- [ ] T076 [P] [US3] Create services/driver-service/migrations/0001_init_gis.up.sql
- [ ] T077 [P] [US3] Create services/driver-service/migrations/0001_init_gis.down.sql (if needed)
- [ ] T078 [P] [US3] Create services/driver-service/migrations/0002_init_analytics.up.sql
- [ ] T079 [P] [US3] Create services/driver-service/migrations/0002_init_analytics.down.sql (if needed)
- [ ] T080 [P] [US3] Create services/driver-service/migrations/0003_create_analytics_indexes.up.sql
- [ ] T081 [P] [US3] Create services/driver-service/migrations/0003_create_analytics_indexes.down.sql (if needed)

#### admin-service Migrations

- [ ] T082 [P] [US3] Create services/admin-service/migrations/0001_init_inventory.up.sql
- [ ] T083 [P] [US3] Create services/admin-service/migrations/0001_init_inventory.down.sql (if needed)

#### Database Verification

- [ ] T084 [US3] Create infrastructure/scripts/provision_db.sh for database initialization
- [ ] T085 [US3] Create tools/04_validate_schema.sh for database schema validation

**Checkpoint**: At this point, User Story 3 should be fully functional and testable independently

---

## Phase 6: User Story 4 - Service Skeletons Created (Priority: P1)

**Goal**: Three microservices (auth-service, driver-service, admin-service) are created with basic structure

**Independent Test**: Start each service and verify health endpoints respond correctly

### Implementation for User Story 4

#### auth-service Skeleton

- [ ] T086 [P] [US4] Create services/auth-service/Cargo.toml with dependencies
- [ ] T087 [P] [US4] Create services/auth-service/src/main.rs with health endpoint
- [ ] T088 [P] [US4] Create services/auth-service/config.toml for configuration
- [ ] T089 [P] [US4] Create services/auth-service/src/lib.rs for shared library

#### driver-service Skeleton

- [ ] T090 [P] [US4] Create services/driver-service/Cargo.toml with dependencies
- [ ] T091 [P] [US4] Create services/driver-service/src/main.rs with health endpoint
- [ ] T092 [P] [US4] Create services/driver-service/config.toml for configuration
- [ ] T093 [P] [US4] Create services/driver-service/src/lib.rs for shared library

#### admin-service Skeleton

- [ ] T094 [P] [US4] Create services/admin-service/Cargo.toml with dependencies
- [ ] T095 [P] [US4] Create services/admin-service/src/main.rs with health endpoint
- [ ] T096 [P] [US4] Create services/admin-service/config.toml for configuration
- [ ] T097 [P] [US4] Create services/admin-service/src/lib.rs for shared library

#### Service Verification

- [ ] T098 [US4] Create tools/sqlx_prepare.sh to generate offline data for all services
- [ ] T099 [US4] Verify health endpoints respond on ports 3000, 3001, 3002

**Checkpoint**: At this point, User Story 4 should be fully functional and testable independently

---

## Phase 7: User Story 5 - SpecKit Compliance (Priority: P1)

**Goal**: All documentation follows SpecKit standards with proper enforcement layers

**Independent Test**: Verify all SpecKit markers are present in documentation and no violations exist

### Implementation for User Story 5

- [ ] T100 [P] [US5] Create infrastructure/scripts/deploy.sh for service deployment
- [ ] T101 [P] [US5] Create infrastructure/scripts/migrate.sh for schema migrations
- [ ] T102 [P] [US5] Create infrastructure/README.md for infrastructure documentation
- [ ] T103 [P] [US5] Create docs/SYSTEM_STATE.md with current system inventory and status
- [ ] T104 [P] [US5] Create docs/roadmap_status.md with sprint pipeline and milestones
- [ ] T105 [P] [US5] Create docs/sprints/sprint_00/review/sprint_00_review.md
- [ ] T106 [P] [US5] Create docs/sprints/sprint_00/sprint_state.json with machine-readable state
- [ ] T107 [P] [US5] Create docs/sprints/sprint_00/validation_report.md for compliance audit
- [ ] T108 [P] [US5] Create docs/sprints/sprint_00/follow_up.md with action items

**Checkpoint**: At this point, User Story 5 should be fully functional and testable independently

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T109 [P] Optional: Create infrastructure/redis/redis.conf for Redis configuration
- [ ] T110 [P] Optional: Create infrastructure/scripts/setup_keycloak.sh for Keycloak setup
- [ ] T111 [P] Optional: Create infrastructure/realm-bornemap.json for Keycloak realm export
- [ ] T112 [P] Optional: Add .specify/extensions/git/git-config.yml for auto-commit hooks
- [ ] T113 [P] Verify all 5 user stories can be tested independently
- [ ] T114 [P] Run full CI pipeline (9 stages) and verify all pass

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-7)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P1 → P1 → P1 → P1)
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 3 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 4 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 5 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories

**All user stories are independently testable and can be implemented in parallel after Foundational phase completes.**

### Within Each User Story

- Parallelizable tasks (marked [P]) can be executed simultaneously
- Non-parallelizable tasks depend on previous tasks within the story
- Stories are fully independent of each other

### Parallel Opportunities

- All Setup tasks (Phase 1) marked [P] can run in parallel
- All Foundational tasks (Phase 2) marked [P] can run in parallel (within Phase 2)
- Within User Story 1: All [P] tasks can run in parallel (creating directories)
- Within User Story 2: All 9 stage tasks can run in parallel
- Within User Story 3: All migration creation tasks can run in parallel
- Within User Story 4: All service skeleton tasks can run in parallel
- Within User Story 5: All documentation creation tasks can run in parallel
- Once Foundational phase completes, ALL 5 user stories can start in parallel (if team capacity allows)

---

## Parallel Example: User Story 1 (Monorepo Initialization)

```bash
# Launch all directory creation tasks together:
Task: "Create apps/packages/ui-kit/src/components/ directory"
Task: "Create apps/packages/ui-kit/src/layouts/ directory"
Task: "Create apps/packages/ui-kit/src/tokens/ directory"
Task: "Create apps/packages/ui-kit/src/accessibility/ directory"
Task: "Create apps/packages/ui-kit/tests/ directory"
Task: "Create apps/packages/domain-types/src/dto/ directory"
Task: "Create apps/packages/domain-types/src/events/ directory"
Task: "Create apps/packages/domain-types/src/ids/ directory"
Task: "Create apps/packages/domain-types/tests/ directory"
Task: "Create apps/packages/client-core/src/api/ directory"
Task: "Create apps/packages/client-core/src/auth/ directory"
Task: "Create apps/packages/client-core/src/mappers/ directory"
Task: "Create apps/packages/client-core/tests/ directory"
Task: "Create services/auth-service/src/models/ directory"
Task: "Create services/auth-service/src/services/ directory"
Task: "Create services/auth-service/src/api/ directory"
Task: "Create services/auth-service/src/db/ directory"
Task: "Create services/auth-service/tests/ directory"
Task: "Create services/auth-service/migrations/ directory"
Task: "Create services/driver-service/src/models/ directory"
Task: "Create services/driver-service/src/services/ directory"
Task: "Create services/driver-service/src/api/ directory"
Task: "Create services/driver-service/src/db/ directory"
Task: "Create services/driver-service/src/telemetry/ directory"
Task: "Create services/driver-service/tests/ directory"
Task: "Create services/driver-service/migrations/ directory"
Task: "Create services/admin-service/src/models/ directory"
Task: "Create services/admin-service/src/services/ directory"
Task: "Create services/admin-service/src/api/ directory"
Task: "Create services/admin-service/src/db/ directory"
Task: "Create services/admin-service/tests/ directory"
Task: "Create services/admin-service/migrations/ directory"
Task: "Create tools/scripts/ directory"
Task: "Create infrastructure/scripts/ directory"
Task: "Create docs/sprints/sprint_00/backlog/ directory"
Task: "Create docs/sprints/sprint_00/review/ directory"
Task: "Create docs/spec/ directory"
```

---

## Parallel Example: User Story 3 (Database Schemas)

```bash
# Launch all migration creation tasks together:
Task: "Create services/auth-service/migrations/0001_init_users.up.sql"
Task: "Create services/auth-service/migrations/0001_init_users.down.sql"
Task: "Create services/driver-service/migrations/0001_init_gis.up.sql"
Task: "Create services/driver-service/migrations/0001_init_gis.down.sql"
Task: "Create services/driver-service/migrations/0002_init_analytics.up.sql"
Task: "Create services/driver-service/migrations/0002_init_analytics.down.sql"
Task: "Create services/driver-service/migrations/0003_create_analytics_indexes.up.sql"
Task: "Create services/driver-service/migrations/0003_create_analytics_indexes.down.sql"
Task: "Create services/admin-service/migrations/0001_init_inventory.up.sql"
Task: "Create services/admin-service/migrations/0001_init_inventory.down.sql"
```

---

## Implementation Strategy

### MVP First (User Stories 1, 2, 3, 4, 5 All in Parallel)

With Sprint 0 being foundational, all user stories should be implemented together to establish the complete project baseline:

1. Complete Phase 1: Setup (monorepo structure)
2. Complete Phase 2: Foundational (CI, tools, documentation)
3. Complete Phase 3: User Story 1 (directory structure) - happens during Setup
4. Complete Phase 4: User Story 2 (CI pipeline) - happens during Setup
5. Complete Phase 5: User Story 3 (database schemas) - foundational work
6. Complete Phase 6: User Story 4 (service skeletons) - foundational work
7. Complete Phase 7: User Story 5 (SpecKit compliance) - documentation work
8. **STOP and VALIDATE**: Test all 5 user stories independently
9. Review system state and roadmap status

### Sprint 0 Focus

Sprint 0 is uniquely focused on infrastructure and setup, making it appropriate to implement all user stories together since they don't depend on each other. This establishes the complete project foundation before feature development begins in Sprint 1.

### Incremental Delivery (Not Applicable for Sprint 0)

Sprint 0 is NOT about incremental feature delivery. It's about establishing the foundation for all subsequent sprints. All user stories must be completed to have a working baseline.

### Parallel Team Strategy

With multiple developers in Sprint 0:

1. Team completes Phase 1 (Setup) together
2. Team completes Phase 2 (Foundational) together
3. Once Foundational is done:
   - Developer A: User Story 3 (database schemas)
   - Developer B: User Story 4 (service skeletons)
   - Developer C: User Story 5 (documentation)
   - Remaining work: User Story 1 and 2 are integrated into Setup phase
4. Stories complete and integrate independently
5. All 5 user stories should be complete for Sprint 0 to be successful

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Sprint 0 is infrastructure-focused - all 5 user stories are foundational
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence

---

## Sprint 0 Task Summary

**Total Tasks**: 114 tasks

**Task Count per User Story**:
- User Story 1 (Monorepo Initialization): 37 tasks (Phase 3)
- User Story 2 (CI Enforcement Pipeline): 12 tasks (Phase 4)
- User Story 3 (Database Schemas Bootstrapped): 12 tasks (Phase 5)
- User Story 4 (Service Skeletons Created): 14 tasks (Phase 6)
- User Story 5 (SpecKit Compliance): 9 tasks (Phase 7)

**Parallel Opportunities**:
- Phase 1: 7 parallelizable tasks (T002-T008)
- Phase 2: 12 parallelizable tasks (T012-T020, T022-T024)
- Phase 3: 37 parallelizable tasks (all directory creation tasks)
- Phase 4: 10 parallelizable tasks (all 9 stage scripts + integration)
- Phase 5: 8 parallelizable tasks (all migration creation tasks)
- Phase 6: 12 parallelizable tasks (all service skeleton tasks)
- Phase 7: 8 parallelizable tasks (all documentation creation tasks)
- **Total Parallelizable Tasks**: 96 out of 114 (84%)

**Independent Test Criteria**:
- US1: Verify directory structure exists and matches spec
- US2: Run `make ci` and verify all 9 stages pass
- US3: Connect to databases and verify tables exist
- US4: Start services and verify health endpoints respond
- US5: Verify all SpecKit markers are present

**Suggested MVP Scope**:
- Sprint 0 is foundational - all 5 user stories must be completed
- MVP for Sprint 0 = complete baseline (no partial MVP as this is infrastructure)

**Format Validation**: ✅ All tasks follow checklist format with checkbox, ID, and file paths