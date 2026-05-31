---

description: "Task list for Infrastructure Foundation (MVP Runtime Core)"

---

# Tasks: Infrastructure Foundation (MVP Runtime Core)

**Input**: Design documents from `specs/001-infrastructure-foundation/`

**Prerequisites**: plan.md (required), spec.md (required for user stories),
research.md, data-model.md, contracts/

**Tests**: Not requested in specification — no test tasks generated.

**Organization**: Tasks are grouped by user story to enable independent
implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Infrastructure project**: Root-level config files, `init/` for DB
  initialization scripts, `scripts/` for utilities
- All paths are relative to repository root

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create `.gitignore` at repository root excluding `.env`,
      `*.log`, and Docker build artifacts
- [ ] T002 [P] Create `init/postgis/` directory for PostgreSQL init scripts
- [ ] T003 [P] Create `scripts/` directory for health check utilities

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Docker network and volumes that all services depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 Define Docker Compose top-level config (version, name) in
      `docker-compose.yml`
- [ ] T005 Create `bornemap-net` bridge network in `docker-compose.yml`
- [ ] T006 [P] Define named volumes (`pg_data`, `mongo_data`,
      `rabbitmq_data`, `keycloak_data`) in `docker-compose.yml`

**Checkpoint**: Foundation ready — user story implementation can now begin

---

## Phase 3: User Story 1 - Boot Full Infrastructure Stack (Priority: P1) 🎯 MVP

**Goal**: All 5 core services start successfully via `docker compose up`
with zero manual setup

**Independent Test**: Run `docker compose up -d && docker compose ps` —
all services show "Up" status and pass health checks within 60 seconds

### Implementation for User Story 1

- [ ] T007 [P] [US1] Define PostgreSQL + PostGIS service with
      `postgis/postgis:16-3.4` image, environment variables, volume mount,
      and health check in `docker-compose.yml`
- [ ] T008 [P] [US1] Create PostgreSQL init SQL at `init/postgis/init.sql`
      enabling postgis and uuid-ossp extensions
- [ ] T009 [P] [US1] Define MongoDB service with `mongo:7` image,
      `MONGO_INITDB_DATABASE=clickstream`, volume mount, and health check
      in `docker-compose.yml`
- [ ] T010 [P] [US1] Define RabbitMQ service with `rabbitmq:4-management`
      image, AMQP (5672) and management UI (15672) ports, volume mount,
      and health check in `docker-compose.yml`
- [ ] T011 [P] [US1] Define Keycloak service with
      `quay.io/keycloak/keycloak:25` image, PostgreSQL backend config,
      volume mount, and health check in `docker-compose.yml`
- [ ] T012 [P] [US1] Create Keycloak realm JSON at
      `init/keycloak/realm-export.json` with `bornemap` realm and three
      roles: `registered_driver`, `partner`, `admin`
- [ ] T013 [US1] Mount Keycloak realm import file in `docker-compose.yml`
      and configure `KC_DB_URL`, `KC_DB_USERNAME`, `KC_DB_PASSWORD` to
      point at the PostgreSQL service
- [ ] T014 [P] [US1] Define Traefik service with `traefik:v3.1` image,
      HTTP entrypoint on port 80, Docker provider, dashboard enabled
      in `docker-compose.yml`
- [ ] T015 [US1] Add Traefik routing labels to the Keycloak service for
      `/auth/*` → keycloak:8080 route in `docker-compose.yml`
- [ ] T016 [US1] Wire all services to `bornemap-net` network and attach
      volumes in `docker-compose.yml`

**Checkpoint**: User Story 1 is functional — `docker compose up` boots
all 5 services with Traefik routing to Keycloak at `/auth`

---

## Phase 4: User Story 2 - Verify Service Connectivity (Priority: P2)

**Goal**: Each service is reachable and responsive from within the Docker
network

**Independent Test**: From a temporary container on `bornemap-net`, connect
to each service endpoint and verify a successful response

### Implementation for User Story 2

- [ ] T017 [US2] Add Docker Compose health check configurations to all
      5 services with appropriate intervals and retries in
      `docker-compose.yml` (postgres: `pg_isready`, mongo: `ping`,
      rabbitmq: `check_running`, keycloak: `curl /health`,
      traefik: dashboard reachable)
- [ ] T018 [US2] Create connectivity verification script at
      `scripts/verify-connectivity.sh` that tests each service endpoint
      from within the Docker network
- [ ] T019 [US2] Add `depends_on` with `condition: service_healthy`
      between Keycloak and PostgreSQL to ensure startup ordering in
      `docker-compose.yml`

**Checkpoint**: User Stories 1 AND 2 complete — all services boot and
are reachable; connectivity can be verified via script

---

## Phase 5: User Story 3 - Configure Stack via Environment (Priority: P3)

**Goal**: All service parameters configurable through `.env` file for
portability between local and CI environments

**Independent Test**: Modify a `.env` value, restart stack, verify the
corresponding service uses the new value

### Implementation for User Story 3

- [ ] T020 [US3] Create `.env.example` at repository root with all
      configurable environment variables and their default values
- [ ] T021 [US3] Replace hardcoded values in `docker-compose.yml` with
      `${VAR_NAME}` references for all configurable variables
- [ ] T022 [US3] Add `env_file: .env` directive to each service in
      `docker-compose.yml`
- [ ] T023 [US3] Add validation note to `docker-compose.yml` comments
      referencing required environment variables

**Checkpoint**: All user stories complete — ports, passwords, and DB names
are configurable via `.env` without editing compose files

---

## Phase N: Polish & Cross-Cutting Concerns

**Purpose**: Final verification and documentation

- [ ] T024 Run `docker compose up` end-to-end and verify all services
      pass health checks
- [ ] T025 [P] Verify `.gitignore` correctly excludes `.env` from
      version control
- [ ] T026 Run full restart test (`docker compose down && docker compose
      up -d`) and verify volume persistence
- [ ] T027 Validate all services produce structured JSON logs via
      `docker compose logs`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all
  user stories
- **User Stories (Phase 3-5)**: All depend on Foundational phase completion
  - US1 (Phase 3) can start immediately after Foundational
  - US2 (Phase 4) has no dependency on US1
  - US3 (Phase 5) has no dependency on US1 or US2
- **Polish (Final Phase)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: No dependencies on other stories
- **User Story 2 (P2)**: No dependencies on other stories (health checks
  can be defined independently)
- **User Story 3 (P3)**: No dependencies on other stories (env
  substitution is independent of specific services)

### Within Each User Story

- Independent service definitions can be parallelized
- Core config before integration (network wiring, volume mounting)
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks are [P] — T002 and T003 can run in parallel
- T007-T012, T014 are [P] — each service definition is an independent file
  section in `docker-compose.yml`
- US1 and US2 can be worked on in parallel after Foundational phase
- US3 can begin independently after Foundational phase

---

## Parallel Example: User Story 1

```bash
# Launch all independent service definitions together:
Task: "T007 [P] [US1] Define PostgreSQL service in docker-compose.yml"
Task: "T009 [P] [US1] Define MongoDB service in docker-compose.yml"
Task: "T010 [P] [US1] Define RabbitMQ service in docker-compose.yml"
Task: "T011 [P] [US1] Define Keycloak service in docker-compose.yml"
Task: "T014 [P] [US1] Define Traefik service in docker-compose.yml"

# Then wire them together:
Task: "T015 [US1] Add Traefik routing labels to Keycloak"
Task: "T016 [US1] Wire all services to bornemap-net network"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Run `docker compose up` — all 5 services must
   boot and pass health checks
5. Demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Boot stack → Deploy/Demo (MVP!)
3. Add User Story 2 → Connectivity verification → Deploy/Demo
4. Add User Story 3 → Env config → Deploy/Demo
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (all service definitions)
   - Developer B: User Story 2 (health checks, connectivity)
   - Developer C: User Story 3 (.env.example, env substitution)
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different sections in `docker-compose.yml`, no file conflicts
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group (docker-compose.yml can be
  incrementally committed)
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that
  break independence
