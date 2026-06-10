---

description: "Task list for Docker Compose and CI/CD feature implementation"
---

# Tasks: Docker Compose and CI/CD

**Input**: Design documents from `specs/011-docker-compose-ci/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1+US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

- `docker-compose.yml` at repo root
- `.github/workflows/` at repo root
- `source/services/driver-service/Dockerfile` and `source/services/admin-service/Dockerfile`
- `source/apps/{dashboard,driver-web,driver-mobile}/` for frontend apps

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create configuration files and directory structure

- [ ] T001 Create `docker-compose.yml` at repo root with initial structure (version, services map)
- [ ] T002 Create `.github/workflows/` directory at repo root

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Ensure all build artifacts support Docker Compose health checks and container networking

- [ ] T003 [P] Add `curl` to `source/services/driver-service/Dockerfile` runtime apt-get install list
- [ ] T004 [P] Add `curl` to `source/services/admin-service/Dockerfile` runtime apt-get install list
- [ ] T005 Verify both Dockerfiles build successfully: `docker build -f source/services/driver-service/Dockerfile source/` and `docker build -f source/services/admin-service/Dockerfile source/`

**Checkpoint**: Foundation ready — Docker images include curl for health checks

---

## Phase 3: User Stories 1+2 — Docker Compose with Health Checks (Priority: P1) 🎯 MVP

**Goal**: Single `docker-compose up` command starts PostgreSQL, Driver Service, and Admin Service with proper startup ordering and health checks.

**Independent Test**:
1. Run `docker compose up --build -d postgres driver-service admin-service`
2. Verify all containers show "healthy" via `docker compose ps`
3. Verify health endpoints return 200: `curl http://localhost:8080/api/health`, `curl http://localhost:8081/api/health`
4. Verify database migrations ran by querying an API endpoint that reads DB

### Implementation

- [ ] T006 [US1+US2] Define `postgres` service in `docker-compose.yml` (image: `postgis/postgis:17-3.5`, container name: `borne-postgres`, port 5432, env vars: POSTGRES_USER/PASSWORD/DB, volume: pgdata)
- [ ] T007 [US1+US2] Define `driver-service` in `docker-compose.yml` (build context `source/`, Dockerfile `services/driver-service/Dockerfile`, port 8080, depends_on: postgres (healthy), env vars: DATABASE_URL, PORT, RUST_LOG)
- [ ] T008 [US1+US2] Define `admin-service` in `docker-compose.yml` (build context `source/`, Dockerfile `services/admin-service/Dockerfile`, port 8081, depends_on: postgres (healthy), env vars: DATABASE_URL, PORT, RUST_LOG)
- [ ] T009 Add health check configuration to all three services (`pg_isready -U postgres` for postgres, `curl -f http://localhost:808X/api/health` for Rust services) with interval/retries per contracts
- [ ] T010 [US1+US2] Configure named network `borne-network` (bridge driver) and named volume `pgdata` in `docker-compose.yml`
- [ ] T011 Validate `docker-compose.yml` syntax: `docker compose config`

**Checkpoint**: US1+US2 complete — `docker compose up` starts a healthy multi-service stack

---

## Phase 4: User Story 3 — CI/CD Pipelines (Priority: P2)

**Goal**: Pull requests to service branches trigger automated CI that builds, tests, and lints.

**Independent Test**:
1. Push a trivial change to `source/services/driver-service/` on a test branch
2. Verify `driver-service.yml` workflow runs (via GitHub Actions UI or `gh run list`)
3. Check workflow logs show successful `cargo build`, `cargo test`, `cargo clippy`
4. Repeat for admin-service changes

### Implementation

- [ ] T012 [P] [US3] Create `.github/workflows/driver-service.yml` (trigger: push/PR to branches, path: `source/services/driver-service/**` + `source/crates/**`, steps: checkout, Rust toolchain, cache, cargo build/test/clippy, optional Docker build)
- [ ] T013 [P] [US3] Create `.github/workflows/admin-service.yml` (same pattern as driver-service but scoped to `source/services/admin-service/**`)

**Checkpoint**: US3 complete — both CI pipelines auto-trigger on service-specific changes

---

## Phase 5: User Story 4 — Frontend API Configuration (Priority: P2)

**Goal**: Frontend apps (Dashboard, Driver Web, Driver Mobile) are configured with `API_BASE_URL` to communicate with correct backend services via Docker Compose.

**Independent Test**:
1. Run `docker compose up --build -d` (all services including frontends)
2. Verify frontend containers start without errors
3. Verify frontend containers can reach backend: `docker exec borne-dashboard curl -f http://driver-service:8080/api/health`

### Implementation

- [ ] T014 [P] [US4] Define `dashboard` service in `docker-compose.yml` (build: `source/apps/dashboard/`, port 5173, depends_on: driver-service+admin-service, env: API_BASE_URL=http://driver-service:8080)
- [ ] T015 [P] [US4] Define `driver-web` service in `docker-compose.yml` (build: `source/apps/driver-web/`, port 5174, depends_on: driver-service, env: API_BASE_URL=http://driver-service:8080)
- [ ] T016 [P] [US4] Define `driver-mobile` service in `docker-compose.yml` (build: `source/apps/driver-mobile/`, port 8081, depends_on: driver-service, env: API_BASE_URL=http://driver-service:8080)
- [ ] T017 [US4] Wire all frontend services to the `borne-network` network for inter-service DNS resolution

**Checkpoint**: US4 complete — frontend apps can discover and call backend services by container name

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Validation, documentation, and final verification

- [ ] T018 Run full validation: `docker compose config` + build all images + verify no warnings
- [ ] T019 Update `quickstart.md` with accurate commands and test steps
- [ ] T020 Run `cargo build --all` and `cargo clippy --all -- -D warnings` from workspace root to verify no regressions

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories (Dockerfiles need curl)
- **US1+US2 (Phase 3)**: Depends on Foundational — BLOCKS all other stories (no stack to deploy on)
- **US3 (Phase 4)**: Depends on Setup only (workflows are independent files) — can be parallel with Phase 3
- **US4 (Phase 5)**: Depends on Phase 3 (frontend services depend on backend services)
- **Polish (Phase 6)**: Depends on all prior phases

### User Story Dependencies

- **US1+US2 (P1)**: Can start after Foundational (Phase 2) — No dependencies on other stories
- **US3 (P2)**: Can start after Setup (Phase 1) — Independent of all other stories (pure GitHub config)
- **US4 (P2)**: Depends on US1+US2 completing (frontend needs backend containers to exist)

### Parallel Opportunities

- T003 and T004 (Dockerfile edits) can run in parallel
- T012 and T013 (workflow files) can run in parallel
- T014, T015, T016 (frontend services) can run in parallel
- Phase 3 (US1+US2) and Phase 4 (US3) can run in parallel (separate files: docker-compose.yml vs .github/workflows/*.yml)
- Phase 5 (US4) must wait for Phase 3 (US1+US2) since frontend services reference backend service names

---

## Parallel Example: Phase 3 (US1+US2)

```bash
# Launch health check and network config in parallel with service definitions:
Task: "Add health checks to postgres, driver-service, admin-service"
Task: "Configure borne-network and pgdata volume"
```

## Parallel Example: Phase 4 (US3)

```bash
# Create both workflow files in parallel:
Task: "Create driver-service.yml workflow"
Task: "Create admin-service.yml workflow"
```

---

## Implementation Strategy

### MVP First (US1+US2 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (Dockerfile curl)
3. Complete Phase 3: US1+US2 (docker-compose.yml with health checks)
4. **STOP and VALIDATE**: `docker compose up --build -d` + verify health endpoints
5. Demo ready — full backend stack deployable

### Incremental Delivery

1. Setup + Foundational → Dockerfiles ready for containerization
2. Add US1+US2 → Full backend stack in Docker → **MVP!**
3. Add US3 → CI/CD pipelines auto-test PRs
4. Add US4 → Frontend apps included in stack
5. Polish → Validation and docs finalized

### Parallel Team Strategy

With multiple developers:
1. Developer A: Phase 1 + Phase 2 (Setup + Dockerfile curl)
2. Developer B: Phase 4 (CI workflows) — starts immediately after Phase 1
3. Once Phase 2 is done:
   - Developer A: Phase 3 (docker-compose.yml)
   - Developer B: Phase 4 (continue if not done, or help with Phase 5)
4. Developer C: Phase 5 (frontend services) — starts after Phase 3
5. Polish: team validation together

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability (US1+US2, US3, US4)
- Each user story should be independently completable and testable
- No test tasks generated (spec does not request tests — verification is manual `docker compose` and `gh` commands)
- Commit after each phase or logical group
- Stop at any checkpoint to validate independently
