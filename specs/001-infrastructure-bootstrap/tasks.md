# Tasks: Infrastructure Bootstrap

**Input**: Design documents from `/specs/001-infrastructure-bootstrap/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md

**Tests**: No test tasks — infrastructure sprint uses healthcheck verification.

**Organization**: Tasks are grouped by user story to enable independent verification.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and monorepo structure

- [x] T001 Create monorepo layout with `/source`, `/infra`, `/docs`, `/scripts` directories
- [x] T002 Create `/source/services/` and `/source/front/` placeholder directories
- [x] T003 Create `.env` file template in `/infra/.env.example` with database credentials
- [x] T004 Create `.gitignore` at repo root (Rust, Node, Docker, .env patterns)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Docker network and base infrastructure

- [x] T005 Create Docker Compose file in `/infra/docker-compose.yml` with PostgreSQL services
- [x] T006 Create internal Docker network (`borne-net`) definition in compose file
- [x] T007 Configure platform_db service (postgis/postgis:16-3.4, port 5432, volume)

**Checkpoint**: Docker network and database services defined — user stories can begin

---

## Phase 3: User Story 1 — Developer can run the full system locally (Priority: P1) 🎯 MVP

**Goal**: Single-command infrastructure startup with all required services

**Independent Test**: Run `docker compose up -d` and confirm all 3 containers (platform_db, analytics_db, Keycloak) are running via `docker ps`

- [x] T008 [US1] Add analytics_db service (postgres:16, port 5433, volume) in compose file
- [x] T009 [US1] Add Keycloak service (quay.io/keycloak/keycloak:24.0, port 8083) in compose file
- [x] T010 [US1] Configure environment variables for all services in compose file
- [x] T011 [US1] Create `docker compose down` cleanup with volume removal option, verify completes in < 10s
- [x] T012 [US1] Create startup/shutdown scripts in `/scripts/start.sh` and `/scripts/stop.sh`
- [x] T013 [US1] Add prerequisite detection (Docker check, port availability) in startup script

**Checkpoint**: At this point, `scripts/start.sh` boots all infrastructure and `scripts/stop.sh` cleans up

---

## Phase 4: User Story 2 — Developer can verify system health (Priority: P1)

**Goal**: Healthcheck endpoints for all infrastructure components

**Independent Test**: Run verification commands and confirm each service responds with success within 2 seconds

- [x] T014 [P] [US2] Add Docker healthcheck for platform_db in compose file (`pg_isready`)
- [x] T015 [P] [US2] Add Docker healthcheck for analytics_db in compose file (`pg_isready`)
- [x] T016 [P] [US2] Add Docker healthcheck for Keycloak in compose file (port check)
- [x] T017 [P] [US2] Create verification script in `/scripts/healthcheck.sh` testing all services with timeout checks and startup timing assertion (< 60s)
- [x] T018 [US2] Create error state output for each service when unhealthy

**Checkpoint**: All services expose health status — developers can diagnose startup failures

---

## Phase 5: User Story 3 — Developer can run spatial queries (Priority: P1)

**Goal**: PostGIS enabled and seed data loaded in platform_db

**Independent Test**: Connect to platform_db and run `SELECT PostGIS_Version();` and a distance query

- [x] T019 [US3] Enable PostGIS extension and run `SELECT PostGIS_Version();` verification query in platform_db init script
- [x] T020 [US3] Create SQL init script for inventory schema in `/infra/db/init/001_schema.sql`
- [x] T021 [US3] Add partner table (id, name, type, verification fields)
- [x] T022 [US3] Add station table (id, partner_id, name, address, lat/lng)
- [x] T023 [US3] Add charger table (id, station_id, connector_type, power_kw, status)
- [x] T024 [P] [US3] Add reference tables (connector_type, charger_status)
- [x] T025 [US3] Add GiST spatial index on station lat/lng
- [x] T026 [US3] Create seed data script in `/infra/db/seed/001_partners.sql`
- [x] T027 [US3] Create seed data script in `/infra/db/seed/002_stations.sql` (10 stations, Tunis)
- [x] T028 [US3] Create seed data script in `/infra/db/seed/003_chargers.sql` (30 chargers)
- [x] T029 [US3] Mount init and seed scripts as Docker volumes in compose file

**Checkpoint**: PostGIS queries work, seed data is loaded, spatial search is ready for Sprint 1.2

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, troubleshooting, and validation

- [x] T030 Create `README.md` with project overview and setup instructions
- [x] T031 Document troubleshooting guide for common startup issues
- [x] T032 Create `scripts/reset.sh` for full cleanup (volumes, rebuild)
- [x] T033 Update `AGENTS.md` with plan reference for active sprint
- [x] T034 Run quickstart.md validation — fresh clone to running system

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup
- **User Stories (Phase 3-5)**: All depend on Foundational, sequential within themselves
  - US1 (Docker Compose) must complete before US2 (healthchecks) and US3 (PostGIS)
  - US2 (healthchecks) and US3 (PostGIS) can proceed in parallel after US1
- **Polish (Phase 6)**: Depends on all user stories

### User Story Dependencies

- **US1 (P1)**: Foundational complete — Docker Compose structure ready
- **US2 (P1)**: US1 complete — containers must exist to add healthchecks
- **US3 (P1)**: US1 complete — platform_db container must exist for init scripts

### Within Each User Story

- Docker Compose structure before per-service config
- Core service definition before healthchecks
- Schema before seed data

### Parallel Opportunities

- T014, T015, T016 (healthchecks) can run in parallel
- T024 (reference tables) can run in parallel with main schema tables
- T026, T027, T028 (seed scripts) can run in parallel
- US2 and US3 can proceed in parallel after US1 completes

---

## Parallel Example: User Story 3

```bash
# Launch all PostGIS and schema tasks together:
Task: "Enable PostGIS extension in platform_db init script"
Task: "Add reference tables (connector_type, charger_status)"

# Launch all seed scripts together:
Task: "Create seed data for partners"
Task: "Create seed data for stations"
Task: "Create seed data for chargers"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (Docker Compose with all containers)
4. **STOP and VALIDATE**: Run `scripts/start.sh` and verify all 3 containers up
5. Proceed to US2 + US3

### Incremental Delivery

1. Setup + Foundational → directory structure ready
2. Add US1 → single-command startup works (MVP reached)
3. Add US2 + US3 → healthchecks and PostGIS ready
4. Polish → documentation complete
5. Each increment adds value without breaking previous work

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently verifiable
- All infrastructure config lives in `/infra/`, never in `/source/`
- Commit after each phase or logical group
