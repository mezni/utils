# Tasks: Infrastructure Bootstrap

**Input**: Design documents from `/specs/001-infra-bootstrap/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks grouped by user story for independent implementation and testing.

**Format**: `[ID] [P?] [Story] Description with file path`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project directory structure and environment configuration

- [ ] T001 Create `source/infra/` directory structure per plan.md layout
- [ ] T002 Create `source/infra/.env.example` with all documented environment variables (Postgres passwords, Keycloak creds, etc.)
- [ ] T003 Add `source/infra/.env` to `.gitignore`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: All configuration files and Docker Compose orchestration that must exist before any user story can be verified

- [ ] T004 Create `source/infra/docker-compose.yml` with Postgres+PostGIS, Redis, Keycloak, Traefik, and stub services, including named volumes, single `bornemap-net` bridge network, health checks, and dependency ordering
- [ ] T005 [P] Create Postgres init scripts in `source/infra/postgres/init/`: `01-create-dbs.sql` (platform_db, keycloak_db, analytics_db), `02-schemas-and-roles.sql` (gis, inventory, users schemas + 4 DB roles with schema-scoped grants), `03-initial-tables.sql` (partners, stations, chargers with ID prefix CHECK constraints)
- [ ] T006 [P] Create Keycloak realm export at `source/infra/keycloak/realm-export/bornemap-realm.json` with `bornemap` realm, 3 clients (mobile-driver-app, web-driver-app, admin-dashboard), 3 roles (role:admin, role:partner, role:driver), and protocol mappers
- [ ] T007 [P] Create Traefik configuration: `source/infra/traefik/traefik.yml` (static — entryPoints.web:80, file provider) and `source/infra/traefik/dynamic/routing.yml` (3 route rules + 404 catch-all)
- [ ] T008 [P] Create stub HTTP containers for auth-service, admin-service, and driver-service under `source/infra/stubs/` — each returning a distinct JSON response on any path

**Checkpoint**: Foundation ready — Docker Compose starts all 7 containers with healthy status. User story implementation can begin.

---

## Phase 3: User Story 1 — Start the full backend stack with one command (Priority: P1) 🎯 MVP

**Goal**: Developer can run a single command to start the entire infrastructure stack

**Independent Test**: Run `docker compose up -d` on a clean machine, verify all 4 infrastructure containers reach healthy state within 120 seconds

- [ ] T009 [US1] Create `source/infra/scripts/start.sh` — orchestrates `docker compose up -d` and waits for health
- [ ] T010 [US1] Create `source/infra/scripts/stop.sh` — clean shutdown via `docker compose down`; verify zero running containers with `docker compose ps`, then confirm clean recreate with `docker compose up -d`
- [ ] T011 [US1] Verify containers reuse on repeated start (run start.sh twice, confirm data persists via container reuse)

**Checkpoint**: US1 complete — developer can start/stop the stack and containers reach healthy state

---

## Phase 4: User Story 2 — Verify all services are reachable (Priority: P1)

**Goal**: Developer can confirm each infrastructure component is accessible from the host

**Independent Test**: After stack start, run connectivity checks — psql to Postgres, redis-cli PING, curl to Keycloak admin console, verify all DB roles

- [ ] T012 [P] [US2] Verify Postgres databases exist (`platform_db`, `keycloak_db`, `analytics_db`) and PostGIS extension is available via psql connect at localhost:5432
- [ ] T013 [P] [US2] Verify Redis PING/PONG via redis-cli at localhost:6379
- [ ] T014 [P] [US2] Verify Keycloak admin console loads at `http://localhost:8080` and `bornemap` realm is visible after login with `.env` credentials
- [ ] T015 [US2] Verify DB schema isolation: connect as `auth_service_role` → `users` accessible, `inventory` denied; connect as `admin_service_role` → `inventory` accessible; connect as `admin_analytics_role` → `analytics_db.audit_log` accessible

**Checkpoint**: US2 complete — all infrastructure components are reachable and correctly configured

---

## Phase 5: User Story 3 — Verify Traefik routes to correct backends (Priority: P2)

**Goal**: Developer can confirm Traefik routes by path prefix to the correct stub service

**Independent Test**: After stack start, curl each route and verify the response matches the expected stub

- [ ] T016 [P] [US3] Verify `GET /api/v1/auth/login` reaches auth-stub → returns `{"service":"auth-service","status":"stub"}`
- [ ] T017 [P] [US3] Verify `GET /api/v1/admin/partner` reaches admin-stub → returns `{"service":"admin-service","status":"stub"}`
- [ ] T018 [P] [US3] Verify `GET /api/v1/driver/stations` reaches driver-stub → returns `{"service":"driver-service","status":"stub"}`
- [ ] T019 [US3] Verify unmatched paths like `/api/v1/unknown` return `404` with `{"error":"route_not_found"}`

**Checkpoint**: US3 complete — all routes verified, stub responses correct, 404 catch-all working

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Health check automation, documentation updates, and final validation

- [ ] T020 [P] Create `source/infra/scripts/healthcheck.sh` — single script that checks all components (Postgres, Redis, Keycloak, Traefik, all routes) and exits 0 on success, 1 on failure
- [ ] T021 Update `docs/SYSTEM_STATE.md` to reflect infrastructure provisioned (platform_db, keycloak_db, analytics_db, Keycloak, Redis, Traefik all live)
- [ ] T022 Update `docs/roadmap_status.md` — mark Sprint 0 infra complete, advance MVP-1 status
- [ ] T023 Verify reproducibility: `git clone` a fresh copy, run `start.sh`, confirm all 7 containers reach healthy state (Config-as-Code repeatability)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — stack must be configured before it can start
- **User Story 2 (Phase 4)**: Depends on US1 — stack must be running to verify reachability
- **User Story 3 (Phase 5)**: Depends on US1 — stack must be running to verify routing; also depends on T008 (stubs in Foundational)
- **Polish (Phase 6)**: Depends on all user stories complete

### Within Each User Story

Tasks within a phase execute left-to-right (models before services, services before verification). Parallel `[P]` tasks within a phase can run concurrently.

### Parallel Opportunities

| Group | Tasks | Why |
|-------|-------|-----|
| Foundational config files | T005, T006, T007, T008 | Independent files, no cross-dependencies |
| US2 connectivity checks | T012, T013, T014 | Independent service verification |
| US3 route verification | T016, T017, T018 | Independent curl commands |
| Polish | T020 | Independent of doc updates |

---

## Parallel Example: Phase 2 — Foundational

```bash
# Launch all independent config files in parallel:
Task: "T005 — Postgres init scripts"
Task: "T006 — Keycloak realm export"
Task: "T007 — Traefik config"
Task: "T008 — Stub containers"
# All produce independent files under source/infra/
```

## Parallel Example: Phase 4 — US2 Verification

```bash
# Launch all connectivity checks in parallel:
Task: "T012 — Postgres DB check"
Task: "T013 — Redis PING check"
Task: "T014 — Keycloak admin check"
```

---

## Implementation Strategy

### MVP First (Phase 1 + 2 + 3 only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Run `start.sh` → all containers healthy → `stop.sh` → clean shutdown
5. MVP achieved: infrastructure stack can start, stop, and survive restart

### Incremental Delivery

1. Setup + Foundational → configuration complete
2. US1 (P1) → stack starts and stops → **MVP!**
3. US2 (P1) → all services verified reachable
4. US3 (P2) → routing verified
5. Polish → health check script + doc sync
