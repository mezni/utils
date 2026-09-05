---

description: "Task list for Infrastructure & Multi-Schema Engine"

---

# Tasks: Infrastructure & Multi-Schema Engine

**Input**: Design documents from `/specs/001-infra-multi-schema-engine/`

**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/

**Tests**: The plan's project structure prescribes contract and integration test
files (`tests/contract/test_health.py`, `tests/integration/test_startup.py`,
`tests/integration/test_config.py`), so test tasks are included and follow a
write-first (TDD) sequence.

**Organization**: Tasks are grouped by user story to enable independent
implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Single project**: `app/`, `migrations/`, `tests/` at repository root (per
  plan.md "Project Structure")
- Container files (`Dockerfile`, `docker-compose.yml`, `.dockerignore`) at repo
  root

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Create `pyproject.toml` at repo root with project metadata and
  dependencies (fastapi, sqlmodel, sqlalchemy[asyncio], asyncpg, alembic,
  pydantic-settings, uvicorn; dev: pytest, pytest-asyncio, httpx, ruff) plus
  lint/format configuration
- [X] T002 [P] Create `app/` package skeleton with `app/__init__.py`
- [X] T003 [P] Create multi-stage `Dockerfile` at repo root (Python 3.12 runtime
  with project dependencies installed, uvicorn entrypoint)
- [X] T004 [P] Create `docker-compose.yml` at repo root: `app` service (build ., 
  ports 8000:8000, env defaults) and `db` service (postgres:16, healthcheck via
  pg_isready, named volume `telco-pgdata` mounted at /var/lib/postgresql/data)
- [X] T005 [P] Create `.dockerignore` at repo root (`.git`, `__pycache__`,
  `.venv`, `tests`, `.pytest_cache`, `*.pyc`)
- [X] T006 [P] Create test packages `tests/__init__.py`,
  `tests/contract/__init__.py`, `tests/integration/__init__.py`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can
be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T007 Implement `app/config.py` — pydantic-settings `Settings` with
  `DATABASE_URL`, `API_HOST`, `API_PORT`, `DB_RETRY_WINDOW`, defaults per
  `contracts/environment-config.md`, env-var override support
- [X] T008 [P] Implement `app/database.py` — SQLModel async engine (asyncpg)
  bound to `DATABASE_URL`, session factory, and `check_connection()` helper
  (SELECT 1) for health readiness (depends on T007)
- [X] T009 [P] Initialize Alembic at repo root — `alembic.ini` and
  `migrations/` scaffolding (env.py, script.py.mako, versions/)
- [X] T010 Configure `migrations/env.py` — `include_schemas=True`, explicit
  six-schema list (`catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`),
  auto-`CREATE SCHEMA IF NOT EXISTS` for each before upgrade
- [X] T011 Generate baseline revision `initial_multi_schema` in
  `migrations/versions/` that creates the six empty domain schemas
- [X] T012 Implement startup migration runner in `app/main.py` lifespan —
  bounded DB connectivity retry within `DB_RETRY_WINDOW` (default 30s) →
  `alembic upgrade head` → emit the pinned `READY: app listening on
  {API_HOST}:{API_PORT}` line → start serving; on failure log a clear error and
  exit non-zero (depends on T007, T010; per `contracts/startup-migrations.md`)

**Checkpoint**: Foundation ready - user story implementation can now begin in
parallel

---

## Phase 3: User Story 1 - Containerized Development Environment (Priority: P1) 🎯 MVP

**Goal**: Single-command start/teardown of an `app` + `db` environment with
persistent data and a health signal.

**Independent Test**: Run `docker compose up -d --build` on a clean host; both
services reach a healthy state, `GET /health` returns ok, the `READY` log line is
emitted, and app restart preserves database data.

### Tests for User Story 1 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T013 [P] [US1] Write contract test for `GET /health` in
  `tests/contract/test_health.py` asserting 200 `{"status":"ok",
  "database":"up"}` and 503 `{"status":"error","database":"down"}` per
  `contracts/health-api.md`

### Implementation for User Story 1

- [X] T014 [P] [US1] Implement `/health` endpoint in `app/main.py` returning
  application and database connectivity status via
  `database.check_connection()` (depends on T013, T008)
- [X] T015 [US1] Verify compose lifecycle per quickstart Scenarios 1 and 4 —
  `up -d --build`, `down`, `up`: `telco-pgdata` volume preserved, `/health` ok,
  `READY` log present
- [X] T016 [US1] Verify port-conflict behavior per quickstart Scenario 7 —
  with host port 8000 occupied, Compose fails with a clear binding error

**Checkpoint**: At this point, User Story 1 should be fully functional and
testable independently

---

## Phase 4: User Story 2 - Multi-Schema Migration Framework (Priority: P1)

**Goal**: Reproducible six-schema database via versioned, idempotent migrations.

**Independent Test**: From an empty database, `upgrade head` creates exactly the
six domain schemas; re-running at the head is a no-op.

### Tests for User Story 2 ⚠️

- [X] T017 [P] [US2] Write integration test in `tests/integration/test_startup.py`
  asserting that after startup the six schemas (`catalog`, `inventory`, `crm`,
  `usage`, `billing`, `dunning`) exist and `alembic current` is at head

### Implementation for User Story 2

- [X] T018 [US2] Verify fresh-instance migration per quickstart Scenario 2 —
  `docker compose down -v`, `up -d`, `\dn` shows six schemas, `alembic current`
  reports head
- [X] T019 [US2] Verify incremental upgrade per US2/AC4 (spec.md:49) — with an
  environment at an earlier migration revision, start the app and confirm
  `alembic upgrade head` applies only the pending revisions and `alembic
  current` reports head
- [X] T020 [US2] Verify migration idempotency per quickstart Scenario 3 —
  restart app, confirm no-op at head and `READY` emitted again
- [X] T021 [US2] Verify migration-failure behavior per quickstart Scenario 6 —
  with `db` stopped, app retries within the bounded window then exits with a
  clear error and non-zero status; also confirm that a modified applied
  revision fails startup with a clear error (per
  `contracts/startup-migrations.md`)

**Checkpoint**: At this point, User Stories 1 AND 2 should both work
independently

---

## Phase 5: User Story 3 - Configurable Database Connectivity (Priority: P2)

**Goal**: Environment-variable-driven connectivity that can target any
PostgreSQL instance without code changes.

**Independent Test**: Override `DATABASE_URL`; the application connects to the
alternate target and reaches `READY`; `/health` reflects connectivity.

### Tests for User Story 3 ⚠️

- [X] T022 [P] [US3] Write test in `tests/integration/test_config.py` asserting a
  `Settings` instance picks up an overridden `DATABASE_URL` from the environment
  (per `contracts/environment-config.md`)

### Implementation for User Story 3

- [X] T023 [US3] Verify alternate-target connectivity end-to-end per quickstart
  Scenario 5 — app launched with an overridden `DATABASE_URL` connects and
  reaches `READY`
- [X] T024 [US3] Verify degraded behavior per `contracts/health-api.md` — with an
  unreachable target, `GET /health` returns 503 `{"database":"down"}`

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [X] T025 [P] Update `docs/OPERATIONS.md`, `docs/ARCHITECTURE.md`, and
  `README.md` quick-start commands to the canonical `app.*` layout and
  six-schema automatic-migration startup behavior
- [X] T026 Run ALL quickstart.md scenarios end-to-end (Scenarios 1-7) in order,
  record results, and record time-to-READY to confirm it meets SC-001/SC-004
  (<5 min)
- [X] T027 [P] Run the full test suite (`pytest -q`) in the `app` container and
  confirm green
- [X] T028 [P] Run `ruff check` and `ruff format --check` and fix any findings

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Final Phase)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No
  dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational (Phase 2) - Startup
  migration runner lives in Foundational (T012); no dependency on US1
- **User Story 3 (P2)**: Can start after Foundational (Phase 2) - Depends on
  T007 (config) and T008 (database) completed in Foundational

### Within Each User Story

- Tests (where included) MUST be written and FAIL before implementation
- Contract/schema tests before endpoint/migration implementation
- Implementation before verification tasks (T015/T016, T018-T021, T023/T024)
- Core implementation before integration verification

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel (T002-T006)
- Foundational T008 and T009 can run in parallel after T007
- T013 and T014 can run in parallel once T008 is complete
- Different user stories can be worked on in parallel by different team members
  after Foundational completes

---

## Parallel Example: Setup and Foundational Tasks

```bash
# Launch all Phase 1 setup tasks together:
Task: "Create app/ package skeleton with app/__init__.py" (T002)
Task: "Create multi-stage Dockerfile at repo root (Python 3.12)" (T003)
Task: "Create docker-compose.yml with app and db services" (T004)

# Launch parallel foundational tasks together (after T007 config):
Task: "Implement app/database.py async engine + session factory" (T008)
Task: "Initialize Alembic at repo root (alembic.ini + migrations/)" (T009)
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1 (container + `/health` + readiness)
4. **STOP and VALIDATE**: `docker compose up -d --build`, `GET /health`, `READY`
   log
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready (config, db engine,
   migrations runner)
2. Add User Story 1 → health endpoint + container lifecycle verified (MVP!)
3. Add User Story 2 → migration behaviors verified (fresh, incremental,
   idempotent, fail)
4. Add User Story 3 → env-driven connectivity verified
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (container readiness)
   - Developer B: User Story 2 (migration behaviors)
   - Developer C: User Story 3 (config connectivity) - after US1/US2 to avoid
     shared-file conflicts in `app/main.py`
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break
  independence
- US1/US2 both target `app/main.py` (health endpoint vs startup runner) — T012
  lives in Foundational so US1 and US2 phases avoid editing the same file