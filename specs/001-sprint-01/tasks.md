# Tasks: Sprint 1 Backend and Database

**Input**: Design documents from `/specs/001-sprint-01/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Include test tasks for runtime, API, data, and user-facing changes.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and local runtime structure

- [ ] T001 Create the backend service and database directory skeleton in `source/services/bornemap-service/`, `database/migrations/`, and `database/seeds/`
- [ ] T002 [P] Add Python project metadata and dependency declarations in `source/services/bornemap-service/pyproject.toml`
- [ ] T003 [P] Add local development runtime files in `docker-compose.yml`, `source/services/bornemap-service/Dockerfile`, and `source/services/bornemap-service/.env.example`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before any user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 Configure Alembic bootstrap for the service in `database/alembic.ini` and `database/migrations/env.py`
- [ ] T005 Create shared configuration and database session modules in `source/services/bornemap-service/app/core/config.py` and `source/services/bornemap-service/app/core/db.py`
- [ ] T006 [P] Create the initial migration for `inventory` and `gis` schemas plus `partner`, `station`, and `charger` tables with foreign-key indexes in `database/migrations/versions/0001_create_inventory_gis_and_catalog_tables.py`
- [ ] T007 [P] Create the seed loader and Tunisia seed dataset in `database/seeds/seed_catalog.py` and `database/seeds/tunisia_catalog.json`
- [ ] T008 Create the FastAPI app factory, API router shell, and shared error handling in `source/services/bornemap-service/app/main.py`, `source/services/bornemap-service/app/api/router.py`, and `source/services/bornemap-service/app/core/errors.py`

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Local Service Is Usable Against Real Data (Priority: P1) 🎯 MVP

**Goal**: A team member can start the service locally and confirm it connects to a real database with the expected catalog structures in place.

**Independent Test**: A tester can start from a clean checkout, bring the local service up, and verify `GET /api/health` returns `200` against the real database.

### Tests for User Story 1

- [ ] T009 [P] [US1] Add health-check smoke coverage in `source/services/bornemap-service/tests/test_health.py`
- [ ] T010 [P] [US1] Add startup and database-connectivity smoke coverage in `source/services/bornemap-service/tests/test_startup.py`

### Implementation for User Story 1

- [ ] T011 [US1] Implement `GET /api/health` with a database reachability check in `source/services/bornemap-service/app/api/health.py`
- [ ] T012 [US1] Mount the health router through `source/services/bornemap-service/app/api/router.py` and `source/services/bornemap-service/app/main.py`

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Partners Manage the Catalog (Priority: P1)

**Goal**: A partner can create, update, and remove partner, station, and charger records so the catalog stays accurate.

**Independent Test**: A tester can exercise create, read, update, and delete actions for partners, stations, and chargers and see the changes persist.

### Tests for User Story 2

- [ ] T013 [P] [US2] Add partner CRUD smoke coverage in `source/services/bornemap-service/tests/test_partners_api.py`
- [ ] T014 [P] [US2] Add station CRUD smoke coverage in `source/services/bornemap-service/tests/test_stations_api.py`
- [ ] T015 [P] [US2] Add charger CRUD smoke coverage in `source/services/bornemap-service/tests/test_chargers_api.py`

### Implementation for User Story 2

- [ ] T016 [P] [US2] Create Partner, Station, and Charger ORM models in `source/services/bornemap-service/app/models/partner.py`, `source/services/bornemap-service/app/models/station.py`, and `source/services/bornemap-service/app/models/charger.py`
- [ ] T017 [P] [US2] Create request and response schemas for partners, stations, and chargers in `source/services/bornemap-service/app/schemas/partner.py`, `source/services/bornemap-service/app/schemas/station.py`, and `source/services/bornemap-service/app/schemas/charger.py`
- [ ] T018 [P] [US2] Implement shared catalog CRUD service operations in `source/services/bornemap-service/app/services/catalog_service.py`
- [ ] T019 [US2] Implement partner CRUD routes in `source/services/bornemap-service/app/api/partners.py`
- [ ] T020 [US2] Implement station CRUD routes in `source/services/bornemap-service/app/api/stations.py`
- [ ] T021 [US2] Implement charger CRUD routes in `source/services/bornemap-service/app/api/chargers.py`

**Checkpoint**: At this point, User Story 2 should be fully functional and testable independently

---

## Phase 5: User Story 3 - Drivers Find Nearby Stations (Priority: P1)

**Goal**: A driver can search nearby stations using location and radius so they can find the closest useful options quickly.

**Independent Test**: A tester can search from a known Tunisia location and see nearby stations returned in ascending distance order, with clean empty results when no stations match.

### Tests for User Story 3

- [ ] T022 [P] [US3] Add nearby-search smoke coverage in `source/services/bornemap-service/tests/test_nearby_api.py`

### Implementation for User Story 3

- [ ] T023 [US3] Implement the distance helper and result ordering in `source/services/bornemap-service/app/services/nearby_service.py`
- [ ] T024 [US3] Implement `GET /api/stations/nearby` in `source/services/bornemap-service/app/api/stations.py`

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T025 [P] Validate the quickstart steps and sample commands in `specs/001-sprint-01/quickstart.md`
- [ ] T026 [P] Reconcile the API contract document with implemented endpoints in `specs/001-sprint-01/contracts/api.md`
- [ ] T027 [P] Validate seed data and data-model consistency against `specs/001-sprint-01/data-model.md` and `database/seeds/seed_catalog.py`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - blocks all user stories
- **User Stories (Phase 3+)**: Depend on Foundational phase completion
- **Polish (Final Phase)**: Depends on the desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational - no dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational - may reuse shared models and services but remains independently testable
- **User Story 3 (P1)**: Can start after Foundational - may reuse the station read model but remains independently testable

### Within Each User Story

- Tests should be written and fail before implementation for runtime changes
- Models and schemas before services
- Services before routes
- Story complete before moving to the next priority

### Parallel Opportunities

- `T002` and `T003` can run in parallel after `T001`
- `T006` and `T007` can run in parallel after `T004` and `T005`
- `T009` and `T010` can run in parallel
- `T013`, `T014`, and `T015` can run in parallel
- `T016`, `T017`, and `T018` can run in parallel
- `T025`, `T026`, and `T027` can run in parallel

---

## Parallel Example: User Story 2

```bash
# Run catalog CRUD smoke coverage in parallel:
Task: "Add partner CRUD smoke coverage in source/services/bornemap-service/tests/test_partners_api.py"
Task: "Add station CRUD smoke coverage in source/services/bornemap-service/tests/test_stations_api.py"
Task: "Add charger CRUD smoke coverage in source/services/bornemap-service/tests/test_chargers_api.py"

# Build the catalog domain layer in parallel:
Task: "Create Partner, Station, and Charger ORM models in source/services/bornemap-service/app/models/partner.py, source/services/bornemap-service/app/models/station.py, and source/services/bornemap-service/app/models/charger.py"
Task: "Create request and response schemas for partners, stations, and chargers in source/services/bornemap-service/app/schemas/partner.py, source/services/bornemap-service/app/schemas/station.py, and source/services/bornemap-service/app/schemas/charger.py"
Task: "Implement shared catalog CRUD service operations in source/services/bornemap-service/app/services/catalog_service.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1
4. Stop and validate `GET /api/health` against the real database

### Incremental Delivery

1. Setup + Foundational → local runtime and database ready
2. User Story 1 → service health validated against real DB
3. User Story 2 → catalog CRUD becomes usable
4. User Story 3 → nearby search becomes usable
5. Finish with polish and cross-cutting validation

### Parallel Team Strategy

1. Team completes Setup + Foundational together
2. Once foundational work is done:
   - Developer A: User Story 1
   - Developer B: User Story 2
   - Developer C: User Story 3
3. Stories complete and integrate independently

---

## Notes

- `[P]` tasks can run in parallel when they touch different files and have no dependencies
- `[Story]` labels map tasks to specific user stories for traceability
- Each user story should be independently completable and testable
- Keep the MVP slice focused on backend/database delivery
