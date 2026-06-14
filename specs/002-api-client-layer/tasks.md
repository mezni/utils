---

description: "Task list for API Client Layer implementation"
---

# Tasks: API Client Layer

**Input**: Design documents from `/specs/002-api-client-layer/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are included per constitution requirement — unit + integration for all 3 API functions.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- All paths under `source/front/packages/@bm/api-client/`
- `src/` for source, `tests/` for tests

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Package initialization and project structure

- [X] T001 Create `source/front/packages/@bm/api-client/` directory structure (`src/`, `tests/`)
- [X] T002 [P] Create `package.json` with workspace name `@bm/api-client`, TypeScript dependency, vitest
- [X] T003 [P] Create `tsconfig.json` with strict mode, ES2022 target, path aliases
- [X] T004 Create stub `src/index.ts` with placeholder exports

**Checkpoint**: Package skeleton ready — foundation work can begin

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T005 Create `ApiError` class in `src/errors.ts` extending `Error` with `status` and `data` fields
- [X] T006 [P] Create HTTP transport abstraction in `src/transport.ts` wrapping platform `fetch` with base URL injection
- [X] T007 [P] Define internal types in `src/types.ts` (request config, response shape, parameter validation helpers)
- [X] T008 Create `createApiClient` factory in `src/client.ts` that wires transport + error handling
- [X] T009 Export all public API from `src/index.ts` (`createApiClient`, `ApiError`, types)

**Checkpoint**: Foundation ready — all 3 user stories can now begin in parallel

---

## Phase 3: User Story 1 - Frontend queries all stations (Priority: P1) 🎯 MVP

**Goal**: Implement `getStations()` so a frontend app can fetch all stations via a single typed call.

**Independent Test**: Call `getStations()` against a mock backend — verify returned `Station[]` matches expected shape.

### Tests for User Story 1

- [X] T010 [P] [US1] Unit test for `getStations()` response parsing in `tests/client.test.ts`
- [X] T011 [P] [US1] Integration test for `getStations()` with mocked HTTP in `tests/integration.test.ts`

### Implementation for User Story 1

- [X] T012 [US1] Implement `getStations()` method in `src/client.ts` calling `GET /api/v1/stations`
- [X] T013 [US1] Add parameter validation and error handling for `getStations()` in `src/client.ts`

**Checkpoint**: User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Frontend queries station details by ID (Priority: P1)

**Goal**: Implement `getStationById(id)` so a frontend app can fetch a single station by ID.

**Independent Test**: Call `getStationById("STA-001")` against a mock backend — verify returned single `Station` object.

### Tests for User Story 2

- [X] T014 [P] [US2] Unit test for `getStationById()` with valid and invalid IDs in `tests/client.test.ts`
- [X] T015 [P] [US2] Integration test for `getStationById()` with mocked HTTP in `tests/integration.test.ts`

### Implementation for User Story 2

- [X] T016 [US2] Implement `getStationById(id)` method in `src/client.ts` calling `GET /api/v1/stations/{id}`
- [X] T017 [US2] Add parameter validation (non-empty string) for `getStationById()` in `src/client.ts`

**Checkpoint**: User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Frontend queries nearby stations (Priority: P1)

**Goal**: Implement `getNearbyStations(lat, lng, radius)` so a frontend app can fetch stations near map viewport.

**Independent Test**: Call `getNearbyStations(36.8, 10.18, 5000)` against a mock backend — verify results filtered by distance.

### Tests for User Story 3

- [X] T018 [P] [US3] Unit test for `getNearbyStations()` parameter validation in `tests/client.test.ts`
- [X] T019 [P] [US3] Integration test for `getNearbyStations()` with mocked HTTP in `tests/integration.test.ts`

### Implementation for User Story 3

- [X] T020 [US3] Implement `getNearbyStations(lat, lng, radius)` method in `src/client.ts` calling `GET /api/v1/stations/nearby?lat&lng&radius`
- [X] T021 [US3] Add parameter validation (lat/lng range, radius > 0) for `getNearbyStations()` in `src/client.ts`

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [X] T022 [P] Add request timeout handling in `src/transport.ts` (AbortController with configurable timeout)
- [X] T023 [P] Verify cross-platform compilation — confirm package compiles for both DOM and React Native tsconfig targets
- [X] T024 [P] Add eslint rule or barrel-file guard preventing `fetch`/`axios` imports outside `@bm/api-client` in consuming apps
- [X] T025 Run quickstart.md validation by implementing a minimal integration smoke test
- [X] T026 Audit final `src/index.ts` exports — ensure only `createApiClient`, `ApiError`, and types are public

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3-5)**: All depend on Foundational phase completion
  - User stories are fully independent — can proceed in parallel or sequentially
- **Polish (Phase 6)**: Depends on all 3 user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational — No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational — No dependencies on other stories
- **User Story 3 (P1)**: Can start after Foundational — No dependencies on other stories

### Within Each User Story

- Tests MUST be written and FAIL before implementation
- Implementation: method → validation → export
- Story complete before moving to next

### Parallel Opportunities

- T002, T003 (Setup) can run in parallel
- T006, T007 (Foundational) can run in parallel
- All US1 tests (T010, T011) can run in parallel
- All US2 tests (T014, T015) can run in parallel
- All US3 tests (T018, T019) can run in parallel
- **All 3 user stories can be implemented in parallel** — no cross-story dependencies

---

## Parallel Example: All User Stories

```bash
# Phase 2 (parallel):
Task: "Create ApiError class in src/errors.ts"
Task: "Create HTTP transport abstraction in src/transport.ts"
Task: "Define internal types in src/types.ts"

# Phase 3-5 (parallel — all 3 stories independent):
Task: "Implement getStations() in src/client.ts"
Task: "Implement getStationById() in src/client.ts"
Task: "Implement getNearbyStations() in src/client.ts"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1 (`getStations()`)
4. **STOP and VALIDATE**: Test `getStations()` independently
5. At this point the MVP delivers a working `getStations()` call — deployable for basic station listing

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add User Story 1 (`getStations()`) → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 (`getStationById()`) → Test independently → Deploy/Demo
4. Add User Story 3 (`getNearbyStations()`) → Test independently → Deploy/Demo
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

- 1 developer: Implement sequentially in priority order
- 2+ developers: Phase 2 together, then each developer takes a user story in parallel

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Verify tests fail before implementing
- Stop at any checkpoint to validate story independently
