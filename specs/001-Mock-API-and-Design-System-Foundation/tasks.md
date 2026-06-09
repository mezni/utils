# Tasks: Mock API and Design System Foundation

**Input**: Design documents from `specs/001-Mock-API-and-Design-System-Foundation/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md

**Tests**: Not requested in this sprint (manual verification via curl/httpie for mock API, TypeScript compilation for tokens)

**Organization**: Tasks grouped by user story enabling independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: User story label (US1, US2, US3)
- Exact file paths are included in descriptions

## Path Conventions

Project paths follow the monorepo structure under `source/` as defined in the constitution and plan.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Initialize pnpm workspace and project root

- [X] T001 Create pnpm-workspace.yaml listing source/apps/* and source/packages/* in pnpm-workspace.yaml
- [X] T002 [P] Create root package.json with scripts: mock, dev:dashboard, dev:web, dev:mobile, dev in package.json
- [X] T003 [P] Create .gitignore for Node.js/pnpm monorepo at .gitignore

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Mock API infrastructure that MUST be complete before US1 can be verified

**⚠️ CRITICAL**: No US1 verification can begin until this phase is complete

- [X] T004 Create source/mock/ directory and install json-server dependency in source/mock/package.json
- [X] T005 Write routes.json for /api/* → /$1 rewrite in source/mock/routes.json
- [X] T006 Create source/packages/ui/ directory with package.json, tsconfig.json for the design token package in source/packages/ui/package.json

**Checkpoint**: Foundation ready — mock API and token package infrastructure exists

---

## Phase 3: User Story 1 — Mock API (Priority: P1) 🎯 MVP

**Goal**: json-server runs with all four resources reachable under /api prefix with seeded Tunisian EV data

**Independent Test**: Start `pnpm mock` and verify GET /api/partners returns 3 partners, GET /api/stations?partner_id=PRT002 returns filtered results, GET /api/chargers?station_id=STN001 returns station chargers

### Implementation for User Story 1

- [X] T007 [P] [US1] Define partner data: 3 partners with distinct flag states in source/mock/db.json
- [X] T008 [P] [US1] Define station data: 15 stations across 15 Tunisian cities in source/mock/db.json
- [X] T009 [P] [US1] Define charger data: 24 chargers with mixed connector types in source/mock/db.json
- [X] T010 [P] [US1] Define station_availability data: 15 availability records in source/mock/db.json
- [X] T011 [US1] Wire all four resources together in source/mock/db.json and verify relationships

**Checkpoint**: At this point, User Story 1 should be fully functional — all API endpoints respond correctly

---

## Phase 4: User Story 2 — Design Tokens (Priority: P2)

**Goal**: Shared design token package with colors, typography, spacing, radius, shadows consumable by all apps

**Independent Test**: Import colors.ts and verify brand.primary = #007943; import native.ts and verify values match colors.ts

### Implementation for User Story 2

- [X] T012 [P] [US2] Create color tokens with all brand.*, surface.*, text.*, border.*, status.* values in source/packages/ui/src/tokens/colors.ts
- [X] T013 [P] [US2] Create typography tokens with font family, size, weight values in source/packages/ui/src/tokens/typography.ts
- [X] T014 [P] [US2] Create spacing and radius tokens in source/packages/ui/src/tokens/spacing.ts and source/packages/ui/src/tokens/radius.ts
- [X] T015 [P] [US2] Create shadow tokens (card, panel, float, pin) in source/packages/ui/src/tokens/shadows.ts
- [X] T016 [P] [US2] Create native.ts with same values as colors.ts for React Native in source/packages/ui/src/tokens/native.ts
- [X] T017 [US2] Create barrel export index.ts for all tokens in source/packages/ui/src/tokens/index.ts
- [X] T018 [US2] Create tailwind.config.base.js extending all tokens in source/packages/ui/tailwind.config.base.js

**Checkpoint**: All design tokens defined and exportable — usable by web and React Native apps

---

## Phase 5: User Story 3 — Workspace Commands (Priority: P3)

**Goal**: `pnpm mock` starts json-server; `pnpm dev` lists all commands; workspace commands work from root

**Independent Test**: Run `pnpm mock` from repo root — json-server starts on port 3001

### Implementation for User Story 3

- [X] T019 [P] [US3] Add json-server dev dependency and mock script in source/mock/package.json
- [X] T020 [US3] Wire root pnpm mock script to run json-server from source/mock/ in package.json
- [X] T021 [US3] Add placeholder dev:dashboard, dev:web, dev:mobile scripts in root package.json

**Checkpoint**: Workspace fully operational — single command starts the mock API

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Verification and documentation

- [X] T022 [P] Verify all four resources reachable under /api prefix with curl
- [X] T023 [P] Verify filter queries (stations by partner_id, chargers by station_id)
- [X] T024 [P] Verify all token files compile without TypeScript errors
- [X] T025 [P] Verify native.ts values match colors.ts (no diff)
- [X] T026 Verify tailwind.config.base.js resolves brand.primary to #007943
- [X] T027 Run quickstart.md validation from scratch

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational (T005 routes.json, T004 json-server install)
- **User Story 2 (Phase 4)**: Depends on Foundational (T006 package init) — independent of US1
- **User Story 3 (Phase 5)**: Depends on Phase 1 completion — independent of US1 and US2
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: No dependencies on other user stories — fully independent
- **User Story 2 (P2)**: No dependencies on other user stories — fully independent
- **User Story 3 (P3)**: No dependencies on other user stories — fully independent

### Within Each Phase

- [P] tasks within a phase can run in parallel
- Order of non-[P] tasks within a phase should be respected

### Parallel Opportunities

- **Phase 1**: All tasks marked [P] can run in parallel (T002, T003)
- **Phase 2**: T005 and T006 are independent — can run in parallel
- **Phase 3**: T007–T010 are fully independent — all four seed files can be written in parallel
- **Phase 4**: T012–T016 are fully independent — all token files can be written in parallel
- **Phase 5**: T019 and T020 can be parallel

---

## Parallel Example: User Story 1

```bash
# All four data seed files can be written simultaneously:
Task: "Define partner data in source/mock/db.json"
Task: "Define station data in source/mock/db.json"
Task: "Define charger data in source/mock/db.json"
Task: "Define station_availability data in source/mock/db.json"
```

## Parallel Example: User Story 2

```bash
# All token files can be written simultaneously:
Task: "Create color tokens in source/packages/ui/src/tokens/colors.ts"
Task: "Create typography tokens in source/packages/ui/src/tokens/typography.ts"
Task: "Create spacing and radius tokens in source/packages/ui/src/tokens/spacing.ts and radius.ts"
Task: "Create shadow tokens in source/packages/ui/src/tokens/shadows.ts"
Task: "Create native.ts in source/packages/ui/src/tokens/native.ts"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (T004 routes.json, T005 json-server)
3. Complete Phase 3: User Story 1 (mock API with all 4 resources)
4. **STOP and VALIDATE**: Test US1 independently — curl all endpoints
5. Deploy/demo if ready — mock API is functional

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Mock API ready (MVP!)
3. Add User Story 2 → Test independently → Design tokens available
4. Add User Story 3 → Test independently → Workspace complete
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (mock API data)
   - Developer B: User Story 2 (design tokens)
   - Developer C: User Story 3 (workspace commands)
3. Stories are fully independent — zero cross-story dependencies

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- US1 and US2 have zero dependency on each other — genuinely parallel
- No tests requested for Sprint 1.1; verification is manual via curl and TypeScript compilation
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
