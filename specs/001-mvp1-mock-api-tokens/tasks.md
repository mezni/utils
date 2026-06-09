---

description: "Task list for MVP-1 Foundation Setup — mock API, design tokens, workspace"

---

# Tasks: MVP-1 Foundation Setup

**Input**: Design documents from `specs/001-mvp1-mock-api-tokens/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Manual verification only — no test framework tasks generated per spec scope.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2)
- Include exact file paths in descriptions

## Path Conventions

- **Monorepo root**: `.` (project root)
- **Mock API**: `source/mock/`
- **UI package**: `source/packages/ui/`
- Paths are relative to repository root unless noted

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization — workspace, scripts, dependencies

- [ ] T001 Create `pnpm-workspace.yaml` at repository root listing `source/apps/*` and `source/packages/*`
- [ ] T002 Create root `package.json` with dev scripts: `mock`, `dev:dashboard`, `dev:web`, `dev:mobile`, and `dev` (pnpm mock + pnpm dev:dashboard via concurrently)
- [ ] T003 Install root workspace dependencies: json-server, concurrently, typescript — run `pnpm install`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core directory structure that MUST be complete before user story work

- [ ] T004 Create directory structure: `source/mock/`, `source/packages/ui/src/tokens/`, `source/apps/`

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Station Data API (Priority: P1)

**Goal**: json-server mock API with 3 partners, 15 Tunisian stations, 24 chargers under `/api` prefix

**Independent Test**: Start `pnpm mock`, curl `GET /api/stations` returns 15 stations with coordinates and status

- [ ] T005 [P] [US1] Write `source/mock/db.json` with 3 partners, 15 stations (Tunisian city coordinates, status values), 24 chargers (AC/DC types, power_kw)
- [ ] T006 [P] [US1] Write `source/mock/routes.json` mapping `/api/*` → `/$1`
- [ ] T007 [US1] Verify mock API: `GET /api/partners` returns 3, `GET /api/stations` returns 15, `GET /api/chargers` returns 24, `GET /api/stations?partner_id=1` filters correctly, `GET /api/chargers?station_id=1` filters correctly

**Checkpoint**: All three apps can consume station data from the mock API

---

## Phase 4: User Story 2 - Design System Foundation (Priority: P1)

**Goal**: Design token package consumable by all apps — web (Tailwind) and mobile (native.ts)

**Independent Test**: Import color tokens from `source/packages/ui`, verify brand primary resolves to `#007943` and Tailwind config extends the full token set

- [ ] T008 [P] [US2] Initialize `source/packages/ui/package.json` as a scoped pnpm package
- [ ] T009 [P] [US2] Create `source/packages/ui/src/tokens/colors.ts` with full token set: brand, surface, text, border, status, neutral
- [ ] T010 [P] [US2] Create `source/packages/ui/src/tokens/typography.ts` with font families (Plus Jakarta Sans, Inter, Cairo) and sizes
- [ ] T011 [P] [US2] Create `source/packages/ui/src/tokens/spacing.ts` with 4px base scale: 4 8 12 16 20 24 32 40 48 64 80 96
- [ ] T012 [P] [US2] Create `source/packages/ui/src/tokens/radius.ts` with sm(4) md(8) lg(12) xl(16) 2xl(20) 3xl(24) full(9999)
- [ ] T013 [P] [US2] Create `source/packages/ui/src/tokens/shadows.ts` with card, panel, float, pin tokens
- [ ] T014 [US2] Create `source/packages/ui/src/tokens/native.ts` — React Native compatible values mirroring colors.ts (must stay synchronized per constitution)
- [ ] T015 [US2] Create `source/packages/ui/src/tokens/index.ts` re-exporting all token modules
- [ ] T016 [US2] Create `source/packages/ui/tailwind.config.base.js` extending all design tokens
- [ ] T017 [US2] Verify all token files compile: `npx tsc --noEmit source/packages/ui/src/tokens/*.ts`

**Checkpoint**: Design tokens are available and consumable from both web (Tailwind) and mobile (TypeScript imports)

---

## Phase 5: Polish & Cross-Cutting Concerns

**Purpose**: End-to-end verification and final validation

- [ ] T018 Full verification: `pnpm mock` serves all endpoints correctly; `pnpm dev` starts mock + dashboard concurrently; brand primary `#007943` resolves via Tailwind config
- [ ] T019 Run `specs/001-mvp1-mock-api-tokens/quickstart.md` verification checklist

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3+)**:
  - Both depend on Foundational phase completion
  - US1 and US2 are independent and can proceed in parallel
- **Polish (Phase 5)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) — No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational (Phase 2) — No dependencies on other stories
- **User Story 3 (P2)**: Fulfilled by Setup (Phase 1) and Polish (Phase 5) — workspace and scripts created in T001–T003, verified in T018

### Within Each User Story

- Core files before verification
- Parallelizable tasks marked [P] can run simultaneously

### Parallel Opportunities

- T005 and T006 can run in parallel (mock data + routes are separate files)
- T009 through T013 (all token files) can run in parallel
- US1 (Phase 3) and US2 (Phase 4) can be staffed by different developers

---

## Parallel Example: User Story 1

```bash
# Launch mock data and routes together:
Task: "T005 [P] [US1] Write source/mock/db.json with seeded data"
Task: "T006 [P] [US1] Write source/mock/routes.json mapping /api/* to /$1"
```

## Parallel Example: User Story 2

```bash
# Launch all independent token files together:
Task: "T009 [P] [US2] Create colors.ts"
Task: "T010 [P] [US2] Create typography.ts"
Task: "T011 [P] [US2] Create spacing.ts"
Task: "T012 [P] [US2] Create radius.ts"
Task: "T013 [P] [US2] Create shadows.ts"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (workspace + scripts + dependencies)
2. Complete Phase 2: Foundational (directories)
3. Complete Phase 3: User Story 1 (mock API with seeded data)
4. **STOP and VALIDATE**: Test that `pnpm mock` serves stations, partners, chargers
5. Stub the three apps in subsequent sprints

### Full Sprint Delivery

1. Complete Setup + Foundational → Foundation ready
2. Complete US1 (mock API) + US2 (design tokens) in parallel
3. Verify both stories independently
4. Run end-to-end verification in Polish phase

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Commit after each task or logical group
- US3 (Workspace & Developer Experience) is satisfied by Setup phase deliverables plus Polish phase verification
