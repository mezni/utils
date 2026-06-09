---

description: "Task list for Sprint 1.6 — Integration and Hardening implementation"
---

# Tasks: Integration and Hardening

**Input**: Design documents from `/specs/006-integration-hardening/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Not requested — manual verification only (no test framework in MVP-1)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- Dashboard: `source/apps/dashboard/src/`
- Driver Web: `source/apps/driver-web/src/`
- Driver Mobile: `source/apps/driver-mobile/src/`
- Docs: `docs/`

---

## Phase 1: Setup

**Purpose**: Verify all apps are runnable and baseline the current state

- [X] T001 [P] Verify `pnpm mock` starts json-server with all 4 resources reachable at `/api/partners`, `/api/stations`, `/api/chargers`, `/api/station_availability`
- [X] T002 [P] Verify `pnpm dev:dashboard` starts without errors
- [X] T003 [P] Verify `pnpm dev:web` starts without errors
- [X] T004 [P] Verify `pnpm dev:mobile` starts without errors

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Audit current state of form validation, ErrorState coverage, and partner deletion behavior before making fixes

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T005 Audit all Dashboard forms in `source/apps/dashboard/src/pages/*/` — identify which forms lack required-field validation, which lat/lng fields lack range checking, and which screens lack ErrorState
- [X] T006 Audit all screens in `source/apps/driver-web/src/pages/` and `source/apps/driver-mobile/src/screens/` — verify ErrorState is present on every screen that fetches data
- [X] T007 Test json-server partner deletion behavior — document: does deleting a partner cascade to stations? Are orphaned stations created?

**Checkpoint**: Audit complete — known issues documented before fix sweep

---

## Phase 3: User Story 1 — Full Loop Verification (Priority: P1) 🎯 MVP

**Goal**: End-to-end verification of the complete MVP-1 product loop across all 4 apps

**Independent Test**: Walk through the 7 acceptance scenarios: admin create → verify → set live → partner manages → driver sees → admin deactivates → driver no longer sees. All steps verifiable without writing code.

### Implementation for User Story 1

- [X] T008 [US1] Create partner via Dashboard admin Partners screen — verify type field defaults, all three flags at correct initial state in `source/apps/dashboard/src/pages/Partners/PartnersPage.tsx`
- [X] T009 [US1] Verify partner via Dashboard admin Partners screen — confirm is_verified badge turns green, is_live toggle works in `source/apps/dashboard/src/pages/Partners/PartnersPage.tsx`
- [X] T010 [US1] Create station under verified partner via Dashboard admin Stations screen in `source/apps/dashboard/src/pages/Stations/StationsPage.tsx`
- [X] T011 [US1] Create chargers (one available, one maintenance) under that station via admin Chargers screen in `source/apps/dashboard/src/pages/Chargers/ChargersPage.tsx`
- [X] T012 [US1] Switch to Partner View in dev role switcher — verify partner sees only own station and chargers in `source/apps/dashboard/src/context/RoleContext.tsx`
- [X] T013 [US1] Update charger status to "maintenance" in Partner My Chargers screen in `source/apps/dashboard/src/pages/PartnerChargers/PartnerChargersPage.tsx`
- [X] T014 [US1] Verify Driver Web map reflects changes — station marker turns red on reload in `source/apps/driver-web/src/pages/MapPage.tsx`
- [X] T015 [US1] Verify Driver Mobile map reflects changes — station marker turns red on reload in `source/apps/driver-mobile/src/screens/MapScreen.tsx`
- [X] T016 [US1] Deactivate partner via admin Partners screen — verify stations disappear from both driver apps on reload
- [X] T017 [US1] Delete station via admin Stations screen — verify it disappears from both driver apps on reload

**Checkpoint**: Full loop verified end to end — all 7 acceptance scenarios pass

---

## Phase 4: User Story 2 — Fix Sweep (Priority: P2)

**Goal**: Edge case fixes across all 4 apps — form validation, ErrorState coverage, partner scoping, cross-browser/platform test

**Independent Test**: Stop json-server → all screens show ErrorState with retry. Enter invalid lat/lng → inline error. Submit empty forms → inline errors.

### Implementation for User Story 2

- [X] T018 [P] [US2] Fix lat/lng range validation in `source/apps/dashboard/src/pages/Stations/StationsPage.tsx` — reject latitude outside -90 to 90, longitude outside -180 to 180 with inline field errors before form submission
- [X] T019 [P] [US2] Fix required-field validation across all Dashboard forms in `source/apps/dashboard/src/components/shared/Input.tsx` and `source/apps/dashboard/src/components/shared/Modal.tsx` — ensure empty required fields blocked with inline errors before submission
- [X] T020 [P] [US2] Add or verify ErrorState with retry on every Dashboard screen in `source/apps/dashboard/src/pages/*/` — audit Overview, Partners, Stations, Chargers and all partner screens
- [X] T021 [P] [US2] Add or verify ErrorState with retry on Driver Web screens in `source/apps/driver-web/src/pages/` — MapPage and StationDetailPage
- [X] T022 [P] [US2] Add or verify ErrorState with retry on Driver Mobile screens in `source/apps/driver-mobile/src/screens/` — MapScreen and StationDetailScreen
- [X] T023 [US2] Verify partner scoping in Dashboard partner view — switch between partners, confirm each sees only own data in `source/apps/dashboard/src/pages/PartnerOverview/PartnerOverviewPage.tsx` and related partner pages
- [X] T024 [US2] Cross-browser test Dashboard and Driver Web — verify identical behavior in Chrome, Firefox, Safari (document any issues found)
- [X] T025 [US2] Cross-platform test Driver Mobile — verify identical behavior on iOS Simulator and Android Emulator; deny location permission → Tunisia fallback, no crash

**Checkpoint**: Fix sweep complete — all 8 acceptance scenarios pass

---

## Phase 5: User Story 3 — Documentation (Priority: P3)

**Goal**: Onboarding guide, mock API documentation, and MVP-1 status report

**Independent Test**: A developer follows the onboarding guide from a fresh clone and gets all apps running. The API doc describes all resources and filters correctly.

### Implementation for User Story 3

- [X] T026 [P] [US3] Write `docs/guides/onboarding.md` — step-by-step setup guide from fresh clone: prerequisites, install, start json-server, start each app, verify screens
- [X] T027 [P] [US3] Write `docs/api/mock-api.md` — document all 4 json-server resources with fields, filter params, and known limitations
- [X] T028 [P] [US3] Write `docs/project/phases/mvp-01-status.md` — record all completed sprints (1.1–1.6), what each delivered, and known trade-offs

**Checkpoint**: All documentation written and reviewable

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Decision recording, full loop re-verification, final sign-off

- [X] T029 Record partner deletion cascade/block decision in `docs/project/decisions.md` — recommended: block deletion when partner has stations, show warning with station count
- [X] T030 Run full end-to-end verification against quickstart.md — confirm all 7 acceptance scenarios in US1 still pass after fixes
- [X] T031 Final cross-browser and cross-platform spot check — verify no regressions from fix sweep

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories (audit must precede fixes)
- **US1 Full Loop (Phase 3)**: Depends on Foundational audit — runs verification without code changes
- **US2 Fix Sweep (Phase 4)**: Depends on Foundational audit results — applies fixes identified in T005/T006
- **US3 Documentation (Phase 5)**: Independent of US1/US2 — can start after Foundational
- **Polish (Phase 6)**: Depends on US1 + US2 + US3 completion

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — independent verification, no code changes
- **US2 (P2)**: Can start after Foundational — depends on audit results from T005/T006
- **US3 (P3)**: Can start after Foundational — independent of US1 and US2

### Within Each User Story

- Audit before fixes (Phase 2 before Phase 4)
- Core fixes before cross-browser/platform testing
- Documentation independent of code changes

### Parallel Opportunities

- T001, T002, T003, T004 can run in parallel (different apps/ports)
- T005, T006, T007 can run in parallel (different audit scopes)
- T018, T019, T020, T021, T022 can run in parallel (different files)
- T026, T027, T028 can run in parallel (different docs)
- US3 can run in parallel with US1 and US2
- Cross-browser testing (T024) can run in parallel with cross-platform testing (T025)

---

## Parallel Example: User Story 1

```bash
# Full loop is sequential walkthrough — no parallel tasks within US1.
# Each step depends on the previous (create partner → verify → create station → create chargers → etc.)
```

## Parallel Example: User Story 2

```bash
# Launch all fix tasks together:
Task: "Fix lat/lng range validation in StationsPage.tsx"
Task: "Fix required-field validation in Input.tsx / Modal.tsx"
Task: "Add ErrorState to all Dashboard screens"
Task: "Add ErrorState to Driver Web screens"
Task: "Add ErrorState to Driver Mobile screens"
```

## Parallel Example: User Story 3

```bash
# Launch all documentation tasks together:
Task: "Write onboarding guide"
Task: "Write mock API documentation"
Task: "Write MVP-1 status document"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (verify all apps run)
2. Complete Phase 2: Foundational (audit current state)
3. Complete Phase 3: User Story 1 (full loop verification)
4. **STOP and VALIDATE**: Full loop works — this alone validates MVP-1 completeness
5. Add documentation if needed

### Incremental Delivery

1. Setup + Foundational → Baseline established
2. US1 Full Loop → Verified product (can declare MVP-1 complete!)
3. US2 Fix Sweep → Edge cases handled
4. US3 Documentation → Future-proofed
5. Polish → Final sign-off

### Parallel Team Strategy

With multiple developers:

1. One person runs the full loop (US1) while another audits code (Phase 2)
2. Once audit is complete: Dev A fixes forms (US2), Dev B writes docs (US3)
3. Team reconvenes for final sign-off

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
