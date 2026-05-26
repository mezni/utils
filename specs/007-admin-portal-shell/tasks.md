# Tasks: Admin Portal — Shell, Navigation & BaseMap

**Input**: Design documents from `specs/007-admin-portal-shell/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Not requested. All tasks focus on implementation only.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Frontend web app**: `sources/frontend/apps/admin-portal/src/`, `sources/frontend/packages/ui/src/`

## Phase 1: Setup

**Purpose**: Verify project initialization and dependency installation

- [ ] T001 Install dependencies and verify build — run `pnpm install` in `sources/frontend/`, then `pnpm -r build`

**Checkpoint**: `pnpm -r build` passes with no errors

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core layout infrastructure that MUST be complete before ANY user story can display content

- [ ] T002 [P] Create AppShell layout component in `sources/frontend/apps/admin-portal/src/components/layout/app-shell.tsx` — sidebar slot on left, main content area on right with `<Outlet/>` for React Router
- [ ] T003 [P] Create SidebarNav component in `sources/frontend/apps/admin-portal/src/components/layout/sidebar-nav.tsx` — 6 navigation items (Overview, Users, Data, Analytics, Security, Settings) with icons, active state highlighting, and route click handlers
- [ ] T004 [P] Create Header component in `sources/frontend/apps/admin-portal/src/components/layout/header.tsx` — contains heading text and a reserved slot for the sandbox toggle (toggle functionality implemented in US3)
- [ ] T005 [P] Create `sources/frontend/apps/admin-portal/src/App.tsx` — extract the App component from `main.tsx` into its own file with all necessary imports; add `export default App` so `main.tsx` can import it
- [ ] T006 Refactor `sources/frontend/apps/admin-portal/src/App.tsx` — import and render AppShell as layout wrapper; define `<Routes>` and `<Route>` for all 6 sections inside AppShell's children; keep existing BrowserRouter wrapper in `main.tsx`

**Checkpoint**: Portal renders AppShell with sidebar and header, main content area is ready for route content

---

## Phase 3: User Story 1 — Admin navigates the portal via sidebar (Priority: P1) 🎯 MVP

**Goal**: Admin can click sidebar items to navigate between 6 sections, each with its own placeholder page. URL updates reflect the active section.

**Independent Test**: Open the portal, click each sidebar item, verify the main content area updates with a unique page title and the URL changes accordingly.

### Implementation for User Story 1

- [ ] T007 [P] [US1] Create Overview placeholder page in `sources/frontend/apps/admin-portal/src/pages/overview.tsx` — page component shell (full implementation in US2)
- [ ] T008 [P] [US1] Create Users placeholder page in `sources/frontend/apps/admin-portal/src/pages/users.tsx` — page component with placeholder content
- [ ] T009 [P] [US1] Create Data placeholder page in `sources/frontend/apps/admin-portal/src/pages/data.tsx` — page component with placeholder content
- [ ] T010 [P] [US1] Create Analytics placeholder page in `sources/frontend/apps/admin-portal/src/pages/analytics.tsx` — page component with placeholder content
- [ ] T011 [P] [US1] Create Security placeholder page in `sources/frontend/apps/admin-portal/src/pages/security.tsx` — page component with placeholder content
- [ ] T012 [P] [US1] Create Settings placeholder page in `sources/frontend/apps/admin-portal/src/pages/settings.tsx` — page component with placeholder content
- [ ] T013 [US1] Wire React Router routes in `sources/frontend/apps/admin-portal/src/App.tsx` — define `<Route>` for each of the 6 sections (overview at `/`, users at `/users`, data at `/data`, analytics at `/analytics`, security at `/security`, settings at `/settings`) with `<Navigate to="/" />` for root redirect

**Checkpoint**: All 6 sidebar items navigate to distinct placeholder pages. URL updates correctly. Active nav item is highlighted.

---

## Phase 4: User Story 2 — Admin views the Overview Dashboard with map and metrics (Priority: P1)

**Goal**: Landing page shows three metric chips (total stations, total chargers, total partners), an interactive Leaflet map with station markers, skeleton loading placeholders, and placeholder analytics cards.

**Independent Test**: Load the portal landing page — metric chips show numeric values, map displays station markers, and clicking a marker shows a popup with station info.

### Implementation for User Story 2

- [ ] T014 [P] [US2] Create MetricChip component in `sources/frontend/packages/ui/src/components/ui/metric-chip.tsx` — accepts `label`, `value`, `isLoading` props; shows skeleton placeholder when loading
- [ ] T015 [US2] Create BaseMap component in `sources/frontend/apps/admin-portal/src/components/map/base-map.tsx` — Leaflet map centered on Tunisia [33.8869, 9.5375] zoom 7, CartoDB light tiles, station markers as green circles with lightning bolt SVG icon
- [ ] T016 [US2] Create OverviewDashboard component in `sources/frontend/apps/admin-portal/src/components/overview/overview-dashboard.tsx` — fetches station list and total from `/api/v1/stations`, charger count from `/api/v1/chargers`, partner count from `/api/v1/partners`; renders MetricChip components, BaseMap, and analytics placeholder cards; shows skeleton placeholders during loading
- [ ] T017 [US2] Implement error state display in OverviewDashboard at `sources/frontend/apps/admin-portal/src/components/overview/overview-dashboard.tsx` — when any API call fails, replace failed section's skeleton with inline error message; map shows empty state with error note; page must not crash (per FR-012)
- [ ] T018 [US2] Add marker click popup to BaseMap in `sources/frontend/apps/admin-portal/src/components/map/base-map.tsx` — clicking a station marker shows popup with station name, city, charger count, and "View Details" link
- [ ] T019 [US2] Wire OverviewDashboard into Overview page in `sources/frontend/apps/admin-portal/src/pages/overview.tsx` — replace placeholder with `<OverviewDashboard />`

**Checkpoint**: Overview Dashboard loads metric chips with correct data, map shows station markers, marker popup works, skeleton placeholders shown during load, error states handled gracefully.

---

## Phase 5: User Story 3 — Admin uses the Sandbox Workspace toggle (Priority: P2)

**Goal**: Admin activates sandbox mode via header toggle, seeing a `border-t-4 border-sky-500` indicator. Preference persists in localStorage.

**Independent Test**: Toggle the sandbox switch on → blue border appears. Toggle off → border disappears. Refresh the page → state persists.

### Implementation for User Story 3

- [ ] T020 [P] [US3] Implement SandboxToggle component in `sources/frontend/apps/admin-portal/src/components/layout/header.tsx` — toggle switch UI, reads/writes `bornemap_admin_sandbox` key in localStorage
- [ ] T021 [US3] Add sandbox context provider in `sources/frontend/apps/admin-portal/src/context/sandbox-context.tsx` — React context providing `isSandboxActive` state and setter, initialized from localStorage, syncs back on change
- [ ] T022 [US3] Apply blue border indicator in `sources/frontend/apps/admin-portal/src/components/layout/app-shell.tsx` — when `isSandboxActive` is true, add `border-t-4 border-sky-500` class to layout top

**Checkpoint**: Sandbox toggle in header activates/deactivates blue border. State persists across page refreshes via localStorage.

---

## Phase 6: User Story 4 — Admin sees consistent UI components and design tokens (Priority: P2)

**Goal**: All UI components follow consistent design tokens. Reusable components exist for settings cards, dropdown selects, and destructive confirmation modals.

**Independent Test**: Open Settings section — SettingsCard renders with correct styling. Trigger a delete action — ConfirmDeleteModal appears with disabled button until exact ID is typed.

### Implementation for User Story 4

- [ ] T023 [P] [US4] Create SettingsCard component in `sources/frontend/packages/ui/src/components/ui/settings-card.tsx` — accepting `title`, `description`, `children` props; styled with rounded-2xl, shadow-card, p-6 per design tokens
- [ ] T024 [P] [US4] Create SelectSetting component in `sources/frontend/packages/ui/src/components/ui/select-setting.tsx` — dropdown with rounded-md styling per design tokens; accepts `label`, `options`, `value`, `onChange`
- [ ] T025 [P] [US4] Create ConfirmDeleteModal component in `sources/frontend/packages/ui/src/components/ui/confirm-delete-modal.tsx` — confirmation modal requiring exact resource ID match before confirm button enables; accepts `isOpen`, `resourceId`, `resourceLabel`, `onConfirm`, `onCancel`
- [ ] T026 [US4] Export new UI components from `sources/frontend/packages/ui/src/index.ts` — add exports for SettingsCard, SelectSetting, ConfirmDeleteModal, MetricChip

**Checkpoint**: SettingsCard, SelectSetting, and ConfirmDeleteModal render correctly with design token styling. All exported from packages/ui.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Verification and cleanup across all stories

- [ ] T027 [P] Run type-check — `pnpm -r type-check` in `sources/frontend/`
- [ ] T028 [P] Run lint — `pnpm -r lint` in `sources/frontend/`
- [ ] T029 [P] Run build — `pnpm -r build` in `sources/frontend/`
- [ ] T030 Run quickstart validation — execute all verification steps in `specs/007-admin-portal-shell/quickstart.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 — Navigation (Phase 3)**: Depends on Foundational completion
- **US2 — Overview Dashboard (Phase 4)**: Depends on Foundational completion; independent of US1
- **US3 — Sandbox Toggle (Phase 5)**: Depends on Foundational completion; independent of US1/US2
- **US4 — Design System (Phase 6)**: Depends on Foundational completion; independent of US1/US2/US3
- **Polish (Phase 7)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: After Foundational — No dependencies on other stories
- **User Story 2 (P1)**: After Foundational — No dependencies on other stories
- **User Story 3 (P2)**: After Foundational — No dependencies on other stories
- **User Story 4 (P2)**: After Foundational — No dependencies on other stories

### Within Each Phase

- Parallel tasks ([P]) as noted can be executed concurrently
- Non-[P] tasks are sequential within each phase
- Phases must complete in order due to story dependencies

### Parallel Opportunities

- T002, T003, T004, T005 (Foundational) can run in parallel — different files, no cross-dependencies
- T007-T012 (US1 placeholder pages) can all run in parallel — independent page components
- T014 (MetricChip) and T015 (BaseMap) can run in parallel
- T020 (SandboxToggle) and T021 (SandboxContext) can run in parallel
- T023, T024, T025 (Design system components) can all run in parallel
- US2, US3, US4 can be worked on in parallel after Foundational completes

---

## Parallel Example: Phase 2 Foundational

```bash
# Launch all Foundational tasks together:
Task: "Create AppShell layout component"
Task: "Create SidebarNav component"
Task: "Create Header component"
Task: "Create App.tsx component"
```

## Parallel Example: User Story 1

```bash
# Launch all placeholder pages together:
Task: "Create Overview placeholder page"
Task: "Create Users placeholder page"
Task: "Create Data placeholder page"
Task: "Create Analytics placeholder page"
Task: "Create Security placeholder page"
Task: "Create Settings placeholder page"
```

## Parallel Example: User Story 4

```bash
# Launch all design system components together:
Task: "Create SettingsCard component"
Task: "Create SelectSetting component"
Task: "Create ConfirmDeleteModal component"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (Navigation with placeholder pages)
4. **STOP and VALIDATE**: Sidebar navigation works, all 6 pages render
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add US1 (Navigation) → Can navigate between 6 placeholder sections
3. Add US2 (Overview Dashboard) → Landing page shows metrics + map
4. Add US3 (Sandbox Toggle) → Sandbox mode with persistent toggle
5. Add US4 (Design System) → Consistent UI components throughout
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Complete Setup + Foundational together
2. Once Foundational is done:
   - Developer A: US1 (Navigation + placeholder pages)
   - Developer B: US2 (Overview Dashboard)
   - Developer C: US3 + US4 (Sandbox + Design System)
3. Stories complete and integrate independently (no cross-story dependencies)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
