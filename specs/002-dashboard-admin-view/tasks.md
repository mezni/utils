# Tasks: Dashboard Admin View

**Input**: Design documents from `/specs/002-dashboard-admin-view/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: No tests requested in Sprint 1.2 — manual verification against json-server per Sprint 1.1 convention.

**Organization**: Tasks grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Dashboard App**: `source/apps/dashboard/` — Vite + React + TypeScript + Tailwind

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Initialize the Dashboard App project with Vite + React + TypeScript + Tailwind

- [ ] T001 Create `source/apps/dashboard/` by running `pnpm create vite` with React + TypeScript template
- [ ] T002 Install dependencies: `react-router-dom`, `tailwindcss`, `postcss`, `autoprefixer` in `source/apps/dashboard/`
- [ ] T003 Configure Tailwind to extend `source/packages/ui/tailwind.config.base.js` as a preset in `source/apps/dashboard/tailwind.config.js`
- [ ] T004 Create `postcss.config.js` in `source/apps/dashboard/` with Tailwind and autoprefixer plugins
- [ ] T005 Add `@tailwind base; @tailwind components; @tailwind utilities;` directives in `source/apps/dashboard/src/index.css`
- [ ] T006 Set up `VITE_API_BASE_URL=http://localhost:3001` in `source/apps/dashboard/.env`
- [ ] T007 Add `dev:dashboard` script (`pnpm --filter @borne-map/dashboard dev`) to root `package.json`

**Checkpoint**: `pnpm dev:dashboard` starts Vite dev server on port 5173 with Tailwind processing active

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared infrastructure that MUST be complete before any user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T008 [P] Create fetch wrapper in `source/apps/dashboard/src/api/client.ts` with `/api` prefix, error handling, and `fetchWithError` helper
- [ ] T009 [P] Create `RoleContext` provider in `source/apps/dashboard/src/context/RoleContext.tsx` storing `role` (admin|partner) and `selectedPartnerId`
- [ ] T010 [P] Create `StatCard` component in `source/apps/dashboard/src/components/shared/StatCard.tsx` with label, value, icon props
- [ ] T011 [P] Create `DataTable` component in `source/apps/dashboard/src/components/shared/DataTable.tsx` with configurable columns, render functions, and actions column
- [ ] T012 [P] Create `StatusBadge` component in `source/apps/dashboard/src/components/shared/StatusBadge.tsx` with color-coded variants (available/in_use/maintenance/offline)
- [ ] T013 [P] Create `Modal` component in `source/apps/dashboard/src/components/shared/Modal.tsx` with `isOpen`, `onClose`, title, children props
- [ ] T014 [P] Create `EmptyState` component in `source/apps/dashboard/src/components/shared/EmptyState.tsx` with icon, message, action button props
- [ ] T015 [P] Create `ErrorState` component in `source/apps/dashboard/src/components/shared/ErrorState.tsx` with error message and Retry button
- [ ] T016 [P] Create `Skeleton` component in `source/apps/dashboard/src/components/shared/Skeleton.tsx` for loading states
- [ ] T017 [P] Create `Button` component in `source/apps/dashboard/src/components/shared/Button.tsx` with variants (primary, secondary, danger, ghost)
- [ ] T018 [P] Create `Input` component in `source/apps/dashboard/src/components/shared/Input.tsx` with label, error state, and validation styling

**Checkpoint**: Shared components render correctly in isolation — foundation ready for all user stories

---

## Phase 3: User Story 1 — Dashboard Shell and Navigation (Priority: P1) 🎯 MVP

**Goal**: AppShell with fixed sidebar, top bar, React Router, and dev role switcher

**Independent Test**: Open the Dashboard App on port 5173. The left sidebar shows four navigation items (Overview, Partners, Stations, Chargers) with the first item highlighted. Toggle the dev role switcher — navigation changes to partner items and a partner selector dropdown appears. Refresh the page — dev role state resets.

### Implementation for User Story 1

- [ ] T019 [P] [US1] Create `NavigationItem` component in `source/apps/dashboard/src/components/layout/NavigationItem.tsx` with icon, label, active state (brand.sageLight bg, brand.primary text)
- [ ] T020 [P] [US1] Create `Sidebar` component in `source/apps/dashboard/src/components/layout/Sidebar.tsx` with brand header, nav items, and dev role switcher at bottom
- [ ] T021 [P] [US1] Create `TopBar` component in `source/apps/dashboard/src/components/layout/TopBar.tsx` with page title and placeholder avatar
- [ ] T022 [P] [US1] Create `PageContent` component in `source/apps/dashboard/src/components/layout/PageContent.tsx` as scrollable flex-1 wrapper with p-6 and surface.background
- [ ] T023 [US1] Create `AppShell` component in `source/apps/dashboard/src/components/layout/AppShell.tsx` composing Sidebar + TopBar + PageContent with Outlet
- [ ] T024 [US1] Create dev role switcher inside Sidebar with toggle button ("Admin View" / "Partner View") labeled "Dev Only — removed in MVP-3"
- [ ] T025 [US1] Create partner selector dropdown inside Sidebar (visible only in Partner View) that fetches all partners from API and calls `setSelectedPartnerId`
- [ ] T026 [US1] Set up React Router in `source/apps/dashboard/src/App.tsx` with layout route wrapping AppShell and nested index routes for Overview, Partners, Stations, Chargers (placeholder pages initially)
- [ ] T027 [US1] Update `source/apps/dashboard/src/main.tsx` to render App with BrowserRouter and RoleContext provider

**Checkpoint**: AppShell renders with working sidebar navigation, dev role switcher, and placeholder pages for all routes

---

## Phase 4: User Story 2 — Admin Overview Screen (Priority: P1)

**Goal**: Three stat cards (total partners, stations, chargers) and recent stations table

**Independent Test**: Navigate to Overview. Three stat cards show correct counts from API. Stations table loads with name, partner name, charger count. Stop json-server — screen shows ErrorState with Retry.

### Implementation for User Story 2

- [ ] T028 [US2] Create `OverviewPage` in `source/apps/dashboard/src/pages/Overview/OverviewPage.tsx` with loading/error/data state management
- [ ] T029 [P] [US2] Create stat card section in OverviewPage fetching `GET /api/partners`, `GET /api/stations`, `GET /api/chargers` and rendering StatCard for each with appropriate icons
- [ ] T030 [US2] Create `RecentStationsTable` in `source/apps/dashboard/src/pages/Overview/RecentStationsTable.tsx` fetching stations, enriching each with partner name and charger count via API
- [ ] T031 [US2] Add ErrorState display when API is unreachable on OverviewPage
- [ ] T032 [US2] Add EmptyState when API returns zero data

**Checkpoint**: Overview screen shows real stat counts and recent stations table with error/empty states

---

## Phase 5: User Story 3 — Admin Partner Management (Priority: P1)

**Goal**: Partner data table with full CRUD, verify, deactivate/reactivate actions

**Independent Test**: Open Partners screen. Table shows 3 seeded partners. Add Partner — new row appears. Verify PRT003 — badge flips green. Deactivate — toggle changes. Edit — modal pre-filled. Delete — confirmation then removed. Stop json-server — ErrorState shown.

### Implementation for User Story 3

- [ ] T033 [US3] Create `PartnerTable` component in `source/apps/dashboard/src/pages/Partners/PartnerTable.tsx` with DataTable columns: name, type badge, verified badge (green check / gray x), live badge, active toggle, actions (Verify, Edit, Delete)
- [ ] T034 [US3] Create `PartnerForm` component in `source/apps/dashboard/src/pages/Partners/PartnerForm.tsx` with name field and type select, validation on required fields
- [ ] T035 [US3] Create `PartnersPage` in `source/apps/dashboard/src/pages/Partners/PartnersPage.tsx` with loading/error/data state, modal management, and all CRUD operations
- [ ] T036 [US3] Implement Add Partner: modal opens PartnerForm, POST to `/api/partners` with defaults (is_verified=false, is_live=false, is_active=true), refresh table on success
- [ ] T037 [US3] Implement Verify: PATCH `/api/partners/:id` with `is_verified=true`, refresh badge; when partner has stations, note in UI that is_live will also be set
- [ ] T038 [US3] Implement Deactivate/Reactivate: PATCH `/api/partners/:id` with toggled `is_active`, refresh toggle
- [ ] T039 [US3] Implement Edit: modal opens PartnerForm pre-filled, PATCH on submit, refresh row
- [ ] T040 [US3] Implement Delete: confirmation modal, DELETE `/api/partners/:id`, remove row
- [ ] T041 [US3] Add EmptyState when no partners exist
- [ ] T042 [US3] Add ErrorState when API is unreachable

**Checkpoint**: Full partner CRUD with verify, deactivate, edit, delete — all actions update the table without page reload

---

## Phase 6: User Story 4 — Admin Station Management (Priority: P2)

**Goal**: Station data table with partner filter dropdown, CRUD, lat/lng validation

**Independent Test**: Open Stations screen. Table shows 15 stations. Filter by partner — scoped results. Add station with invalid lat — inline error. Add valid station — row appears. Edit — updates. Delete — confirmation then removed.

### Implementation for User Story 4

- [ ] T043 [US4] Create `StationTable` component in `source/apps/dashboard/src/pages/Stations/StationTable.tsx` with DataTable columns: name, address, partner name, charger count, actions
- [ ] T044 [US4] Add partner filter dropdown to StationsPage that filters by `partner_id` query param
- [ ] T045 [US4] Create `StationForm` component in `source/apps/dashboard/src/pages/Stations/StationForm.tsx` with fields: name, address, latitude, longitude, partner select
- [ ] T046 [US4] Implement lat validation (-90 to 90) and lng validation (-180 to 180) with inline field errors in StationForm
- [ ] T047 [US4] Create `StationsPage` in `source/apps/dashboard/src/pages/Stations/StationsPage.tsx` with loading/error/data state, modal management, all CRUD operations
- [ ] T048 [US4] Implement Add Station: POST to `/api/stations`, refresh table
- [ ] T049 [US4] Implement Edit Station: PATCH, refresh row
- [ ] T050 [US4] Implement Delete Station: confirmation modal, DELETE, remove row
- [ ] T051 [US4] Add EmptyState when no stations exist
- [ ] T052 [US4] Add ErrorState when API is unreachable

**Checkpoint**: Station CRUD with partner filtering and lat/lng validation working

---

## Phase 7: User Story 5 — Admin Charger Management (Priority: P2)

**Goal**: Charger data table with station filter dropdown, CRUD, status management

**Independent Test**: Open Chargers screen. Table shows 24 chargers. Filter by station — scoped results. Add charger — row appears. Edit status — badge updates. Delete — removed.

### Implementation for User Story 5

- [ ] T053 [US5] Create `ChargerTable` component in `source/apps/dashboard/src/pages/Chargers/ChargerTable.tsx` with DataTable columns: station name, connector type, power kW, status badge (color-coded), actions
- [ ] T054 [US5] Add station filter dropdown to ChargersPage that filters by `station_id` query param
- [ ] T055 [US5] Create `ChargerForm` component in `source/apps/dashboard/src/pages/Chargers/ChargerForm.tsx` with fields: station select, connector type select, power kW input, status select
- [ ] T056 [US5] Create `ChargersPage` in `source/apps/dashboard/src/pages/Chargers/ChargersPage.tsx` with loading/error/data state, modal management, all CRUD operations
- [ ] T057 [US5] Implement Add Charger: POST to `/api/chargers`, refresh table
- [ ] T058 [US5] Implement Edit Charger: PATCH, refresh row
- [ ] T059 [US5] Implement Delete Charger: confirmation modal, DELETE, remove row
- [ ] T060 [US5] Add EmptyState when no chargers exist
- [ ] T061 [US5] Add ErrorState when API is unreachable

**Checkpoint**: Charger CRUD with station filtering and status management working

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Verification, Vite configuration, and documentation

- [ ] T062 [P] Verify `vite.config.ts` has `server.proxy` configured to forward `/api` to `http://localhost:3001` (avoids CORS issues in dev)
- [ ] T063 [P] Verify all components use design tokens (Tailwind theme values) — no hardcoded colors, no hardcoded spacing values
- [ ] T064 Verify dev role switcher state resets on page reload
- [ ] T065 Run quickstart.md validation: open all 4 screens, perform CRUD on all 3 entity types, stop/start json-server to verify error states
- [ ] T066 Update this tasks file to mark all work as complete

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — no dependency on other stories
- **User Story 2 (Phase 4)**: Depends on Foundational + US1 (shell must exist to host Overview screen) — no dependency on US3/4/5
- **User Story 3 (Phase 5)**: Depends on Foundational + US1 (shell must exist) — no dependency on US2/4/5
- **User Story 4 (Phase 6)**: Depends on Foundational + US1 + US3 (partners must exist as filter source) — no dependency on US2/5
- **User Story 5 (Phase 7)**: Depends on Foundational + US1 + US4 (stations must exist as filter source) — no dependency on US2/3
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Foundational → Shell + Nav — First P1 story, no dependency on other stories
- **User Story 2 (P1)**: US1 must be complete (shell provides the page container)
- **User Story 3 (P1)**: US1 must be complete (shell provides the page container)
- **User Story 4 (P2)**: US1 + US3 must be complete (US3 provides partner data for filter)
- **User Story 5 (P2)**: US1 + US4 must be complete (US4 provides station data for filter)

### Within Each Phase

- [P] tasks within a phase can run in parallel
- Order of non-[P] tasks within a phase should be respected (forms before pages, CRUD operations in logical order)

### Parallel Opportunities

- **Phase 1**: All tasks sequential (Vite scaffolding must complete first)
- **Phase 2**: T008–T018 are all fully independent — all 11 shared components can be built in parallel
- **Phase 3**: T019–T022 are independent — T023/24/25/26/27 depend on component assembly
- **Phase 4**: T029 is parallel (stat card API calls are independent); T028/30/31/32 form the page
- **Phase 5**: T033 and T034 are parallel (table + form); T035 onwards builds the page
- **Phase 6**: T043 and T045 are parallel (table + form); T044 and T046 are parallel (filter + validation)
- **Phase 7**: T053 and T055 are parallel (table + form)
- **Phase 8**: T062 and T063 are parallel

---

## Parallel Example: User Story 1

```bash
# Launch all layout components together:
Task: "Create NavigationItem in src/components/layout/NavigationItem.tsx"
Task: "Create Sidebar in src/components/layout/Sidebar.tsx"
Task: "Create TopBar in src/components/layout/TopBar.tsx"
Task: "Create PageContent in src/components/layout/PageContent.tsx"

# Then assemble:
Task: "Create AppShell composing all layout components"
Task: "Add dev role switcher and partner selector to Sidebar"
Task: "Set up React Router with AppShell layout and placeholder pages"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (Vite project, Tailwind, env)
2. Complete Phase 2: Foundational (shared components, API client, context)
3. Complete Phase 3: User Story 1 (AppShell with navigation)
4. **STOP and VALIDATE**: Open app, see sidebar with working nav items and dev role switcher
5. Deploy/demo if ready — shell is functional even without data screens

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Shell functional → Deploy/Demo (MVP!)
3. Add User Story 2 → Overview with real stats → Deploy/Demo
4. Add User Story 3 → Partner CRUD → Deploy/Demo
5. Add User Story 4 → Station CRUD → Deploy/Demo
6. Add User Story 5 → Charger CRUD → Deploy/Demo
7. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (AppShell)
   - Developer B: User Story 2 (Overview) — depends on US1 but can prepare components
   - Developer C: User Story 3 (Partners) — depends on US1 but can prepare components
3. After US1 + US3 done:
   - Developer B: User Story 4 (Stations)
   - Developer C: User Story 5 (Chargers)
4. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- US2, US3 can be partially prepared in parallel with US1 (component files can be written without being wired into the shell)
- US4 depends on US3 (partner data for filter), US5 depends on US4 (station data for filter)
- No test framework required for Sprint 1.2 — verification is manual via browser interaction
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
