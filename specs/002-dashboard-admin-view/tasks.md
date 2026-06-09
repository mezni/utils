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

- [X] T001 Create `source/apps/dashboard/` by running `pnpm create vite` with React + TypeScript template
- [X] T002 Install dependencies: `react-router-dom`, `tailwindcss`, `postcss`, `autoprefixer` in `source/apps/dashboard/`
- [X] T003 Configure Tailwind to extend `source/packages/ui/tailwind.config.base.js` as a preset in `source/apps/dashboard/tailwind.config.js`
- [X] T004 Create `postcss.config.js` in `source/apps/dashboard/` with Tailwind and autoprefixer plugins
- [X] T005 Add `@tailwind base; @tailwind components; @tailwind utilities;` directives in `source/apps/dashboard/src/index.css`
- [X] T006 Set up `VITE_API_BASE_URL=http://localhost:3001` in `source/apps/dashboard/.env`
- [X] T007 Add `dev:dashboard` script (`pnpm --filter @borne-map/dashboard dev`) to root `package.json`

**Checkpoint**: `pnpm dev:dashboard` starts Vite dev server with Tailwind processing active

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared infrastructure that MUST be complete before any user story can be implemented

- [X] T008 [P] Create fetch wrapper in `source/apps/dashboard/src/api/client.ts` with `/api` prefix, error handling, and `fetchWithError` helper
- [X] T009 [P] Create `RoleContext` provider in `source/apps/dashboard/src/context/RoleContext.tsx` storing `role` (admin|partner) and `selectedPartnerId`
- [X] T010 [P] Create `StatCard` component in `source/apps/dashboard/src/components/shared/StatCard.tsx` with label, value, icon props
- [X] T011 [P] Create `DataTable` component in `source/apps/dashboard/src/components/shared/DataTable.tsx` with configurable columns, render functions, and actions column
- [X] T012 [P] Create `StatusBadge` component in `source/apps/dashboard/src/components/shared/StatusBadge.tsx` with color-coded variants
- [X] T013 [P] Create `Modal` component in `source/apps/dashboard/src/components/shared/Modal.tsx` with `isOpen`, `onClose`, title, children props
- [X] T014 [P] Create `EmptyState` component in `source/apps/dashboard/src/components/shared/EmptyState.tsx` with icon, message, action button props
- [X] T015 [P] Create `ErrorState` component in `source/apps/dashboard/src/components/shared/ErrorState.tsx` with error message and Retry button
- [X] T016 [P] Create `Skeleton` component in `source/apps/dashboard/src/components/shared/Skeleton.tsx` for loading states
- [X] T017 [P] Create `Button` component in `source/apps/dashboard/src/components/shared/Button.tsx` with variants (primary, secondary, danger, ghost)
- [X] T018 [P] Create `Input` component in `source/apps/dashboard/src/components/shared/Input.tsx` with label, error state, and validation styling

**Checkpoint**: Shared components render correctly — foundation ready for all user stories

---

## Phase 3: User Story 1 — Dashboard Shell and Navigation (Priority: P1) 🎯 MVP

**Goal**: AppShell with fixed sidebar, top bar, React Router, and dev role switcher

**Independent Test**: Open Dashboard App. Left sidebar shows four navigation items with first highlighted. Toggle dev role switcher — navigation changes and partner selector appears. Refresh — state resets.

### Implementation for User Story 1

- [X] T019 [P] [US1] Create `NavigationItem` component in `source/apps/dashboard/src/components/layout/NavigationItem.tsx`
- [X] T020 [P] [US1] Create `Sidebar` component in `source/apps/dashboard/src/components/layout/Sidebar.tsx` with brand header, nav items, dev switcher
- [X] T021 [P] [US1] Create `TopBar` component in `source/apps/dashboard/src/components/layout/TopBar.tsx`
- [X] T022 [P] [US1] Create `PageContent` component in `source/apps/dashboard/src/components/layout/PageContent.tsx`
- [X] T023 [US1] Create `AppShell` in `source/apps/dashboard/src/components/layout/AppShell.tsx`
- [X] T024 [US1] Dev role switcher in Sidebar labeled "Dev Only — removed in MVP-3"
- [X] T025 [US1] Partner selector dropdown in Sidebar (visible only in Partner View)
- [X] T026 [US1] React Router with layout route and nested index routes in `source/apps/dashboard/src/App.tsx`
- [X] T027 [US1] Update `source/apps/dashboard/src/main.tsx` with BrowserRouter and RoleProvider

**Checkpoint**: AppShell renders with working sidebar, nav, dev role switcher, and all routes

---

## Phase 4: User Story 2 — Admin Overview Screen (Priority: P1)

**Goal**: Three stat cards (total partners, stations, chargers) and recent stations table

**Independent Test**: Navigate to Overview. Stat cards show real counts. Stations table loads. Stop json-server — ErrorState with Retry shown.

### Implementation for User Story 2

- [X] T028 [US2] Create `OverviewPage` in `source/apps/dashboard/src/pages/Overview/OverviewPage.tsx`
- [X] T029 [P] [US2] Stat card section fetching partners, stations, chargers counts
- [X] T030 [US2] `RecentStationsTable` with partner name and charger count enrichment
- [X] T031 [US2] ErrorState for API unreachable
- [X] T032 [US2] EmptyState for zero data

**Checkpoint**: Overview shows real stat counts and stations table

---

## Phase 5: User Story 3 — Admin Partner Management (Priority: P1)

**Goal**: Partner data table with full CRUD, verify, deactivate/reactivate

**Independent Test**: Partners screen shows 3 seeded partners. Add Partner works. Verify flips badge. Deactivate toggles active. Edit modal pre-filled. Delete with confirmation. ErrorState when API down.

### Implementation for User Story 3

- [X] T033 [US3] `PartnerTable` in `source/apps/dashboard/src/pages/Partners/PartnerTable.tsx`
- [X] T034 [US3] `PartnerForm` in `source/apps/dashboard/src/pages/Partners/PartnerForm.tsx`
- [X] T035 [US3] `PartnersPage` in `source/apps/dashboard/src/pages/Partners/PartnersPage.tsx`
- [X] T036 [US3] Add Partner with POST, defaults, table refresh
- [X] T037 [US3] Verify with PATCH is_verified=true
- [X] T038 [US3] Deactivate/Reactivate with PATCH is_active toggle
- [X] T039 [US3] Edit with PATCH, pre-filled form
- [X] T040 [US3] Delete with confirmation modal, DELETE, remove row
- [X] T041 [US3] EmptyState for no partners
- [X] T042 [US3] ErrorState for API down

**Checkpoint**: Full partner CRUD with verify, deactivate, edit, delete

---

## Phase 6: User Story 4 — Admin Station Management (Priority: P2)

**Goal**: Station data table with partner filter, CRUD, lat/lng validation

**Independent Test**: 15 stations in table. Filter by partner. Invalid lat shows inline error. Add/Edit/Delete work.

### Implementation for User Story 4

- [X] T043 [US4] `StationTable` in `source/apps/dashboard/src/pages/Stations/StationTable.tsx`
- [X] T044 [US4] Partner filter dropdown with query param
- [X] T045 [US4] `StationForm` with name, address, lat, lng, partner select
- [X] T046 [US4] Lat (-90 to 90) and lng (-180 to 180) validation with inline errors
- [X] T047 [US4] `StationsPage` in `source/apps/dashboard/src/pages/Stations/StationsPage.tsx`
- [X] T048 [US4] Add Station POST
- [X] T049 [US4] Edit Station PATCH
- [X] T050 [US4] Delete Station with confirmation
- [X] T051 [US4] EmptyState
- [X] T052 [US4] ErrorState

**Checkpoint**: Station CRUD with filtering and validation

---

## Phase 7: User Story 5 — Admin Charger Management (Priority: P2)

**Goal**: Charger data table with station filter, CRUD, status badges

**Independent Test**: 24 chargers in table. Filter by station. Add/Edit/Delete work. Status badge color-coded.

### Implementation for User Story 5

- [X] T053 [US5] `ChargerTable` in `source/apps/dashboard/src/pages/Chargers/ChargerTable.tsx`
- [X] T054 [US5] Station filter dropdown
- [X] T055 [US5] `ChargerForm` with station, connector type, power, status
- [X] T056 [US5] `ChargersPage` in `source/apps/dashboard/src/pages/Chargers/ChargersPage.tsx`
- [X] T057 [US5] Add Charger POST
- [X] T058 [US5] Edit Charger PATCH
- [X] T059 [US5] Delete Charger with confirmation
- [X] T060 [US5] EmptyState
- [X] T061 [US5] ErrorState

**Checkpoint**: Charger CRUD with filtering and status management

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Verification, proxy config, token audit, and documentation

- [X] T062 [P] Verify `vite.config.ts` has `server.proxy` for `/api` → `http://localhost:3001`
- [X] T063 [P] Verify all components use design tokens — no hardcoded colors/spacing
- [X] T064 Verify dev role switcher state resets on page reload
- [X] T065 Run validation: all API endpoints verified (partners, stations, chargers, filter queries, CRUD)
- [X] T066 Update this tasks file to mark all work as complete

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational
- **User Story 2 (Phase 4)**: Depends on Foundational + US1
- **User Story 3 (Phase 5)**: Depends on Foundational + US1
- **User Story 4 (Phase 6)**: Depends on Foundational + US1 + US3
- **User Story 5 (Phase 7)**: Depends on Foundational + US1 + US4
- **Polish (Phase 8)**: Depends on all user stories

### Parallel Opportunities

- **Phase 2**: All 11 foundational components fully parallel
- **Phase 3**: T019-T022 parallel (layout components)
- **Phase 4**: T029 parallel (stat card API calls)
- **Phase 5**: T033 and T034 parallel (table + form)
- **Phase 6**: T043 and T045 parallel; T044 and T046 parallel
- **Phase 7**: T053 and T055 parallel
- **Phase 8**: T062 and T063 parallel
