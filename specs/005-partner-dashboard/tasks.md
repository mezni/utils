# Tasks: Partner Dashboard — Multi-Tenant Views

**Input**: Design documents from `specs/005-partner-dashboard/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api.md

**Tests**: Not requested in feature specification — no test tasks generated.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Frontend**: `sources/frontend/apps/partner-dashboard/src/`
- **UI package**: `sources/frontend/packages/ui/src/`
- **Backend**: `sources/backend/src/`
- All paths below are relative to `sources/frontend/` or `sources/backend/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization — verify existing build is healthy and all needed dependencies are installed.

- [ ] T001 Verify build — run `pnpm install` then `pnpm -r build` in `sources/frontend/`; confirm partner-dashboard builds cleanly
- [ ] T002 Verify backend build — run `cargo build` in `sources/backend/`; confirm existing backend compiles

**Checkpoint**: Both frontend and backend build cleanly

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Partner dashboard app shell, auth flow, and partner context middleware — MUST be complete before ANY user story can display content.

- [ ] T003 [P] Create partner-dashboard AppShell layout in `apps/partner-dashboard/src/components/layout/app-shell.tsx` — sidebar slot on left, main content area on right with `<Outlet/>` for React Router; mirror admin portal AppShell
- [ ] T004 [P] Create SidebarNav component in `apps/partner-dashboard/src/components/layout/sidebar-nav.tsx` — 4 navigation items (Overview, Stations, Chargers, Profile) with icons, active state highlighting, and route click handlers
- [ ] T005 [P] Create partner-dashboard App.tsx in `apps/partner-dashboard/src/App.tsx` — extract from main.tsx; import and render AppShell as layout wrapper; define `<Routes>` and `<Route>` for all 4 sections with `<Navigate to="/" />` for root redirect
- [ ] T006 Create PartnerAuth middleware in `backend/src/auth/partner_middleware.rs` — extract `user_id` from JWT, look up `partner_profile_id` from `partner_profiles` table, inject into request extensions as `owner_id`; return 403 if user is not a partner
- [ ] T007 Register new auth middleware and `/api/v1/partners/me` stubs in `backend/src/main.rs` — apply PartnerAuth middleware to `/api/v1/me` and `/api/v1/stations` and `/api/v1/chargers` route scopes; add `/me` endpoints to partner module
- [ ] T007a Create auth interceptor in `apps/partner-dashboard/src/services/auth-interceptor.ts` — wrap all fetch/API calls to catch 401 responses and redirect to login page; register in App.tsx

**Checkpoint**: Partner dashboard renders with AppShell + sidebar, partner auth middleware blocks non-partner users, login flow works, 401 auto-redirects to login

---

## Phase 3: User Story 1 — Partner sees only their own data (Priority: P1) 🎯 MVP

**Goal**: All station and charger API endpoints filter by the authenticated partner's `owner_id`. Partners cannot see or access other partners' data.

**Independent Test**: Log in as partner A and partner B. Partner A's stations are invisible to partner B and vice versa. Direct URL access to another partner's station ID returns 403.

### Implementation for User Story 1

- [ ] T008 [P] [US1] Add `owner_id` filter to stations repository in `backend/src/domain/stations/repository.rs` — all list/detail queries include `WHERE owner_id = $N` when partner context is present
- [ ] T009 [P] [US1] Add `owner_id` join filter to chargers repository in `backend/src/domain/chargers/repository.rs` — all list/detail queries join through `stations` table and filter by `stations.owner_id = $N`
- [ ] T010 [US1] Update stations handlers in `backend/src/domain/stations/handlers.rs` — pass `owner_id` from request extensions to repository; return 403 when a station does not belong to the partner (PATCH/DELETE)
- [ ] T011 [US1] Update chargers handlers in `backend/src/domain/chargers/handlers.rs` — pass `owner_id` from request extensions to repository; verify station ownership on POST (station_id belongs to partner); return 403 on unauthorized access

**Checkpoint**: All data API endpoints respect owner_id scoping. Cross-partner access returns 403.

---

## Phase 4: User Story 2 — Partner manages stations (Priority: P1)

**Goal**: Partner can view, create, edit, and soft-delete their own stations. The stations page includes the BaseMap for bidirectional table↔map interaction. Owner field is auto-assigned and locked.

**Independent Test**: Partner creates a station with valid coordinates. It appears in the table and on the map. Clicking a table row pans the map. Clicking a marker highlights the row. Owner field is not editable.

### Implementation for User Story 2

- [ ] T012 [P] [US2] Create StationsTable component in `apps/partner-dashboard/src/components/stations/stations-table.tsx` — uses `<ScrollableTable/>` from @bornemap/ui with columns: ID, Name, City, Coordinates, Operational, is_test; includes "Create Station" button, edit/delete actions per row; fetches from `GET /api/v1/stations` (auto-scoped by backend); integrates with BaseMap for bidirectional interaction
- [ ] T013 [P] [US2] Create StationFormModal component in `apps/partner-dashboard/src/components/stations/station-form-modal.tsx` — modal with name, address, city, lng/lat coordinate inputs; NO owner dropdown (auto-assigned); is_operational toggle; supports create (POST /api/v1/stations) and edit (PATCH /api/v1/stations/:id)
- [ ] T014 [US2] Implement bidirectional map-table interaction — clicking a station row calls `map.flyTo(lat, lng)`; clicking a station marker on BaseMap highlights the corresponding table row; requires exposing `onStationSelect` and `highlightedStationId` props on `<StationsTable>`
- [ ] T015 [US2] Wire Stations page at `apps/partner-dashboard/src/pages/stations.tsx` — renders `<StationsTable/>` alongside an embedded `<BaseMap/>` component; delete triggers `<ConfirmDeleteModal/>` with exact `STN-` ID match; re-fetches list after mutations

**Checkpoint**: Stations CRUD fully functional — table loads scoped data, map shows partner's station markers, bidirectional table↔map interaction works, owner field locked, soft-delete works

---

## Phase 5: User Story 3 — Partner manages chargers (Priority: P1)

**Goal**: Partner can view, create, edit, and delete chargers for their own stations. Station dropdown is pre-filtered to only show partner's stations.

**Independent Test**: Partner navigates to Chargers page — sees only chargers belonging to their stations. Create charger — station dropdown shows only own stations. Create/edit/delete works. Cross-partner charger IDs return 403.

### Implementation for User Story 3

- [ ] T016 [P] [US3] Create ChargersTable component in `apps/partner-dashboard/src/components/chargers/chargers-table.tsx` — uses `<ScrollableTable/>` with columns: ID, Station, Connector Type, Power kW, Current Type, Status; status rendered as color-coded badge; fetches from `GET /api/v1/chargers` (auto-scoped); optional `stationId` prop for nested view
- [ ] T017 [P] [US3] Create ChargerFormModal component in `apps/partner-dashboard/src/components/chargers/charger-form-modal.tsx` — modal with station dropdown (filtered to partner's stations via `GET /api/v1/stations`), connector type dropdown, power_kw input, current_type select, status select; supports create (POST /api/v1/chargers) and edit (PATCH /api/v1/chargers/:id)
- [ ] T018 [US3] Wire Chargers page at `apps/partner-dashboard/src/pages/chargers.tsx` — renders `<ChargersTable/>` (flat view); delete triggers `<ConfirmDeleteModal/>` with exact `CHG-` ID match; re-fetches after mutations
- [ ] T019 [US3] Wire nested Chargers view in station detail page at `apps/partner-dashboard/src/pages/station-detail.tsx` — renders `<ChargersTable stationId={id}/>` and a `<ChargerFormModal/>` scoped to that station

**Checkpoint**: Chargers CRUD fully functional — flat list shows scoped data, nested view under station detail works, station dropdown pre-filtered, status badges display correct colors

---

## Phase 6: User Story 4 — Partner profile management (Priority: P2)

**Goal**: Partner can view and edit their own profile. Classification and tax_id are read-only.

**Independent Test**: Partner navigates to Profile page, sees all fields populated, edits display name successfully, finds that tax_id and classification fields are disabled.

### Implementation for User Story 4

- [ ] T020 [P] [US4] Create `GET /api/v1/partners/me` handler in `backend/src/domain/partners/handlers.rs` — returns authenticated partner's profile from partner_profile_id in request context
- [ ] T021 [P] [US4] Create `PATCH /api/v1/partners/me` handler in `backend/src/domain/partners/handlers.rs` — updates display_name, contact_phone, logo_url; rejects classification and tax_id changes with 400
- [ ] T022 [US4] Create ProfileForm component in `apps/partner-dashboard/src/components/profile/profile-form.tsx` — displays all fields; classification and tax_id as read-only disabled inputs; display_name, contact_phone, logo_url as editable inputs; save calls `PATCH /api/v1/partners/me`
- [ ] T023 [US4] Wire Profile page at `apps/partner-dashboard/src/pages/profile.tsx` — renders `<ProfileForm/>`; fetches profile data on mount from `GET /api/v1/partners/me`; shows loading skeleton during fetch; shows error state on failure

**Checkpoint**: Profile CRUD fully functional — display/edit own profile, classification/tax_id read-only, changes persist

---

## Phase 7: User Story 5 — Overview dashboard (Priority: P2)

**Goal**: Landing page at `/` shows metric chips with partner's total stations and total chargers, with empty state when zero infrastructure.

**Independent Test**: Partner logs in — sees Overview dashboard with their station/charger counts. Partner with zero stations sees metric chips showing 0 with empty state.

### Implementation for User Story 5

- [ ] T024 [US5] Create OverviewDashboard component in `apps/partner-dashboard/src/components/overview/overview-dashboard.tsx` — fetches station count from `GET /api/v1/stations` and charger count from `GET /api/v1/chargers`; renders MetricChip components from @bornemap/ui for total stations and total chargers; shows skeleton placeholders during loading; shows empty state when counts are zero
- [ ] T025 [US5] Wire Overview page at `apps/partner-dashboard/src/pages/overview.tsx` — renders `<OverviewDashboard/>` as the landing page at `/`

**Checkpoint**: Overview dashboard renders with correct partner-scoped metric chips, skeleton loading, and empty state

---

## Phase 8: User Story 6 — Navigation guard & admin-route blocking (Priority: P2)

**Goal**: Partner dashboard blocks access to admin-only routes. Sidebar shows exactly 4 navigation items.

**Independent Test**: Partner navigates to `/settings` or `/users` — sees 403 page or redirect to dashboard home.

### Implementation for User Story 6

- [ ] T026 [US6] Add route guard in `apps/partner-dashboard/src/App.tsx` — intercept `/users`, `/settings`, `/analytics`, `/security` routes and redirect to Overview (`/`) or show a 403 "Access Denied" page
- [ ] T027 [US6] Add error boundary wrapper in `apps/partner-dashboard/src/App.tsx` — wraps route content so a rendering error in one section doesn't crash the entire dashboard

**Checkpoint**: Admin routes blocked, error boundary prevents page-wide crashes

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Verification and cleanup across all stories

- [ ] T028 [P] Run type-check — `pnpm -r type-check` in `sources/frontend/`
- [ ] T029 [P] Run lint — `pnpm -r lint` in `sources/frontend/`
- [ ] T030 [P] Run build — `pnpm -r build` in `sources/frontend/`
- [ ] T031 Run backend tests — `cargo test` in `sources/backend/`
- [ ] T032 Run quickstart validation — execute all verification steps in `specs/005-partner-dashboard/quickstart.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 — Data Isolation (Phase 3)**: Depends on Foundational completion — BLOCKS all frontend stories (backend must be scoped first)
- **US2 — Stations (Phase 4)**: Depends on US1 completion (backend scoping required); independent of US5/US3/US4/US6
- **US3 — Chargers (Phase 5)**: Depends on US1 completion (backend scoping required); independent of US2/US4/US5/US6
- **US4 — Profile (Phase 6)**: Depends on Foundational completion (partner auth middleware); independent of US1/US2/US3/US5
- **US5 — Overview (Phase 7)**: Depends on US1 completion (backend scoping for counts); independent of US2/US3/US4
- **US6 — Navigation (Phase 8)**: Depends on Foundational completion; independent of all other stories
- **Polish (Phase 9)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: After Foundational — BLOCKS US2, US3, US5
- **User Story 2 (P1)**: After US1 — No dependencies on other stories
- **User Story 3 (P1)**: After US1 — No dependencies on other stories
- **User Story 4 (P2)**: After Foundational — No dependencies on other stories
- **User Story 5 (P2)**: After US1 — No dependencies on other stories
- **User Story 6 (P2)**: After Foundational — No dependencies on other stories

### Within Each User Story

- Parallel tasks marked [P] can be executed concurrently
- Non-[P] tasks are sequential within each phase
- Phases should complete before moving to next phase due to story dependencies

### Parallel Opportunities

- T003, T004, T005 (Foundational) can run in parallel — app-shell, sidebar, App.tsx
- T006, T007 (Foundational backend) can run in parallel — partner middleware, route registration
- T008, T009 (US1 backend) can run in parallel — stations repo filter, chargers repo filter
- T012, T013 (US2) can run in parallel — StationsTable, StationFormModal
- T016, T017 (US3) can run in parallel — ChargersTable, ChargerFormModal
- T020, T021 (US4) can run in parallel — GET /me, PATCH /me
- US2, US4, US6 can be worked on in parallel after US1 completes
- US3, US4, US6 can be worked on in parallel after US1 completes

---

## Parallel Example: Phase 2 Foundational

```bash
# Launch all Foundational tasks together:
Task: "Create partner-dashboard AppShell layout"
Task: "Create SidebarNav component"
Task: "Create partner-dashboard App.tsx"
Task: "Create PartnerAuth middleware"
Task: "Register new auth middleware and /partners/me stubs"
```

## Parallel Example: User Story 2

```bash
# Launch parallel US2 tasks together:
Task: "Create StationsTable component"
Task: "Create StationFormModal component"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (Multi-tenant data isolation)
4. **STOP and VALIDATE**: Backend scoping works — API returns only partner's data
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready (app shell + auth)
2. Add US1 (Backend scoping) → Data isolation enforced at API level
3. Add US5 (Overview) → Partner sees summary dashboard
4. Add US4 (Profile) → Partner manages profile
5. Add US2 (Stations) → Partner manages stations with map
6. Add US3 (Chargers) → Partner manages chargers
7. Add US6 (Navigation guard) → Admin routes blocked
8. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Complete Setup + Foundational together
2. Once Foundational is done and US1 is done:
   - Developer A: US2 (Stations — map integration)
   - Developer B: US3 (Chargers — dual view)
   - Developer C: US4 + US5 (Profile + Overview — simpler pages)
   - Developer D: US6 (Navigation guard — quick task)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
- All backend scoping follows the owner_id injection pattern documented in research.md
- Partner auth middleware is distinct from admin auth — partners get partner_profile_id in request context, admins do not
- Station soft-delete uses existing `deleted_at` column — no schema changes needed
- ConfirmDeleteModal imported from @bornemap/ui — no need to recreate
- BaseMap reused from admin portal — import via relative path or shared location
