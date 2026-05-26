# Tasks: Admin Data Views & CRUD

**Input**: Design documents from `specs/004-admin-data-views-crud/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api.md, quickstart.md

**Tests**: Not requested in feature specification — no test tasks generated.

**Organization**: Tasks grouped by user story for independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to
- Include exact file paths in descriptions

## Path Conventions

- Frontend source: `sources/frontend/apps/admin-portal/src/`
- UI package: `sources/frontend/packages/ui/src/`
- All paths below are relative to `sources/frontend/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization — verify existing build is healthy and all needed dependencies are installed.

- [x] T001 Verify build — run `pnpm install` then `pnpm -r build` in `sources/frontend/`; confirm admin-portal builds cleanly

**Checkpoint**: `pnpm -r build` passes with no errors

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Route structure and page shells that ALL user stories depend on.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T002 [P] Add nested routes for data section (`partners`, `stations`, `chargers`) and settings section (`infrastructure-types`, `app`) in `sources/frontend/apps/admin-portal/src/App.tsx` — `/data` redirects to `/data/partners`, `/data` has children: `partners`, `stations`, `chargers`; add `/stations/:id/chargers` route for nested charger view; `/settings` redirects to `/settings/infrastructure-types`
- [x] T003 [P] Create Data page layout at `sources/frontend/apps/admin-portal/src/pages/data.tsx` — replaces existing placeholder with sub-navigation tabs (Partners, Stations, Chargers) and an `<Outlet/>` for nested routes
- [x] T004 [P] Create Settings page layout at `sources/frontend/apps/admin-portal/src/pages/settings.tsx` — replaces existing placeholder with sub-navigation tabs (Infrastructure Types, App) and an `<Outlet/>` for nested routes

**Checkpoint**: Navigating to `/data/partners`, `/data/stations`, `/data/chargers`, `/settings/infrastructure-types`, `/settings/app` all render their respective page shells without errors

---

## Phase 3: User Story 1 — Partners Registry Management (Priority: P1) 🎯 MVP

**Goal**: Admin can view, create, edit, and soft-delete partner profiles with scrollable table, modal forms, and confirmation modal.

**Independent Test**: Navigate to Data → Partners, create a Business partner (Tax ID visible), create a Private partner (Tax ID hidden), edit both, delete one via exact `PRT-` ID confirmation. Table shows correct columns.

### Implementation for User Story 1

- [x] T005 [P] [US1] Create PartnersTable component in `sources/frontend/apps/admin-portal/src/components/data/partners-table.tsx` — uses `<ScrollableTable/>` from @bornemap/ui with columns: ID, Display Name, Classification, Tax ID, Contact Phone, Created; includes "Create Partner" button, edit/delete actions per row; fetches from `GET /api/v1/partners`; shows empty state when no results
- [x] T006 [P] [US1] Create PartnerFormModal component in `sources/frontend/apps/admin-portal/src/components/data/partner-form-modal.tsx` — modal with user fields (email, password, name) and partner profile fields (display_name, classification toggle, tax_id, contact_phone); classification toggle conditionally shows/hides tax_id field; supports create (bundled POST to /api/v1/partners with user fields, or two sequential calls: POST /api/v1/users then POST /api/v1/partners) and edit (PATCH /api/v1/partners/:id) modes; inline validation errors from API
- [x] T007 [US1] Wire Partners page at `sources/frontend/apps/admin-portal/src/pages/data/partners.tsx` — renders `<PartnersTable/>`; delete triggers `<ConfirmDeleteModal/>` from @bornemap/ui requiring exact `PRT-` ID match; re-fetches list after create/edit/delete

**Checkpoint**: Partners CRUD fully functional — table loads, modal create/edit works, delete requires exact ID match, empty state shown when no partners

---

## Phase 4: User Story 2 — Stations Management (Priority: P1)

**Goal**: Admin can view, create, edit, and soft-delete stations with scrollable table, map interaction, and confirmation modal.

**Independent Test**: Navigate to Data → Stations, create stations with different coordinates and owners, verify they appear on map. Click table row → map pans to station. Click marker → row highlights. Delete via `STN-` ID confirmation.

### Implementation for User Story 2

- [x] T008 [P] [US2] Create StationsTable component in `sources/frontend/apps/admin-portal/src/components/data/stations-table.tsx` — uses `<ScrollableTable/>` with columns: ID, Name, City, Owner, Coordinates, Operational, is_test; includes create/edit/delete actions; fetches from `GET /api/v1/stations`; owner column displays partner display_name; empty state when no results
- [x] T009 [P] [US2] Create StationFormModal component in `sources/frontend/apps/admin-portal/src/components/data/station-form-modal.tsx` — modal with name, address, city, lng/lat coordinate inputs, owner dropdown (populated from `GET /api/v1/partners`), is_operational toggle; supports create (POST /api/v1/stations) and edit (PATCH /api/v1/stations/:id) modes; validates owner selected before submission
- [x] T010 [US2] Implement bidirectional map-table interaction — clicking a station row calls `map.flyTo(lat, lng)` via `useMap()` hook from react-leaflet; clicking a station marker on BaseMap scrolls the table row into view and applies a highlight background class; requires exposing `onStationSelect` and `highlightedStationId` props on `<StationsTable>`
- [x] T011 [US2] Wire Stations page at `sources/frontend/apps/admin-portal/src/pages/data/stations.tsx` — renders `<StationsTable/>` alongside an embedded `<BaseMap/>` component; delete triggers `<ConfirmDeleteModal/>` with exact `STN-` ID match; re-fetches list after mutations

**Checkpoint**: Stations CRUD fully functional — table loads, map shows station markers, bidirectional table↔map interaction works, delete requires exact ID match

---

## Phase 5: User Story 3 — Chargers Management (Priority: P1)

**Goal**: Admin can view chargers in both flat and nested views, create/edit/delete chargers with status badges and connector type dropdown.

**Independent Test**: Navigate to `/data/chargers` — see all chargers with status badge colors, filter by station. Navigate to a station detail page — see only that station's chargers. Create, edit, and delete chargers via modal.

### Implementation for User Story 3

- [x] T012 [P] [US3] Create ChargersTable component in `sources/frontend/apps/admin-portal/src/components/data/chargers-table.tsx` — uses `<ScrollableTable/>` with columns: ID, Station, Connector Type, Power kW, Current Type, Status; status rendered as color-coded badge (green/amber/red/gray); accepts optional `stationId` prop for nested view; when `stationId` is absent, includes station filter `<SelectSetting/>` dropdown; fetches from `GET /api/v1/chargers` or `GET /api/v1/stations/:id/chargers` depending on `stationId` prop
- [x] T013 [P] [US3] Create ChargerFormModal component in `sources/frontend/apps/admin-portal/src/components/data/charger-form-modal.tsx` — modal with station dropdown (hidden when nested), connector type dropdown (populated from `GET /api/v1/connector-types` on open), power_kw input, current_type select (AC/DC), status select; supports create (POST) and edit (PATCH); re-fetches connector types each time modal opens per research.md
- [x] T014 [US3] Wire flat Chargers page at `sources/frontend/apps/admin-portal/src/pages/data/chargers.tsx` — renders `<ChargersTable/>` without `stationId` prop (flat view); delete triggers `<ConfirmDeleteModal/>` with exact `CHG-` ID match; re-fetches after mutations
- [x] T015 [US3] Wire nested Chargers view as part of station detail page — create a dedicated `StationDetailPage` at `sources/frontend/apps/admin-portal/src/pages/data/station-detail.tsx` that renders `<ChargersTable stationId={id}/>` and a `<ChargerFormModal/>` scoped to that station

**Checkpoint**: Chargers CRUD fully functional — flat list with filter, nested view under station, status badges display correct colors, connector type dropdown populated dynamically, hard delete works

---

## Phase 6: User Story 4 — Infrastructure Types Management (Priority: P2)

**Goal**: Admin can manage connector types in Settings with delete-restrict check. New types appear in Chargers dropdown immediately.

**Independent Test**: Navigate to Settings → Infrastructure Types, create a new type, verify it appears in Chargers dropdown. Delete an unused type. Attempt to delete a type in use — see error.

### Implementation for User Story 4

- [x] T016 [P] [US4] Create ConnectorTypesTable component in `sources/frontend/apps/admin-portal/src/components/data/connector-types-table.tsx` — uses `<ScrollableTable/>` with columns: ID, Name, Description, Created; create/edit/delete actions; fetches from `GET /api/v1/connector-types`
- [x] T017 [P] [US4] Create ConnectorTypeFormModal component in `sources/frontend/apps/admin-portal/src/components/data/connector-type-form-modal.tsx` — modal with name (required, unique) and description (optional) fields; supports create (POST) and edit (PATCH)
- [x] T018 [US4] Wire Infrastructure Types page at `sources/frontend/apps/admin-portal/src/pages/settings/infrastructure-types.tsx` — renders `<ConnectorTypesTable/>`; delete triggers `<ConfirmDeleteModal/>` with exact `CNT-` ID match; if API returns 409 Conflict on delete, show error message instead of proceeding; re-fetches after mutations

**Checkpoint**: Connector types CRUD fully functional — delete blocked with error when type in use, new types available in Chargers dropdown immediately

---

## Phase 7: User Story 5 — App Settings Placeholder (Priority: P3)

**Goal**: Admin sees three placeholder cards in Settings → App for future configuration.

**Independent Test**: Navigate to Settings → App, verify three placeholder cards render without errors or functional actions.

### Implementation for User Story 5

- [x] T019 [US5] Create App Settings placeholder page at `sources/frontend/apps/admin-portal/src/pages/settings/app.tsx` — renders three `<SettingsCard/>` from @bornemap/ui with titles: "Branding" (logo, colors, favicon — coming soon), "Map Tokens" (map provider API keys — coming soon), "Dropzones" (file upload targets — coming soon); cards display dashed border styling with "coming in a future release" message; no functional actions

**Checkpoint**: Placeholder page renders three non-interactive cards

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Verification and cleanup across all stories

- [x] T020 Run type-check — `pnpm -r type-check` in `sources/frontend/`
- [x] T021 Run lint — `pnpm -r lint` in `sources/frontend/`
- [x] T022 Run build — `pnpm -r build` in `sources/frontend/`
- [ ] T023 Run quickstart validation — execute all verification steps in `specs/004-admin-data-views-crud/quickstart.md` — execute all verification steps in `specs/004-admin-data-views-crud/quickstart.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 — Partners (Phase 3)**: Depends on Foundational completion
- **US2 — Stations (Phase 4)**: Depends on Foundational completion; independent of US1
- **US3 — Chargers (Phase 5)**: Depends on Foundational completion; independent of US1/US2
- **US4 — Infrastructure Types (Phase 6)**: Depends on Foundational completion; independent of US1/US2/US3
- **US5 — App Placeholder (Phase 7)**: Depends on Foundational completion; independent of all other stories
- **Polish (Phase 8)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: After Foundational — No dependencies on other stories
- **User Story 2 (P1)**: After Foundational — No dependencies on other stories
- **User Story 3 (P1)**: After Foundational — No dependencies on other stories
- **User Story 4 (P2)**: After Foundational — No dependencies on other stories
- **User Story 5 (P3)**: After Foundational — No dependencies on other stories

### Within Each Phase

- Parallel tasks marked [P] can be executed concurrently
- Non-[P] tasks are sequential within each phase
- Phases must complete in order

### Parallel Opportunities

- T002, T003, T004 (Foundational) can run in parallel — different files, no cross-dependencies
- T005, T006 (US1) can run in parallel
- T008, T009 (US2) can run in parallel
- T012, T013 (US3) can run in parallel
- T016, T017 (US4) can run in parallel
- All user stories (US1-US5) can be worked on in parallel after Foundational completes

---

## Parallel Example: Phase 2 Foundational

```bash
# Launch all Foundational tasks together:
Task: "Add nested routes for data and settings sections in App.tsx"
Task: "Create Data page layout with sub-navigation tabs"
Task: "Create Settings page layout with sub-navigation tabs"
```

## Parallel Example: User Story 1

```bash
# Launch parallel US1 tasks together:
Task: "Create PartnersTable component"
Task: "Create PartnerFormModal component"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (Partners Registry CRUD)
4. **STOP and VALIDATE**: Partners CRUD works independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Routes and page shells ready
2. Add US1 (Partners) → CRUD for foundational business entity
3. Add US2 (Stations) → Map-integrated station management
4. Add US3 (Chargers) → Charger status management with dual views
5. Add US4 (Infrastructure Types) → Configuration management
6. Add US5 (App Placeholder) → Future extension points
7. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Complete Setup + Foundational together
2. Once Foundational is done:
   - Developer A: US1 + US4 (Partners + Connector Types — both table+modal patterns)
   - Developer B: US2 (Stations — requires map integration expertise)
   - Developer C: US3 (Chargers — dual view complexity)
3. Stories complete and integrate independently (no cross-story dependencies)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
- All modal forms follow the pattern established in research.md: overlay on same page, refetch list on close
- ConfirmDeleteModal imported from @bornemap/ui — no need to recreate
- Status badge colors use Tailwind design tokens (bg-green-500, bg-amber-500, bg-red-500, bg-gray-500)
