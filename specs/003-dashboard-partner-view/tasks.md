# Tasks: Dashboard Partner View

**Input**: Design documents from `/specs/003-dashboard-partner-view/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api-contract.md

**Tests**: Not requested — manual verification only.

**Organization**: Tasks grouped by user story for independent implementation.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: No infrastructure changes needed — the Dashboard App is already created and configured.

- [X] T001 No setup tasks required — project exists at `source/apps/dashboard/` with all dependencies installed

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Register partner routes in the router so partner pages can be navigated to.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T002 [P] Register partner routes in `source/apps/dashboard/src/App.tsx` — add Route elements for `/my-stations`, `/my-chargers`, `/availability` alongside the existing admin routes

**Checkpoint**: Foundation ready — partner pages are routable

---

## Phase 3: User Story 1 — Partner Overview Screen (Priority: P1) 🎯 MVP

**Goal**: A partner sees three stat cards (own stations, own chargers, available chargers), a status bar with partner flags, and a table of own stations.

**Independent Test**: Set dev role switcher to Partner View, select PRT001. Overview loads stat cards with PRT001's counts. Status bar shows Verified, Live, Active in green. Stations table shows only PRT001's stations. Switch to PRT002 — counts and status bar update.

### Implementation for User Story 1

- [X] T003 [P] [US1] Create PartnerOverview directory at `source/apps/dashboard/src/pages/PartnerOverview/`
- [X] T004 [P] [US1] Create `PartnerOverviewPage.tsx` — fetch partner data via `get<Partner>('partners', selectedPartnerId)`, stations via `list<Station>('stations', { partner_id })`, chargers via collecting station IDs then `list<Charger>('chargers?' + qs)`, and station_availability records for latest-per-station computation
- [X] T005 [P] [US1] Fetch partner data for status bar — `get<Partner>('partners', selectedPartnerId)` returns is_verified, is_live, is_active flags
- [X] T006 [P] [US1] Render three StatCards in `PartnerOverviewPage.tsx`: own stations count, own chargers count, available chargers count
- [X] T007 [US1] Render status bar with three Badge groups in `PartnerOverviewPage.tsx`: Verified/Awaiting (green/gray), Live/Not Live (green/gray), Active/Suspended (green/red) — driven by partner.is_verified, partner.is_live, partner.is_active
- [X] T008 [US1] Render DataTable of own stations in `PartnerOverviewPage.tsx` with columns: name, charger count (computed from filtered charger list), current availability (latest station_availability per station)
- [X] T009 [US1] Add loading state (`<Skeleton>`) and error state (`<ErrorState>` with retry) to `PartnerOverviewPage.tsx`
- [X] T010 [US1] Add no-partner-selected prompt — when `selectedPartnerId` is null, show a message to select a partner from the dropdown
- [X] T011 [US1] Wire `PartnerOverviewPage` to the root `/` route in App.tsx partner section via conditional rendering on role

**Checkpoint**: Partner Overview fully functional — stat cards, status bar, stations table — all scoped to selected partner

---

## Phase 4: User Story 2 — Partner My Stations Screen (Priority: P1)

**Goal**: A partner sees only their own stations, can add/edit/delete with partner_id locked to their own ID.

**Independent Test**: Open My Stations with PRT001. Table shows PRT001's stations only. Add a station — partner_id locked to PRT001. Edit a station — saves. Delete with confirmation. Switch to PRT002 — different station set.

### Implementation for User Story 2

- [X] T012 [P] [US2] Create PartnerStations directory at `source/apps/dashboard/src/pages/PartnerStations/`
- [X] T013 [P] [US2] Create `PartnerStationsPage.tsx` — fetch stations via `list<Station>('stations', { partner_id })`, wire CRUD create/update/remove from `api/client`
- [X] T014 [P] [US2] Implement Add Station modal — form fields match Station entity (name, address, lat, lng), partner_id field pre-filled and locked (read-only disabled input) from role context
- [X] T015 [P] [US2] Implement Edit Station modal — pre-fill form from existing station data, partner_id is locked/read-only
- [X] T016 [US2] Implement Delete Station with confirmation modal
- [X] T017 [US2] Add loading, error (with retry), and empty states to `PartnerStationsPage.tsx` — EmptyState for zero records with "Add your first station" prompt
- [X] T018 [US2] Add no-partner-selected prompt — when `selectedPartnerId` is null, show select-partner message

**Checkpoint**: Partner Stations CRUD fully functional — all operations scoped to selected partner

---

## Phase 5: User Story 3 — Partner My Chargers Screen (Priority: P2)

**Goal**: A partner sees chargers belonging to their stations, filtered by own stations, CRUD with scoped station selection.

**Independent Test**: Open My Chargers with PRT001. Table shows chargers belonging to PRT001's stations. Station filter shows PRT001's stations. Add charger — station select shows PRT001 stations. Edit/Delete work. Switch to PRT002 — different charger set.

### Implementation for User Story 3

- [X] T019 [P] [US3] Create PartnerChargers directory at `source/apps/dashboard/src/pages/PartnerChargers/`
- [X] T020 [P] [US3] Create `PartnerChargersPage.tsx` — fetch partner's stations first via `list<Station>('stations', { partner_id })`, then fetch chargers via `list<Charger>('chargers?' + qs)`
- [X] T021 [P] [US3] Implement station filter dropdown in `PartnerChargersPage.tsx` — options populated from partner's own stations list (hidden when only 1 station)
- [X] T022 [P] [US3] Implement Add Charger modal — station select dropdown shows only partner's own stations, form fields match Charger entity
- [X] T023 [US3] Implement Edit Charger modal — pre-filled from existing charger data, station_id field shows partner's stations only
- [X] T024 [US3] Implement Delete Charger with confirmation modal
- [X] T025 [US3] Add loading, error (with retry), and empty states to `PartnerChargersPage.tsx`
- [X] T026 [US3] Add no-partner-selected prompt

**Checkpoint**: Partner Chargers CRUD fully functional — all scoped to selected partner's stations

---

## Phase 6: User Story 4 — Partner Availability Screen (Priority: P2)

**Goal**: A partner sees own stations with current availability and can toggle between Available/Partial/Unavailable.

**Independent Test**: Open Availability with PRT001. Table shows PRT001's stations with current availability. Toggle a station from Available to Unavailable — status updates immediately. Fetch station_availability confirms new record. Toggling same status does nothing.

### Implementation for User Story 4

- [X] T027 [P] [US4] Create PartnerAvailability directory at `source/apps/dashboard/src/pages/PartnerAvailability/`
- [X] T028 [P] [US4] Create `PartnerAvailabilityPage.tsx` — fetch partner's stations via `list<Station>('stations', { partner_id })`, fetch station_availability via `list<StationAvailability>('station_availability?' + qs)`, compute latest-per-station status client-side
- [X] T029 [P] [US4] Render DataTable with columns: station name, current availability status (computed from latest record, default "Unknown" if no records), three-option toggle buttons (Available / Partial / Unavailable)
- [X] T030 [US4] Implement toggle click handler — if clicked status matches current status, no-op (FR-015); otherwise POST to `station_availability` with `{ station_id, status, updated_by, updated_at }` via `create`, then refetch
- [X] T031 [US4] Implement pessimistic update UX — disable all three toggle buttons during POST, refetch on success/failure, show error banner on failure
- [X] T032 [US4] Add loading, error (with retry), and empty states to `PartnerAvailabilityPage.tsx`
- [X] T033 [US4] Add no-partner-selected prompt

**Checkpoint**: Availability management fully functional — partner can update any station's availability with one click

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Verify all screens, clean up, and validate against requirements.

- [X] T034 [P] Run `tsc --noEmit` to check TypeScript compilation across all new files — **PASS (0 errors)**
- [ ] T035 [P] Run `pnpm dev:dashboard` and verify all four partner screens against quickstart.md scenarios
- [ ] T036 [P] Verify partner data doesn't leak between partner switches — select PRT001, confirm data, switch to PRT002, confirm different data, switch back to PRT001
- [ ] T037 [P] Verify error recovery — stop json-server, confirm ErrorState with Retry on all screens, restart and click Retry
- [ ] T038 [P] Verify no-partner-selected prompt on all screens — clear selectedPartnerId, confirm each screen shows select-partner message
- [ ] T039 [P] Verify availability toggle idempotency — clicking same status produces no POST (check Network tab)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **User Stories (Phase 3-6)**: All depend on Foundational (Phase 2)
  - US1, US2, US3, US4 can proceed in parallel if staffed
  - Sequential priority order: US1 → US2 → US3 → US4
- **Polish (Phase 7)**: Depends on all desired user stories complete

### User Story Dependencies

- **US1 (P1)**: Can start after Phase 2 — no dependencies on other stories
- **US2 (P1)**: Can start after Phase 2 — no dependencies on other stories
- **US3 (P2)**: Can start after Phase 2 — no dependencies on US2's CRUD code; shares the "fetch stations by partner_id" pattern but reimplements it independently
- **US4 (P2)**: Can start after Phase 2 — no dependencies on other stories; fetches own data independently

### Within Each User Story

- Models (interfaces/types) before state management
- Data fetching before rendering
- Core implementation before error/loading states
- Story complete before moving to next priority

### Parallel Opportunities

- All Phase 2 foundational tasks marked [P] run in parallel
- All tasks within a user story phase marked [P] run in parallel
- US1, US2, US3, US4 can be worked on in parallel by different developers (no file conflicts — each story has its own directory)

---

## Parallel Example: User Story 1

```bash
# Create directory and page component together:
Task: "Create PartnerOverview directory"
Task: "Create PartnerOverviewPage.tsx with data fetching"

# Data fetching tasks:
Task: "Fetch partner data for status bar"
Task: "Render three StatCards"
```

## Parallel Example: Full Sprint

```bash
# Developer A: Phase 2 (routes) + US1 (Overview)
Task: "Register partner routes in App.tsx"
Task: "Create PartnerOverviewPage.tsx — full implementation"

# Developer B: US2 (My Stations)
Task: "Create PartnerStationsPage.tsx — full CRUD implementation"

# Developer C: US3 (My Chargers)
Task: "Create PartnerChargersPage.tsx — full CRUD implementation"

# Developer D: US4 (Availability)
Task: "Create PartnerAvailabilityPage.tsx — full toggle implementation"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 2: Foundational (register routes)
2. Complete Phase 3: User Story 1 (Partner Overview)
3. **STOP and VALIDATE**: Verify Overview with quickstart.md scenarios
4. Demo-ready after US1 alone

### Incremental Delivery

1. Phase 2 → Foundation ready (routes registered, pages return blank)
2. US1 (Overview) → Partner can see their data at a glance → Deploy/Demo (MVP!)
3. US2 (My Stations) → Partner can manage stations → Deploy/Demo
4. US3 (My Chargers) → Partner can manage chargers → Deploy/Demo
5. US4 (Availability) → Partner can toggle availability → Deploy/Demo
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With 4 developers:
1. One developer: Phase 2 routes + US1
2. Second developer: US2 (fully independent)
3. Third developer: US3 (fully independent — reimplements station fetch)
4. Fourth developer: US4 (fully independent — reimplements station fetch)

### Single Developer Strategy

1. Phase 2 → US1 → US2 → US3 → US4 → Polish
2. Each US is self-contained with no file conflicts

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently testable
- No existing files are modified except `App.tsx` (Phase 2 route registration)
- Commit after each user story phase
- Stop at any checkpoint to validate story independently
