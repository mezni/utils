# Tasks: Frontend Apps Scaffold

**Input**: Design documents from `/specs/005-frontend-apps-scaffold/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Not requested — no test tasks included.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Web apps** (driver-web, dashboard): `apps/<app>/src/`
- **Mobile app** (driver-mobile): `apps/driver-mobile/app/`, `apps/driver-mobile/components/`
- **Shared packages**: `packages/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Install dependencies and bootstrap missing project scaffolding

- [X] T001 Install driver-web dependencies (tailwindcss, postcss, autoprefixer, leaflet, react-leaflet, @types/leaflet)
- [X] T002 [P] Install dashboard dependencies (tailwindcss, postcss, autoprefixer)
- [X] T003 [P] Create driver-mobile Expo scaffold: app/, components/, hooks/, services/ directories, app.json, tsconfig.json
- [X] T004 [P] Create Tailwind CSS config for driver-web in apps/driver-web/tailwind.config.js and apps/driver-web/postcss.config.js
- [X] T005 [P] Create Tailwind CSS config for dashboard in apps/dashboard/tailwind.config.js and apps/dashboard/postcss.config.js
- [X] T006 [P] Add Tailwind CSS directives to driver-web entry CSS in apps/driver-web/src/index.css
- [X] T007 [P] Add Tailwind CSS directives to dashboard entry CSS in apps/dashboard/src/index.css
- [X] T008 [P] Update driver-mobile package.json scripts to include `"tsc": "tsc --noEmit"` for CI compatibility

**Checkpoint**: All three apps have their dependency trees installed and can build or typecheck.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared types and configuration that all user stories depend on

- [X] T009 Create shared Station interface type file at apps/driver-web/src/types/station.ts (used by US1, US2)
- [X] T010 [P] Configure Vite proxy for driver-web in apps/driver-web/vite.config.ts — forward /api/v1 to http://localhost:3001
- [X] T011 [P] Configure Vite proxy for dashboard in apps/dashboard/vite.config.ts — forward /api/v1 to http://localhost:3002

**Checkpoint**: Foundation ready — all three user stories can now be implemented in parallel.

---

## Phase 3: User Story 1 — Driver Web Map Browsing (Priority: P1) 🎯 MVP

**Goal**: A driver opens the BorneMap website and sees a Leaflet map with station markers from the real driver-service API. Clicking a marker shows station name, available charger count, and distance.

**Independent Test**: Open localhost:5173 — Leaflet map renders with OSM tiles, centered on Tunisia (34.0, 9.0) zoom 7, markers appear from API, click shows popup with station info.

### Implementation for User Story 1

- [X] T012 [P] [US1] Create API client in apps/driver-web/src/services/api.ts — fetch wrapper for GET /api/v1/stations/nearby with lat/lng/radius_km params
- [X] T013 [P] [US1] Create useStations hook in apps/driver-web/src/hooks/useStations.ts — fetches stations on mount, returns loading/error/stations state
- [X] T014 [US1] Create StationMap component in apps/driver-web/src/components/StationMap.tsx — MapContainer with TileLayer (OSM), markers from useStations, Popup with station name, available chargers, distance calculation
- [X] T015 [US1] Update App.tsx in apps/driver-web/src/App.tsx — render StationMap and handle full-viewport layout
- [X] T016 [US1] Add error state to StationMap — show fallback message "Unable to load stations" when API is unreachable, map still renders

**Checkpoint**: Driver Web shows map with markers from real API. Marker click shows station name, charger count, distance.

---

## Phase 4: User Story 2 — Driver Mobile Map with Location (Priority: P1)

**Goal**: A driver opens the mobile app and sees a MapView with station markers from the API and their current location (or Tunis default if permission denied).

**Independent Test**: Open Driver Mobile app — MapView renders with markers, location permission prompt appears (or defaults to 36.8065, 10.1815).

### Implementation for User Story 2

- [X] T017 [P] [US2] Create API client in apps/driver-mobile/services/api.ts — fetch wrapper for GET /api/v1/stations/nearby targeting driver-service host
- [X] T018 [P] [US2] Create useLocation hook in apps/driver-mobile/hooks/useLocation.ts — requestForegroundPermissionsAsync → getCurrentPositionAsync or fallback to 36.8065/10.1815
- [X] T019 [US2] Create StationMarker component in apps/driver-mobile/components/StationMarker.tsx — Marker with Callout showing station name and available charger count
- [X] T020 [US2] Create index screen in apps/driver-mobile/app/index.tsx — MapView with markers from API, region centered on user location or Tunis default, StationMarker child components
- [X] T021 [US2] Configure app.json in apps/driver-mobile/app.json — set up expo-router scheme, iOS/Android location permission descriptions (English)

**Checkpoint**: Driver Mobile shows MapView with markers and location handling.

---

## Phase 5: User Story 3 — Dashboard Navigation (Priority: P2)

**Goal**: An admin opens the Dashboard and sees a left sidebar with four navigation items (Overview, Partners, Stations, Chargers). Active item is highlighted. Overview shows stat cards.

**Independent Test**: Open localhost:5174 — sidebar renders with four items, clicking each navigates correctly, active item has #EAF0E6 bg / #007943 text, Overview shows stat cards.

### Implementation for User Story 3

- [X] T022 [P] [US3] Create Sidebar component in apps/dashboard/src/components/Sidebar.tsx — NavLink items for Overview, Partners, Stations, Chargers with active styling (#EAF0E6 bg, #007943 text)
- [X] T023 [US3] Create AppShell component in apps/dashboard/src/components/AppShell.tsx — layout with Sidebar (left) and content area (right), responsive handling
- [X] T024 [P] [US3] Create OverviewPage in apps/dashboard/src/pages/OverviewPage.tsx — stat cards for total partners, stations, chargers fetched from admin-service API
- [X] T025 [P] [US3] Create PartnersPage placeholder in apps/dashboard/src/pages/PartnersPage.tsx — placeholder content with page title
- [X] T026 [P] [US3] Create StationsPage placeholder in apps/dashboard/src/pages/StationsPage.tsx — placeholder content with page title
- [X] T027 [P] [US3] Create ChargersPage placeholder in apps/dashboard/src/pages/ChargersPage.tsx — placeholder content with page title
- [X] T028 [US3] Update App.tsx in apps/dashboard/src/App.tsx — setup react-router-dom routes with BrowserRouter, Routes, Route for all four pages wrapped in AppShell
- [X] T029 [US3] Add redirect for unknown routes in apps/dashboard/src/App.tsx — Navigate to /overview on invalid path

**Checkpoint**: Dashboard shows sidebar with navigation, active state highlighted, Overview loads stats from API.

---

## Phase 6: Polish & CI Validation

**Purpose**: Ensure all three apps pass their respective CI workflows

- [X] T030 [P] Run `npm run build` in apps/driver-web — verify build succeeds
- [X] T031 [P] Run `npx tsc --noEmit` in apps/driver-mobile — verify typecheck passes
- [X] T032 [P] Run `npm run build` in apps/dashboard — verify build succeeds
- [X] T033 [P] Run `npm run lint` in apps/driver-web and apps/dashboard — verify lint passes

**Checkpoint**: All CI workflows green across all three frontend apps.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — T009 (shared types) needed before US1/US2 type implementations
- **User Story 1 (Phase 3)**: Depends on Foundational — all tasks independent of other stories
- **User Story 2 (Phase 4)**: Depends on Foundational — shares only Station type with US1; fully independent otherwise
- **User Story 3 (Phase 5)**: Depends on Foundational — no dependencies on US1 or US2
- **Polish (Phase 6)**: Depends on all three user stories being complete

### User Story Dependencies

- **US1 (P1 - MVP)**: No dependencies on other stories — can start immediately after Foundational
- **US2 (P1)**: No dependencies on other stories — can start in parallel with US1
- **US3 (P2)**: No dependencies on US1 or US2 — fully independent Dashboard

### Within Each User Story

- API client before hooks/components
- Components before App.tsx integration
- All [P] marked tasks within a story can run in parallel

### Parallel Opportunities

- **Phase 1**: T001–T008 mostly parallel; T001 (npm install) blocks T004/T006 for driver-web, T002 blocks T005/T007 for dashboard
- **Phase 2**: T009 must complete before T012/T013/T017/T018; T010 and T011 are parallel
- **Phase 3+**: US1, US2, and US3 can all proceed in parallel (three independent apps)

---

## Parallel Example: User Story 1

```bash
# Launch API client and hook in parallel:
Task: "Create API client in apps/driver-web/src/services/api.ts"
Task: "Create useStations hook in apps/driver-web/src/hooks/useStations.ts"

# Then component depends on both:
Task: "Create StationMap component in apps/driver-web/src/components/StationMap.tsx"
```

## Parallel Example: All Three Stories

```bash
# Once foundational is complete, assign one story per developer:

# Developer A - US1 (Driver Web):
Task: "Create API client + useStations + StationMap + App.tsx update"

# Developer B - US2 (Driver Mobile):
Task: "Create API client + useLocation + StationMarker + index screen"

# Developer C - US3 (Dashboard):
Task: "Create Sidebar + AppShell + all four pages + App.tsx routes"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup — install deps, Tailwind configs, Expo scaffold
2. Complete Phase 2: Foundational — shared types, Vite proxy
3. Complete Phase 3: User Story 1 — Driver Web map
4. **STOP and VALIDATE**: Open localhost:5173, verify map + markers + popups
5. Deploy/demo if ready

### Incremental Delivery

1. Setup + Foundational → dev environment ready
2. Add US1 (Driver Web Map) → Test independently → **MVP achieved**
3. Add US2 (Driver Mobile Map) → Test independently → mobile coverage
4. Add US3 (Dashboard) → Test independently → admin tool ready
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 — Driver Web (P1, MVP)
   - Developer B: User Story 2 — Driver Mobile (P1)
   - Developer C: User Story 3 — Dashboard (P2)
3. Stories are fully independent — no integration conflicts
4. Phase 6 (CI validation) after all stories merge

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
- US3 (Dashboard) uses admin-service API via Vite proxy (:3002); US1/US2 use driver-service (:3001)
