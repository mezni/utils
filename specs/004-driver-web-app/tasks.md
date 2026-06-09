# Tasks: Driver Web App

**Input**: Design documents from `/specs/004-driver-web-app/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api-contract.md

**Tests**: Not requested — manual verification only.

**Organization**: Tasks grouped by user story for independent implementation.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Initialize the Driver Web App project with Vite + React + TypeScript + Tailwind + Leaflet.

- [ ] T001 Create `source/apps/driver-web/` with Vite + React + TypeScript (same versions as Dashboard — React 19, Vite 8, TypeScript 6.0)
- [ ] T002 [P] Configure `source/apps/driver-web/vite.config.ts` with `@vitejs/plugin-react` and proxy `/api` → `http://localhost:3001`
- [ ] T003 [P] Configure `source/apps/driver-web/tailwind.config.js` extending shared tokens from `packages/ui/tailwind.config.base.js`
- [ ] T004 [P] Install Leaflet and react-leaflet dependencies in `source/apps/driver-web/package.json`
- [ ] T005 [P] Create `source/apps/driver-web/src/api/client.ts` with `fetchWithError`, `list`, `get` functions matching the Dashboard's API client pattern
- [ ] T006 [P] Create `source/apps/driver-web/src/main.tsx` — React root with BrowserRouter, import Leaflet CSS
- [ ] T007 [P] Create `source/apps/driver-web/src/App.tsx` with React Router routes: `/` (MapPage) and `/stations/:id` (StationDetailPage)
- [ ] T008 [P] Update root `source/packages/ui/tailwind.config.base.js` to ensure brand colors include `brand.glow` (#00E676) if not already present
- [ ] T009 Update root `package.json` — change `dev:web` script from placeholder to `pnpm --filter @borne-map/driver-web dev`
- [ ] T010 [P] Add `source/apps/driver-web/index.html` with proper title and viewport meta for mobile

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Create shared components and data types used by both screens.

- [ ] T011 [P] Create `source/apps/driver-web/src/components/StationCard.tsx` — displays station name, address, available/total charger count
- [ ] T012 [P] Create `source/apps/driver-web/src/components/ChargerRow.tsx` — displays connector type, power kW, status badge for a single charger
- [ ] T013 [P] Create `source/apps/driver-web/src/components/ZoomControls.tsx` — zoom-in/zoom-out buttons for the map (Leaflet zoom control alternative)
- [ ] T014 [P] Define shared types in `source/apps/driver-web/src/api/client.ts` or a separate types file: `Partner`, `Station`, `Charger`, `VisibleStation`

**Checkpoint**: Foundation ready — shared components and types available

---

## Phase 3: User Story 1 — Map Discovery (Priority: P1) 🎯 MVP

**Goal**: Full-screen Leaflet map centered on Tunisia showing color-coded station markers with popups.

**Independent Test**: Open Driver Web App. Map loads centered on Tunisia at zoom 7. Green markers for PRT001/PRT002 stations with available chargers; red markers for zero-available stations. No markers for PRT003. Click marker → popup with name, address, charger count, "View Details" link.

### Implementation for User Story 1

- [ ] T015 [P] [US1] Create `source/apps/driver-web/src/pages/MapPage.tsx` — full-screen Leaflet MapContainer with OpenStreetMap tiles, initial center (33.8869, 9.5375) zoom 7
- [ ] T016 [P] [US1] Implement data fetching in `MapPage.tsx` — fetch all partners, stations, chargers on mount via `list` from the API client
- [ ] T017 [US1] Implement partner visibility filter in `MapPage.tsx` — build set of visible partner IDs where `is_verified && is_live && is_active`, filter stations to those IDs
- [ ] T018 [US1] Compute `availableCount` per station in `MapPage.tsx` — count chargers per station with `status === 'available'`, assign green (`availableCount > 0`) or red (`availableCount === 0`) marker color
- [ ] T019 [US1] Render CircleMarker per visible station in `MapPage.tsx` — radius 8, white border weight 2, fillColor green (#00E676) or red (#EF4444)
- [ ] T020 [US1] Implement marker click handler in `MapPage.tsx` — popup showing station name, address, "X/Y available", and "View Details" link navigating to `/stations/:id`
- [ ] T021 [US1] Add floating top bar in `MapPage.tsx` — "BorneMap" brand name, height 56px, map fills remaining height (`calc(100vh - 56px)`)
- [ ] T022 [US1] Add loading state — spinner or skeleton while fetching data
- [ ] T023 [US1] Add error state — error message with Retry button when API unreachable
- [ ] T024 [US1] Track map position (center, zoom) in component state for restoration on back navigation
- [ ] T025 [US1] Handle edge case — zero visible stations (all partners filtered out or API returns empty arrays): show map with no markers, no crash

**Checkpoint**: Map Discovery fully functional — color-coded markers, popups, error/loading states

---

## Phase 4: User Story 2 — Station Detail (Priority: P2)

**Goal**: Station Detail screen showing station info and charger list with back navigation to map.

**Independent Test**: Open map, click marker, click "View Details" in popup. Detail screen loads with station name, address, charger list. Click back → map at same position.

### Implementation for User Story 2

- [ ] T026 [P] [US2] Create `source/apps/driver-web/src/pages/StationDetailPage.tsx` — fetch station by ID via `get<Station>('stations', id)` and chargers via `list<Charger>('chargers', { station_id: id })`
- [ ] T027 [P] [US2] Implement StationDetailPage header — back button, station name, station address
- [ ] T028 [US2] Render charger list in StationDetailPage — iterate chargers, render each with `ChargerRow` component showing connector type, power kW, status
- [ ] T029 [US2] Implement back navigation — returns to map at same position and zoom via React Router's `useNavigate()` and `location.state`
- [ ] T030 [US2] Add loading state — spinner while fetching station and chargers
- [ ] T031 [US2] Add error state — error message with Retry on API failure
- [ ] T032 [US2] Handle edge case — station with no chargers: show "No chargers at this station" message instead of empty list

**Checkpoint**: Station Detail fully functional — charger list, back nav, error/loading states

---

## Phase 5: Polish & Cross-Cutting Concerns

**Purpose**: Verify all screens, typecheck, update workspace config.

- [ ] T033 [P] Run `tsc --noEmit` in `source/apps/driver-web/` to verify TypeScript compilation
- [ ] T034 [P] Run `pnpm dev:web` and verify map screen against quickstart.md scenarios
- [ ] T035 [P] Verify marker colors — check PRT001/PRT002 stations are visible, PRT003 stations are hidden
- [ ] T036 [P] Verify error recovery — stop json-server, confirm error state on both screens, restart and click Retry
- [ ] T037 [P] Verify back navigation — navigate to Station Detail, click back, confirm map position is preserved
- [ ] T038 [P] Verify deep link — open `/stations/STN001` directly in browser, confirm Station Detail loads correctly
- [ ] T039 [P] Verify Dashboard integration — change a charger status in Dashboard, refresh Driver Web, confirm marker color updates

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 — BLOCKS all user stories
- **User Stories (Phase 3-4)**: Both depend on Phase 2
  - US1 and US2 can proceed in parallel if staffed (different files, no shared mutable state)
  - Sequential priority order: US1 → US2
- **Polish (Phase 5)**: Depends on both user stories

### User Story Dependencies

- **US1 (P1)**: Depends on Phase 2 components (StationCard, ChargerRow shared) — can start after Phase 2
- **US2 (P2)**: Depends on Phase 2 components and US1's route structure — needs shared types but MapPage and StationDetailPage are fully independent files

### Within Each User Story

- Data fetching before rendering
- Core implementation before error/loading states
- Story complete before moving to next priority

### Parallel Opportunities

- All Phase 1 tasks marked [P] run in parallel
- All Phase 2 tasks marked [P] run in parallel
- US1 and US2 tasks can be worked on in parallel (different page directories)

---

## Parallel Example: User Story 1

```bash
# Launch all independent tasks for Map Discovery together:
Task: "Create MapPage.tsx with MapContainer"
Task: "Implement data fetching on mount"
Task: "Implement partner visibility filter"
Task: "Compute availableCount per station"
Task: "Render CircleMarkers"
Task: "Implement marker click → popup"
Task: "Add floating top bar"
Task: "Add loading state"
Task: "Add error state"
Task: "Track map position state"
Task: "Handle zero visible stations edge case"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (Map Discovery)
4. **STOP and VALIDATE**: Test Map screen with quickstart.md
5. Demo-ready after US1 alone

### Incremental Delivery

1. Phase 1 + Phase 2 → Foundation ready (project initialized, components available)
2. US1 (Map Discovery) → Driver can see stations on a map → Deploy/Demo (MVP!)
3. US2 (Station Detail) → Driver can see charger details → Deploy/Demo
4. Polish → Verify cross-cutting scenarios

### Single Developer Strategy

1. Phase 1 → Phase 2 → US1 → US2 → Polish
2. No file conflicts between phases

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story
- Each user story is independently testable
- Commit after each user story phase
- Stop at any checkpoint to validate story independently
