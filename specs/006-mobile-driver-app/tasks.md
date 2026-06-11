# Tasks: Mobile Driver App (Core UX)

**Input**: Design documents from `/specs/006-mobile-driver-app/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/components.md, quickstart.md

**Tests**: No test tasks — the spec defines manual/integration test scenarios for each story, not unit test requirements.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US4)
- Include exact file paths in descriptions

## Path Conventions

- **Project root**: `source/front/` (Expo SDK 54 app root)
- **Routing**: `app/`
- **Components**: `src/components/`
- **Hooks**: `src/hooks/`
- **Services**: `src/services/`
- **Types**: `src/types/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Initialize the Expo SDK 54 project — directory structure, package.json, app.json, tsconfig

- [ ] T001 Create mobile app directory structure at `source/front/` (app/, src/components/, src/hooks/, src/services/, src/types/)
- [ ] T002 [P] Initialize package.json with Expo SDK 54 dependencies (expo, react-native, react-native-maps, expo-router, expo-location, react-native-reanimated, react-native-gesture-handler, react-native-safe-area-context, @borne/design-system) and app.json with Expo Router scheme/plugins at `source/front/package.json` and `source/front/app.json`
- [ ] T003 [P] Create tsconfig.json extending expo/tsconfig.base at `source/front/tsconfig.json`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared infrastructure that MUST be complete before any user story — API client, type definitions, and app root layout

- [ ] T004 Create service layer: shared HTTP client in `src/services/api.ts` and environment config with service URLs in `src/services/config.ts`
- [ ] T005 [P] Create TypeScript type definitions for Station, Charger, MapRegion in `src/types/station.ts` and ClickstreamEvent in `src/types/events.ts`
- [ ] T006 Create Expo Router root layout at `app/_layout.tsx` with ThemeProvider (from @borne/design-system) and GestureHandlerRootView

**Checkpoint**: Foundation ready — types, services, and root layout exist. User story implementation can start.

---

## Phase 3: User Story 1 — Map Screen with Station Markers (Priority: P1) 🎯 MVP

> **Note**: US3 (Skeleton Loading & Error States) is embedded within this phase. Skeleton, error, and empty states are built directly into MapScreen and StationBottomSheet — not implemented as a standalone story.

**Goal**: Full-screen map centered on user location, station markers from nearby search, markers update on pan/zoom, empty state when no stations found.

**Independent Test**: Open app in simulator with Tunis location. Verify map renders with station markers. Pan to empty area — verify empty state message. Verify skeleton placeholder during initial load.

### Implementation for User Story 1

- [ ] T007 [P] [US1] Create useLocation hook (GPS permission, current location, fallback to Tunis center) in `src/hooks/useLocation.ts`
- [ ] T008 [P] [US1] Create useNearbyStations hook (calls GET /api/v1/stations/nearby with lat/lng/radius_m, re-fetches on region change, 10s timeout) in `src/hooks/useNearbyStations.ts`
- [ ] T009 [P] [US1] Create StationMarker component (react-native-maps Marker with station pin) in `src/components/StationMarker.tsx`
- [ ] T010 [P] [US1] Create MapErrorState component (renders ErrorState or EmptyState from design system based on error/empty condition) in `src/components/MapErrorState.tsx`
- [ ] T011 [US1] Create MapScreen component (react-native-maps MapView, markers from useNearbyStations, loading skeleton via Skeleton variant="map", error/empty via MapErrorState) in `src/components/MapScreen.tsx`
- [ ] T012 [US1] Create Expo Router index screen at `app/index.tsx` using MapScreen as the default route

**Checkpoint**: Map renders full-screen with station markers. Panning triggers re-fetch. Skeleton shows during load. Empty state when no stations.

---

## Phase 4: User Story 2 — Station Details Bottom Sheet (Priority: P1)

**Goal**: Tapping a marker opens a bottom sheet with station name, distance, charger list with connector type and status. Sheet dismisses on swipe-down.

**Independent Test**: Tap a station marker — verify bottom sheet animates up with name, distance, and at least one charger entry with status. Swipe down — sheet dismisses. Tap different marker — sheet updates.

### Implementation for User Story 2

- [ ] T013 [P] [US2] Create useStationDetail hook (calls GET /api/v1/stations/{id}, returns station with chargers, 10s timeout) in `src/hooks/useStationDetail.ts`
- [ ] T014 [P] [US2] Create ChargerList component (renders charger rows with connector type, power_kw, status badge) in `src/components/ChargerList.tsx`
- [ ] T015 [US2] Create StationBottomSheet component (BottomSheet from design system, uses useStationDetail and ChargerList, skeleton via Skeleton variant="list" while loading) in `src/components/StationBottomSheet.tsx`
- [ ] T016 [US2] Integrate StationBottomSheet into MapScreen — open on marker tap, update on different marker, close on swipe-down in `src/components/MapScreen.tsx`

**Checkpoint**: Marker tap opens sheet with station details. Sheet skeleton shows during fetch. Swipe-down dismisses. Different marker updates content.

---

## Phase 5: User Story 4 — Interaction Event Tracking (Priority: P2)

**Goal**: All user interactions (map_open, map_pan, map_zoom, station_click, station_view, nearby_search) generate clickstream events sent to the Clickstream Service without blocking the UX.

**Independent Test**: Perform a series of actions (open app, pan map, tap marker, view sheet). Verify events appear in analytics_db within seconds.

### Implementation for User Story 4

- [ ] T017 [P] [US4] Create useClickstream hook (fire-and-forget POST /api/v1/events with silent error handling) in `src/hooks/useClickstream.ts`
- [ ] T018 [US4] Integrate clickstream event tracking into MapScreen (map_open, map_pan, map_zoom, nearby_search) and StationBottomSheet (station_click, station_view) in `src/components/MapScreen.tsx` and `src/components/StationBottomSheet.tsx`

**Checkpoint**: All interaction events fire and reach the Clickstream Service. No UX impact when service is unreachable.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final verification and cleanup

- [ ] T019 Run quickstart.md verification checklist — all 10 checks pass

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **US1 Map Screen (Phase 3)**: Depends on Phase 2
- **US2 Station Details (Phase 4)**: Depends on Phase 2 + Phase 3 (Marker tap integration)
- **US4 Event Tracking (Phase 5)**: Depends on Phase 2 — can run in parallel with Phase 3 and 4
- **Polish (Phase 6)**: Depends on all phases complete

### User Story Dependency Graph

```
Phase 1 (Setup) → Phase 2 (Foundational) → US1 Map Screen ──→ US2 Station Details
                                          → US4 Event Tracking (parallel)
```

### Within Each User Story

- Hooks before components
- Components before screen integration
- Screen integration before routing

### Parallel Opportunities

- Setup tasks T002 and T003 can run in parallel
- Foundational tasks T005 (types) can run in parallel with T004 (services)
- US1 implementation tasks T007–T010 are all [P] — can run in parallel
- US2 implementation tasks T013–T014 are all [P] — can run in parallel
- US4 (Event Tracking) can run in parallel with US1 and US2 after Phase 2

---

## Parallel Example: Phase 3 (US1 — Map Screen)

```bash
# Launch all US1 foundation tasks in parallel:
Task: "Create useLocation hook in src/hooks/useLocation.ts"
Task: "Create useNearbyStations hook in src/hooks/useNearbyStations.ts"
Task: "Create StationMarker component in src/components/StationMarker.tsx"
Task: "Create MapErrorState component in src/components/MapErrorState.tsx"
```

## Parallel Example: Phases 3, 4, 5 (All Stories)

```bash
# After Phase 2, US1 and US4 can start together:
Task: "US1: MapScreen components and hooks"
Task: "US4: useClickstream hook"

# After US1 is done, US2 can proceed:
Task: "US2: StationBottomSheet with marker tap integration"
Task: "US4: Event tracking integration (depends on US1 + US4 hook)"
```

---

## Implementation Strategy

### MVP First (US1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: US1 Map Screen with Station Markers
4. **STOP and VALIDATE**: Verify map renders, markers appear, skeleton shows during load
5. Deploy/demo if ready

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add US1 Map Screen → Test independently → Deploy/Demo (MVP!)
3. Add US2 Station Details → Test independently → Deploy/Demo
4. Add US4 Event Tracking → Test independently → Deploy/Demo (can be parallel)

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Developer A: US1 Map Screen + US2 Station Details
3. Developer B: US4 Event Tracking
4. Stories complete and integrate independently
