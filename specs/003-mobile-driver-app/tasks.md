# Tasks: Mobile Driver App

**Input**: Design documents from `/specs/003-mobile-driver-app/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **App root**: `source/apps/mobile-driver/`
- **Components**: `src/components/`
- **Hooks**: `src/hooks/`
- **Services**: `src/services/`
- **Cache**: `src/cache/`
- **Types**: `src/types/`
- **Utils**: `src/utils/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [ ] T001 Create `source/apps/mobile-driver/` directory tree (src/components/, src/hooks/, src/services/, src/cache/, src/types/, src/utils/, assets/markers/)
- [ ] T002 [P] Initialize Expo SDK 54 project with `npx create-expo-app` in `source/apps/mobile-driver/` — configure for TypeScript strict mode
- [ ] T003 [P] Install dependencies: `react-native-maps`, `expo-location`, `@react-native-async-storage/async-storage`, `expo-constants`, `@tanstack/react-query`, `@react-native-community/netinfo`
- [ ] T004 [P] Create `tsconfig.json` with strict mode enabled and path aliases
- [ ] T005 [P] Create charging pin SVG/PNG assets in `assets/markers/` (custom vector charging pin icon)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T006 Create TypeScript types in `src/types/index.ts` — Station, Viewport, ApiFetchState (discriminated union of loading/success/empty/error/offline), AsyncCacheEntry, ValidationErrors
- [ ] T007 [P] Create API service in `src/services/api.ts` — configurable base URL from `expo-constants` extras, `fetchNearbyStations(lat, lng, radius)` with 10s timeout and AbortController support
- [ ] T008 [P] Create coordinate utilities in `src/utils/coordinates.ts` — `isWithinTunisia(lat, lng)`, `roundTo2dp(value)`, `TUNISIA_BOUNDS` constant, `DEFAULT_VIEWPORT` (Tunis 36.8, 10.18)
- [ ] T009 [P] Create network detection in `src/utils/network.ts` — `isOnline()` using `@react-native-community/netinfo` or a fetch-based connectivity check

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Interactive Map with Station Markers (Priority: P1) 🎯 MVP

**Goal**: Driver opens the app and sees a full-screen react-native-maps map centered on their location (or Tunis default) with station markers.

**Independent Test**: Open the app on a physical device via Expo Go. The map renders, centers on GPS location (or Tunis), and station markers appear as charging pin icons. Panning outside Tunisia is blocked.

### Implementation for User Story 1

- [ ] T010 [P] [US1] Create `useNearbyStations` hook in `src/hooks/useNearbyStations.ts` — React Query hook calling `api.fetchNearbyStations()`, returns stations array + fetch state
- [ ] T011 [P] [US1] Create `MapContainer.tsx` in `src/components/MapContainer.tsx` — react-native-maps MapView with Tunisia boundary constraints, GPS centering via expo-location, default to Tunis if permission denied
- [ ] T012 [P] [US1] Create `StationCallout.tsx` in `src/components/StationCallout.tsx` — marker callout displaying station name, distance, and partner name
- [ ] T013 [US1] Create `App.tsx` at `source/apps/mobile-driver/App.tsx` — QueryClientProvider wrapper, MapContainer with useNearbyStations hook, pull-to-refresh via RefreshControl
- [ ] T014 [US1] Create `app.json` at `source/apps/mobile-driver/app.json` — Expo config with `expo.extra.apiBaseUrl` field and plugin configurations

**Checkpoint**: Interactive map with station markers renders on device. MVP deliverable.

---

## Phase 4: User Story 2 - Viewport Debouncing (Priority: P2)

**Goal**: Viewport changes are debounced by 300ms before triggering station re-fetch, with in-flight request cancellation.

**Independent Test**: Pan the map rapidly — no API calls fire during the gesture. After 300ms of stillness, exactly one API call fires. Swiftly pan again — the in-flight request is cancelled.

### Implementation for User Story 2

- [ ] T015 [P] [US2] Create `useDebounce.ts` in `src/hooks/useDebounce.ts` — generic debounce hook with configurable delay (default 300ms), returns debounced value
- [ ] T016 [US2] Integrate debounce into `MapContainer.tsx` — debounce viewport center changes before passing to `useNearbyStations` query, cancel in-flight requests via AbortController when new pan starts

**Checkpoint**: Map efficiently debounces API calls during rapid interaction.

---

## Phase 5: User Story 3 - Loading, Error, and Empty States (Priority: P2)

**Goal**: Shimmer loading placeholders, error boundary with retry, and empty-state guidance message when no stations are found.

**Independent Test**: Slow network → shimmer skeleton over map. Airplane mode → error boundary with "Retry Connection" button. Remote area → guidance message to pan to Tunis/Sousse/Sfax.

### Implementation for User Story 3

- [ ] T017 [P] [US3] Create `ShimmerSkeleton.tsx` in `src/components/ShimmerSkeleton.tsx` — animated shimmer placeholder mimicking the map/marker layout (not a spinner)
- [ ] T018 [P] [US3] Create `ErrorBoundary.tsx` in `src/components/ErrorBoundary.tsx` — React error boundary wrapping the map, displays styled error message with "Retry Connection" button, max 3 retries per manual attempt
- [ ] T019 [P] [US3] Create `EmptyState.tsx` in `src/components/EmptyState.tsx` — guidance message when no stations found, instructing user to pan towards Tunis, Sousse, or Sfax
- [ ] T020 [US3] Integrate all three state components into `App.tsx` — conditionally render shimmer/error/empty/success/markers based on `ApiFetchState` discriminated union

**Checkpoint**: All four UI states (loading/success/empty/error) render correctly.

---

## Phase 6: User Story 4 - AsyncStorage Offline Cache (Priority: P2)

**Goal**: Successful API responses are cached to AsyncStorage. On network failure, cached data is displayed with an offline banner.

**Independent Test**: Load stations → airplane mode → cached markers appear + "Viewing cached data" banner at top. Turn off airplane mode → pull-to-refresh → fresh data loads.

### Implementation for User Story 4

- [ ] T021 [P] [US4] Create AsyncStorage cache in `src/cache/asyncStorage.ts` — `writeCache(viewportKey, stations)` and `readCache(viewportKey)` with coordinates rounded to 2dp for privacy; catch storage errors (corruption, full disk) silently and fall back to online-only mode with console warning
- [ ] T022 [P] [US4] Create `OfflineBanner.tsx` in `src/components/OfflineBanner.tsx` — top-bar banner reading "Viewing cached data. Connect to the internet for real-time status updates."
- [ ] T023 [US4] Integrate cache and banner into `useNearbyStations` hook — on success write to cache, on network failure read from cache and show OfflineBanner

**Checkpoint**: App works offline with cached data and visible banner.

---

## Phase 7: User Story 5 - Macro-Zoom Overlay (Priority: P3)

**Goal**: When zoom drops below level 8, an overlay covers the map with "Zoom in closer to view available charging stations."

**Independent Test**: Pinch zoom out past level 8 — overlay appears with instructional text. Zoom back in — overlay disappears, markers return.

### Implementation for User Story 5

- [ ] T024 [P] [US5] Create `MacroZoomOverlay.tsx` in `src/components/MacroZoomOverlay.tsx` — full-viewport overlay with centered text "Zoom in closer to view available charging stations."
- [ ] T025 [US5] Integrate overlay into `MapContainer.tsx` — track zoom level from MapView `onRegionChangeComplete`, show overlay when zoom < 8, hide markers beneath it

**Checkpoint**: Overlay appears/disappears correctly at zoom threshold.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories, plus docs and validation.

- [ ] T026 [P] Run quickstart.md validation — verify all test scenarios produce expected behavior on both iOS and Android via Expo Go
- [ ] T027 Update `source/docs/system_state.md`, `source/docs/roadmap_status.md`, and `source/docs/sprint_backlog.md` with Sprint 1.3 completion
- [ ] T028 Update `.env.template` at `source/apps/mobile-driver/.env.template` with `API_BASE_URL` documentation

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3-7)**: All depend on Foundational phase completion
  - US1 (P1) → US2 (P2) → US3 (P2) → US4 (P2) → US5 (P3) sequential (MapContainer must exist before debounce/state/cache/overlay can integrate)
- **Polish (Phase 8)**: Depends on all user stories complete

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational. No dependencies on other stories.
- **US2 (P2)**: Depends on US1 (needs MapContainer to exist). Independent hook, integrates into existing MapContainer.
- **US3 (P2)**: Depends on US1 (needs MapContainer/App.tsx). Independent components, integrate into App.tsx.
- **US4 (P2)**: Depends on US1 (needs useNearbyStations hook). Independent cache module, integrates into hook.
- **US5 (P3)**: Depends on US1 (needs MapContainer). Independent component, integrates via zoom tracking.

### Within Each User Story

- Types/utilities before services
- Services before components
- Core implementation before integration
- Story complete before moving to next

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel
- Within US1: T010 (hook), T011 (map), T012 (callout) can run in parallel; T013 (App.tsx) depends on all three
- Within US3: T017 (shimmer), T018 (error boundary), T019 (empty state) can run in parallel; T020 (integration) depends on all three
- Within US4: T021 (cache) and T022 (banner) can run in parallel; T023 (integration) depends on both
- Within US5: T024 (overlay) and T025 (integration) sequential
- Polish tasks marked [P] can run in parallel

---

## Parallel Example: User Story 1

```bash
# Launch all [P] tasks for User Story 1 together:
Task: "T010 [P] [US1] Create useNearbyStations hook in src/hooks/useNearbyStations.ts"
Task: "T011 [P] [US1] Create MapContainer.tsx in src/components/MapContainer.tsx"
Task: "T012 [P] [US1] Create StationCallout.tsx in src/components/StationCallout.tsx"

# After all three complete:
Task: "T013 [US1] Create App.tsx at source/apps/mobile-driver/App.tsx"
Task: "T014 [US1] Create app.json at source/apps/mobile-driver/app.json"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: US1 (interactive map + markers)
4. **STOP and VALIDATE**: Open app via Expo Go → see map with station markers
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add US1 (map + markers) → Test independently → **MVP!**
3. Add US2 (viewport debounce) → Test independently
4. Add US3 (loading/error/empty states) → Test independently
5. Add US4 (offline cache) → Test independently
6. Add US5 (macro-zoom overlay) → Test independently
7. Polish (validation, docs)

### Parallel Team Strategy

With multiple developers:
1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: US1 (map + markers + App.tsx)
   - Developer B: US2 (debounce) + US4 (cache)
   - Developer C: US3 (shimmer/error/empty states)
3. US5 (macro-zoom overlay) is small enough for any developer to pick up
4. Stories integrate without conflicts (different files — components/ vs hooks/ vs cache/)
