---

description: "Task list for Sprint 1.5 — Driver Mobile App implementation"
---

# Tasks: Driver Mobile App

**Input**: Design documents from `/specs/005-driver-mobile-app/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Not requested — manual verification on simulators only (no test framework in MVP-1 per plan.md)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

## Path Conventions

- All source under `source/apps/driver-mobile/`
- Types: `src/types/index.ts`
- API: `src/api/client.ts`
- Navigation: `src/navigation/AppNavigator.tsx`
- Screens: `src/screens/MapScreen.tsx`, `src/screens/StationDetailScreen.tsx`
- Components: `src/components/ChargerRow.tsx`

---

## Phase 1: Setup

**Purpose**: Project initialization and basic structure

- [X] T001 Create `source/apps/driver-mobile/` directory with `package.json`, `app.json`, `tsconfig.json`, `babel.config.js` for Expo SDK 54 with TypeScript
- [X] T002 [P] Install core dependencies: `expo@54`, `react-native-maps`, `expo-location`, `@react-navigation/native`, `@react-navigation/native-stack`, `react-native-safe-area-context`, `react-native-screens`, `@tanstack/react-query`
- [X] T003 [P] Create directory structure: `src/api/`, `src/screens/`, `src/components/`, `src/navigation/`, `src/types/`
- [X] T004 Wire `dev:mobile` script in root `package.json` pointing to `@borne-map/driver-mobile dev`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T005 Create type definitions in `source/apps/driver-mobile/src/types/index.ts` — `Partner`, `Station`, `Charger`, `VisibleStation` interfaces matching json-server schema
- [X] T006 [P] Create API client in `source/apps/driver-mobile/src/api/client.ts` — `fetchWithError<T>()`, `list<T>()`, `get<T>()` wrappers with `Platform.select` for base URL resolution
- [X] T007 [P] Create navigation setup in `source/apps/driver-mobile/src/navigation/AppNavigator.tsx` — `NativeStackNavigator` with typed `RootStackParamList` (`Map`, `StationDetail:{stationId}`)
- [X] T008 Create `source/apps/driver-mobile/App.tsx` — wrap with `QueryClientProvider`, `NavigationContainer`, `SafeAreaProvider`

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 — Mobile Map Discovery (Priority: P1) 🎯 MVP

**Goal**: Full-screen map with station markers color-coded by availability, location permission handling, callout navigation

**Independent Test**: Launch app in simulator. Grant location → map centers on device. Deny → Tunisia center. Green pins for stations with available chargers, red pins for zero. Tap marker → callout with name + count. Tap callout → Station Detail screen.

### Implementation for User Story 1

- [X] T009 [P] [US1] Create `source/apps/driver-mobile/src/screens/MapScreen.tsx` — full-screen react-native-maps `MapView` centered on Tunisia (33.8869, 9.5375) zoom delta ~0.5 with "BorneMap" header
- [X] T010 [P] [US1] Implement location permission flow in `MapScreen.tsx` — `expo-location` `requestForegroundPermissionsAsync` → if granted `getCurrentPositionAsync` to center map, if denied keep Tunisia fallback
- [X] T011 [P] [US1] Implement partner visibility filter in `MapScreen.tsx` — fetch all partners/stations/chargers with `@tanstack/react-query`, filter stations where `is_verified && is_live && is_active`, compute `availableCount` per station
- [X] T012 [P] [US1] Implement marker rendering in `MapScreen.tsx` — colored `Marker` components: green (`#00E676`) for availableCount > 0, red (`#EF4444`) for 0
- [X] T013 [US1] Implement marker callouts in `MapScreen.tsx` — `Callout` showing station name and `availableCount/totalChargers`, tap navigates to StationDetail via `navigation.navigate('StationDetail', { stationId })`
- [X] T014 [US1] Add error and loading states in `MapScreen.tsx` — `ActivityIndicator` while fetching, error message with retry on API failure

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 — Mobile Station Detail (Priority: P2)

**Goal**: Station detail screen with full name, address, charger list with colored status text, back navigation

**Independent Test**: From map, tap a marker callout → Station Detail loads with name, address, charger list. Each charger shows connector type, power kW, and status in colored text. Tap back → returns to map.

### Implementation for User Story 2

- [X] T015 [P] [US2] Create `source/apps/driver-mobile/src/components/ChargerRow.tsx` — renders connector type, power_kw, status as colored text (green `#00E676`, orange `#FF9800`, gray `#9E9E9E`, red `#EF4444`)
- [X] T016 [US2] Create `source/apps/driver-mobile/src/screens/StationDetailScreen.tsx` — fetch station by id and chargers by `station_id` with `@tanstack/react-query`, display station name + address, render `ChargerRow` list, handle empty chargers with "No chargers at this station." message
- [X] T017 [US2] Add back navigation in `StationDetailScreen.tsx` — `navigation.goBack()` button/label in header bar, returns to map at previous position
- [X] T018 [US2] Add error and loading states in `StationDetailScreen.tsx` — `ActivityIndicator` while fetching, error message with retry, empty chargers edge case

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [X] T019 [P] Verify `dev:mobile` script works — `pnpm dev:mobile` from monorepo root starts Expo dev server
- [ ] T020 Run quickstart.md validation — manual test on iOS Simulator and Android Emulator against json-server
- [X] T021 Update `.gitignore` in `source/apps/driver-mobile/` for Expo build artifacts (`.expo/`, `dist/`, `*.jks`, `*.p8`, `*.p12`, `*.key`, `*.mobileprovision`, `*.orig.*`)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **US1 Map Discovery (Phase 3)**: Depends on Foundational — no dependency on US2
- **US2 Station Detail (Phase 4)**: Depends on Foundational — no dependency on US1
- **Polish (Phase 5)**: Depends on US1 + US2 completion

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — no dependencies on other stories
- **US2 (P2)**: Can start after Foundational — independently testable from US1 (can navigate directly to StationDetail with hardcoded ID)

### Within Each User Story

- Types before implementation
- API client before screen
- Screen before integration with navigation
- Core implementation before error/loading states

### Parallel Opportunities

- T002 and T003 can run in parallel (different packages)
- T006 and T007 can run in parallel (different files, no overlap)
- All US1 tasks marked [P] can run in parallel
- All US2 tasks marked [P] can run in parallel
- US1 and US2 can be worked on in parallel by different developers

---

## Parallel Example: User Story 1

```bash
# Launch all US1 implementation tasks together:
Task: "Create MapScreen.tsx with full-screen map view"
Task: "Implement location permission flow in MapScreen.tsx"
Task: "Implement partner visibility filter in MapScreen.tsx"
Task: "Implement marker rendering in MapScreen.tsx"

# Then:
Task: "Implement marker callouts in MapScreen.tsx"     # depends on markers
Task: "Add error and loading states in MapScreen.tsx"  # depends on data fetching
```

## Parallel Example: User Story 2

```bash
# Launch all US2 tasks together:
Task: "Create ChargerRow.tsx component"
Task: "Create StationDetailScreen.tsx with navigation param polling"
Task: "Add back navigation in StationDetailScreen"

# Then:
Task: "Add error and loading states in StationDetailScreen.tsx"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test map with location, markers, callouts independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 (Map Discovery) → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 (Station Detail) → Test independently → Full feature

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: US1 Map Discovery (Marker rendering, location, callouts)
   - Developer B: US2 Station Detail (ChargerRow, detail screen, back nav)
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
