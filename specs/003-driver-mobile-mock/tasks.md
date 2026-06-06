---

description: "Task list for Sprint 1.3 — Driver Mobile App with Mock Data"

---

# Tasks: Driver Mobile App with Mock Data

**Input**: Design documents from `/specs/003-driver-mobile-mock/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are NOT explicitly requested in the feature specification. No test tasks are generated per the constitution's "Testing is optional" rule.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Mobile app**: `apps/driver-mobile/src/` — all source files live under the Expo project
- **UI tokens**: `packages/ui/src/tokens/native.ts` — shared native design tokens

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Scaffold the Expo + React Native project and configure the monorepo workspace

- [x] T001 Create `apps/driver-mobile/` directory and scaffold Expo project with `npx create-expo-app@latest`
- [x] T002 [P] Configure `apps/driver-mobile/package.json` with workspace name `@borne-map/driver-mobile`, Expo 52 deps, React Navigation 6 deps, i18next, expo-localization, expo-font
- [x] T003 [P] Create `apps/driver-mobile/tsconfig.json` extending monorepo `tsconfig.base.json` with React Native path aliases
- [x] T004 [P] Create `apps/driver-mobile/app.json` with app name "BorneMap", scheme, platform configs (iOS + Android)
- [x] T005 [P] Create `apps/driver-mobile/babel.config.js` with Expo preset + reanimated plugin
- [x] T006 [P] Create `apps/driver-mobile/metro.config.js` with monorepo watchFolders for `packages/` and `apps/`
- [x] T007 Add `packages/ui` as a workspace dependency in `apps/driver-mobile/package.json` for native token imports

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Types, mock data, i18n, navigation shell, shared hooks, and shared components that ALL user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T008 Create type definitions at `apps/driver-mobile/src/types/index.ts` — Station, Charger, Review, DriverUser, FilterState interfaces (same shape as web mock data)
- [x] T009 [P] Copy mock data files from `apps/driver-web/src/mocks/` to `apps/driver-mobile/src/mocks/` — stations.ts, chargers.ts, reviews.ts, users.ts
- [x] T010 [P] Create i18n configuration at `apps/driver-mobile/src/i18n/index.ts` with i18next + react-i18next + expo-localization, RTL switch via I18nManager.forceRTL()
- [x] T011 [P] Copy i18n translation files from `apps/driver-web/src/i18n/` to `apps/driver-mobile/src/i18n/` — ar.json, fr.json — extending with mobile-specific keys
- [x] T012 Create navigation types at `apps/driver-mobile/src/navigation/types.ts` — RootTabParamList, RootStackParamList with all screen params
- [x] T013 Create RootNavigator at `apps/driver-mobile/src/navigation/RootNavigator.tsx` — bottom tab navigator (Map, Stations List, Search, Favorites, Profile) + native stack (Station Detail, Login/Register)
- [x] T014 [P] Create `useStations` hook at `apps/driver-mobile/src/hooks/useStations.ts` — returns mock stations array + station lookup by ID
- [x] T015 Install Plus Jakarta Sans font via expo-font at app startup, create font loading in `apps/driver-mobile/src/App.tsx` with a splash/loading screen
- [x] T016 [P] Create MobileTopBar component at `apps/driver-mobile/src/components/MobileTopBar.tsx` — header with safe area top inset, brand name, notification bell icon
- [x] T017 [P] Create SearchBar component at `apps/driver-mobile/src/components/SearchBar.tsx` — rounded TextInput with search icon, floating card style, navigates to Search screen on focus
- [x] T018 [P] Create BottomTabBar component at `apps/driver-mobile/src/components/BottomTabBar.tsx` — custom tab bar with safe area bottom inset, 5 tab icons with labels
- [x] T019 [P] Create FilterPills component at `apps/driver-mobile/src/components/FilterPills.tsx` — horizontal ScrollView of pill buttons, active/inactive states, charger type filter
- [x] T020 Create `App.tsx` entry at `apps/driver-mobile/src/App.tsx` — SafeAreaProvider, NavigationContainer, i18n init, font loading, RTL detection

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 — Browse Stations on Map (Priority: P1) 🎯 MVP

**Goal**: Opening the app shows a full-bleed map placeholder with 15 station pin markers and a bottom card showing the first/selected station's details.

**Independent Test**: Open the app and verify the `#EAF0E6` background fills the screen, at least 10 pin markers are visible, and the BottomStationCard shows station name, address, availability badge, distance, charger count, and rating.

- [x] T021 [P] [US1] Create MapPinMarker component at `apps/driver-mobile/src/components/MapPinMarker.tsx` — circular View positioned absolutely, shadow.pin glow, default/selected/unavailable states, onPress handler
- [x] T022 [P] [US1] Create ZoomControls component at `apps/driver-mobile/src/components/ZoomControls.tsx` — floating +/- button group, absolute positioned bottom-right, onZoomIn/onZoomOut callbacks
- [x] T023 [P] [US1] Create BottomStationCard component at `apps/driver-mobile/src/components/BottomStationCard.tsx` — absolute-positioned bottom card with shadow.float, station name, address, availability badge, distance, charger count, rating, tap to navigate to detail
- [x] T024 [P] [US1] Create CenterActionButton component at `apps/driver-mobile/src/components/CenterActionButton.tsx` — raised circular button positioned above BottomTabBar, brand.primary background, lightning icon, onPress handler
- [x] T025 [P] [US1] Create SpecRow component at `apps/driver-mobile/src/components/SpecRow.tsx` — row with label (left) and value (right), used in BottomStationCard
- [x] T026 [US1] Create HomeMapScreen at `apps/driver-mobile/src/screens/HomeMapScreen.tsx` — full-bleed #EAF0E6 View, 15 absolutely positioned MapPinMarkers (derived from station coordinates), SearchBar top, FilterPills below, BottomStationCard bottom, ZoomControls, CenterActionButton
- [x] T027 [US1] Wire HomeMapScreen into RootNavigator as the first tab screen (Map tab)

**Checkpoint**: MVP complete — map screen renders with all elements, pin markers interactive, bottom card shows station details, navigation to Search and Station Detail works

---

## Phase 4: User Story 2 — Browse Station List (Priority: P1)

**Goal**: A scrollable list of all 15 stations in a FlatList with StationCard items, pull-to-refresh, skeleton placeholders on first load.

**Independent Test**: Tap the Station List tab and verify all 15 mock stations appear in a FlatList with StationCard items showing name, address, distance, charger count, and availability badge.

- [x] T028 [P] [US2] Create StationCard component at `apps/driver-mobile/src/components/StationCard.tsx` — name, address, distance, chargerCount/availableCount, availability Badge, rating display, onPress navigates to detail
- [x] T029 [US2] Create StationListScreen at `apps/driver-mobile/src/screens/StationListScreen.tsx` — FlatList of StationCard items, pull-to-refresh (no-op), Skeleton placeholders on first load, EmptyState support
- [x] T030 [US2] Wire StationListScreen into RootNavigator as the second tab screen (Stations List tab)

**Checkpoint**: Station List screen renders all 15 stations in scrollable FlatList, pull-to-refresh shows spinner, skeleton shown on first load

---

## Phase 5: User Story 3 — View Station Details (Priority: P2)

**Goal**: Full station detail screen with charger rows and review cards, empty states for missing chargers/reviews.

**Independent Test**: Navigate from map pin or station list card to the detail screen and verify station info header, ChargerRow FlatList with connector type/power/price/availability, and ReviewCard FlatList with author/rating/date/text.

- [x] T031 [P] [US3] Create ChargerRow component at `apps/driver-mobile/src/components/ChargerRow.tsx` — connector type label, power kW, price per kWh, availability StatusBadge
- [x] T032 [P] [US3] Create ReviewCard component at `apps/driver-mobile/src/components/ReviewCard.tsx` — author name, star rating (filled/empty stars), date, review text (supports Arabic/French content)
- [x] T033 [US3] Create StationDetailScreen at `apps/driver-mobile/src/screens/StationDetailScreen.tsx` — ScrollView with station header (name, address, rating, distance, charger count), FlatList of ChargerRows, FlatList of ReviewCards, EmptyState when no chargers or no reviews
- [x] T034 [US3] Wire StationDetailScreen into RootNavigator as a stack screen (pushed from map pin or station card)

**Checkpoint**: Station Detail screen shows full station info, chargers list, reviews list, empty states for missing data

---

## Phase 6: User Story 4 — Search and Filter Stations (Priority: P2)

**Goal**: Search stations by name/address with debounce, filter by charger type and availability, empty state when no results match.

**Independent Test**: Type a search query in the Search TextInput and verify the results list updates with debounce, then apply filter pills and verify further refinement. Clear the search and see all stations again.

- [x] T035 [P] [US4] Create `useMockFilter` hook at `apps/driver-mobile/src/hooks/useMockFilter.ts` — debounced search (300ms), charger type filter, availability filter
- [x] T036 [US4] Create SearchScreen at `apps/driver-mobile/src/screens/SearchScreen.tsx` — SearchBar at top, FilterPills below, FlatList of StationCard results, EmptyState when no matches
- [x] T037 [US4] Wire SearchScreen into RootNavigator as the third tab screen (Search tab)

**Checkpoint**: Search screen filters stations by text and charger type, EmptyState shown for no matches, clearing search restores all stations

---

## Phase 7: User Story 5 — Manage Favorites (Priority: P3)

**Goal**: Toggle favorite status on station cards with a heart icon. View favorited stations in the Favorites tab. Empty state when no favorites exist.

**Independent Test**: Tap the heart icon on any StationCard, verify it fills in, navigate to Favorites tab and confirm the station appears. Tap the heart again to unfavorite and confirm removal.

- [x] T038 [P] [US5] Create `useFavorites` hook at `apps/driver-mobile/src/hooks/useFavorites.ts` — React Context provider, favoriteStationIds state, toggleFavorite(id), isFavorite(id)
- [x] T039 [US5] Add heart icon and favorite toggle logic to StationCard component at `apps/driver-mobile/src/components/StationCard.tsx` — icon renders hollow/filled based on isFavorite() state, onPress calls toggleFavorite(id)
- [x] T040 [US5] Create FavoritesScreen at `apps/driver-mobile/src/screens/FavoritesScreen.tsx` — FlatList of favorited StationCards, EmptyState when no favorites
- [x] T041 [US5] Wire FavoritesScreen into RootNavigator as the fourth tab screen (Favorites tab)
- [x] T042 [US5] Wrap app in FavoritesProvider in `apps/driver-mobile/src/App.tsx`

**Checkpoint**: Favorites toggle works on all StationCards, Favorites tab shows favorited stations, unfavoriting removes them, EmptyState shown when none

---

## Phase 8: User Story 6 — Profile and Authentication (Priority: P3)

**Goal**: Static profile screen with pre-filled mock user data. Login/Register screen with email/password fields and social login buttons (Google, Apple, Facebook) — all visual-only.

**Independent Test**: Navigate to the Profile tab and verify avatar, name, email, and phone inputs are pre-filled. Tap "Login" to see the login/register screen with email/password fields and social login buttons.

- [x] T043 [P] [US6] Create ProfileScreen at `apps/driver-mobile/src/screens/ProfileScreen.tsx` — ScrollView with avatar placeholder, Input fields (name, email, phone) pre-filled with mock DriverUser data, "Login" button
- [x] T044 [P] [US6] Create LoginRegisterScreen at `apps/driver-mobile/src/screens/LoginRegisterScreen.tsx` — full-screen form with login/register tabs, email + password TextInputs, social login buttons (Google, Apple, Facebook) — all visual-only, no submission logic
- [x] T045 [US6] Wire ProfileScreen into RootNavigator as the fifth tab screen (Profile tab)
- [x] T046 [US6] Wire LoginRegisterScreen into RootNavigator as a stack screen (pushed from ProfileScreen)

**Checkpoint**: Profile screen shows mock user data, tapping Login navigates to login/register screen with all form fields and social buttons rendered

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Ensure RTL correctness, token compliance, build pass, and integration verification

- [x] T047 [P] Audit every file in `apps/driver-mobile/src/components/` — confirm each imports from `packages/ui/src/tokens/native.ts` and uses no inline style values for colors, spacing, typography, radius, or shadows
- [x] T048 [P] Verify RTL layout on all 7 screens — MobileTopBar, SearchBar, FilterPills, BottomStationCard, StationCard, Station Detail, Profile form align correctly in Arabic
- [x] T049 [P] Verify all static strings are present in both `ar.json` and `fr.json` — no missing translations
- [x] T050 [P] Verify `pnpm build` passes for `apps/driver-mobile` with zero warnings
- [x] T051 [P] Run cross-app consistency check — verify mock data shape matches `apps/driver-web` types
- [x] T052 [P] Update `AGENTS.md` sprint status to reflect completion
- [x] T053 [P] Verify no network calls are made — open app, check network inspector, confirm zero external requests
- [x] T054 [P] Add ESLint configuration for `apps/driver-mobile` and verify `pnpm lint` passes with zero warnings

**Checkpoint**: All 7 screens render in Arabic and French, RTL layout correct, build passes with zero warnings

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Stories (Phase 3-8)**: All depend on Foundational phase completion
  - User stories can proceed sequentially in priority order (P1 → P2 → P3)
  - US3 (Station Detail) can be built in parallel with US2 (Station List) since they share no file conflicts
  - US4 (Search) reuses FilterPills and StationCard from US2
  - US5 (Favorites) modifies StationCard (adds heart icon) — should come after US2
  - US6 (Profile & Auth) has no dependencies on other stories
- **Polish (Phase 9)**: Depends on all user stories being implemented

### User Story Dependencies

- **US1 (P1) — Map/Home**: Can start after Foundational — No deps on other stories
- **US2 (P1) — Station List**: Can start after Foundational — No deps on other stories
- **US3 (P2) — Station Detail**: Can start after Foundational — No deps on other stories (can run in parallel with US1, US2)
- **US4 (P2) — Search**: Can start after Foundational — reuses FilterPills (T019), StationCard (T028), SearchBar (T017)
- **US5 (P3) — Favorites**: Depends on StationCard (T028) — modifies it with heart icon
- **US6 (P3) — Profile & Auth**: Can start after Foundational — No deps on other stories

### Within Each User Story

- Shared components (FilterPills, StationCard) need to be built before stories that reuse them
- Each story's screen and components can be built within that story phase
- Story complete before moving to next priority

### Parallel Opportunities

- All Phase 1 [P] tasks can run in parallel (T002-T007)
- All Phase 2 [P] tasks can run in parallel (T009-T011, T014, T016-T019)
- US1 (T021-T025) — all 5 component tasks can run in parallel before T026 (screen assembly)
- US2 (T028) — StationCard must be built before T029 (StationListScreen)
- US3 (T031-T032) — ChargerRow and ReviewCard can run in parallel before T033 (DetailScreen)
- US1, US2, US3 can all be implemented in parallel (no file conflicts)
- US6 can be done entirely in parallel with all other stories

### Recommended Execution Order (Single Developer)

1. Phase 1: Setup (T001-T007)
2. Phase 2: Foundational (T008-T020)
3. Phase 3: US1 — Map/Home (T021-T027) ← MVP checkpoint
4. Phase 4: US2 — Station List (T028-T030)
5. Phase 5: US3 — Station Detail (T031-T034)
6. Phase 6: US4 — Search (T035-T037)
7. Phase 7: US5 — Favorites (T038-T042)
8. Phase 8: US6 — Profile & Auth (T043-T046)
9. Phase 9: Polish (T047-T052)

---

## Parallel Example: User Story 1

```bash
# Launch all 5 components for User Story 1 together:
Task: "Create MapPinMarker component at apps/driver-mobile/src/components/MapPinMarker.tsx"
Task: "Create ZoomControls component at apps/driver-mobile/src/components/ZoomControls.tsx"
Task: "Create BottomStationCard component at apps/driver-mobile/src/components/BottomStationCard.tsx"
Task: "Create CenterActionButton component at apps/driver-mobile/src/components/CenterActionButton.tsx"
Task: "Create SpecRow component at apps/driver-mobile/src/components/SpecRow.tsx"

# After all components done, assemble the screen:
Task: "Create HomeMapScreen at apps/driver-mobile/src/screens/HomeMapScreen.tsx"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup → scaffold Expo project
2. Complete Phase 2: Foundational → types, mocks, i18n, navigation, shared components
3. Complete Phase 3: User Story 1 → Map/Home screen with pins and bottom card
4. **STOP and VALIDATE**: Open app, verify map background + pins + bottom card + tab navigation
5. Deploy/demo if ready

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add US1 (Map/Home) → Test: open app, see map → Deploy/Demo (MVP!)
3. Add US2 (Station List) → Test: tap Stations tab, see list → Deploy/Demo
4. Add US3 (Station Detail) → Test: tap station, see details → Deploy/Demo
5. Add US4 (Search) → Test: search and filter → Deploy/Demo
6. Add US5 (Favorites) → Test: favorite/unfavorite → Deploy/Demo
7. Add US6 (Profile & Auth) → Test: profile form and login screen → Deploy/Demo
8. Polish → RTL verification, build pass → Final

### Parallel Team Strategy

With multiple developers:

1. Team completes Phase 1 + Phase 2 together
2. Once Foundational is done:
   - Developer A: US1 (Map/Home) + US4 (Search)
   - Developer B: US2 (Station List) + US5 (Favorites)
   - Developer C: US3 (Station Detail) + US6 (Profile & Auth)
3. Stories complete independently — minimal merge conflicts (different screen files)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- All visual values from `packages/ui/src/tokens/native.ts` — no hardcoded values
- All static strings in `ar.json` and `fr.json` — verify both languages
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
