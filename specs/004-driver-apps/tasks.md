# Tasks: Mobile & Web Driver Apps

**Input**: Design documents from `/specs/004-driver-apps/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api.md, quickstart.md

**Tests**: Test tasks included per constitution "Test-First (NON-NEGOTIABLE)" principle - 80%+ coverage target

**Organization**: Tasks are grouped by implementation phase (from plan.md), with user story mapping for traceability.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Mobile App**: `source/front/mobile-driver/`
- **Web App**: `source/front/web-driver/src/`
- **Shared**: `source/front/packages/`

---

## Phase 1: Project Setup (2 days)

**Purpose**: Initialize mobile and web driver apps with proper structure

- [ ] T001 Create mobile-driver app directory structure per plan in source/front/mobile-driver/
- [ ] T002 Initialize Expo SDK 54 project with TypeScript in source/front/mobile-driver/
- [ ] T003 Configure pnpm workspace dependencies in source/front/mobile-driver/package.json
- [ ] T004 [P] Install mobile dependencies (Zustand, React Query, reanimated v3, AsyncStorage, react-native-maps) in source/front/mobile-driver/
- [ ] T005 [P] Install web dependencies (React 19, Vite, Zustand, React Query, Leaflet, @bornemap/ui) in source/front/web-driver/
- [ ] T006 [P] Configure TypeScript strict mode in source/front/mobile-driver/tsconfig.json and source/front/web-driver/tsconfig.json
- [ ] T007 [P] Configure ESLint in source/front/mobile-driver/.eslintrc.cjs and source/front/web-driver/.eslintrc.cjs
- [ ] T008 [P] Configure Prettier in source/front/mobile-driver/.prettierrc and source/front/web-driver/.prettierrc
- [ ] T009 [P] Create app directory structure in source/front/mobile-driver/app/ (app/, components/, hooks/, services/, store/, theme/)
- [ ] T010 [P] Create source directory structure in source/front/web-driver/src/ (pages/, components/, hooks/, services/, store/)
- [ ] T011 [P] Configure NativeWind (Tailwind) for mobile app in source/front/mobile-driver/native.config.js
- [ ] T012 [P] Configure CSS variables from tokens package for web app in source/front/web-driver/index.css
- [ ] T013 Set up Expo Router v3 file-based routing in source/front/mobile-driver/app/_layout.tsx
- [ ] T014 Set up Vite configuration in source/front/web-driver/vite.config.ts
- [ ] T015 Test build process for mobile app (pnpm build) in source/front/mobile-driver/
- [ ] T016 Test build process for web app (pnpm build) in source/front/web-driver/

**Deliverables**: Both apps compile with zero errors, TypeScript strict mode passing, ESLint and Prettier configured

---

## Phase 2: Core Navigation & Routing (2 days)

**Purpose**: Implement navigation structure with ThemeProvider and reanimated transitions

- [ ] T017 Create ThemeProvider component using @bornemap/ui in source/front/mobile-driver/theme/ThemeProvider.tsx
- [ ] T018 Implement dark mode state management with Zustand in source/front/mobile-driver/store/useThemeStore.ts
- [ ] T019 Persist theme preference in AsyncStorage (mobile) and localStorage (web) in source/front/mobile-driver/store/useThemeStore.ts
- [ ] T020 Create MapScreen component using react-native-maps in source/front/mobile-driver/app/index.tsx
- [ ] T021 Create StationListScreen component using expo-router in source/front/mobile-driver/app/stations.tsx
- [ ] T022 Create StationDetailScreen component using expo-router in source/front/mobile-driver/app/station/[id].tsx
- [ ] T023 Configure bottom sheet component for station preview in source/front/mobile-driver/components/StationPreviewBottomSheet.tsx
- [ ] T024 Implement pull-to-refresh handler in source/front/mobile-driver/app/index.tsx
- [ ] T025 Implement pull-to-refresh handler in source/front/mobile-driver/app/stations.tsx
- [ ] T026 Configure reanimated for screen transitions in source/front/mobile-driver/app/_layout.tsx
- [ ] T027 Set up React Query client for data fetching in source/front/mobile-driver/services/queryClient.ts
- [ ] T028 Create useThemeStore hook for web app in source/front/web-driver/src/store/useThemeStore.ts
- [ ] T029 Set up theme initialization in source/front/web-driver/src/App.tsx
- [ ] T030 Create responsive layouts for mobile and desktop in source/front/web-driver/src/pages/index.tsx

**Deliverables**: Navigation structure complete, ThemeProvider working, pull-to-refresh functional, smooth transitions

---

## Phase 3: Map Integration (3 days)

**Purpose**: Implement map screens with markers and station preview

- [ ] T031 [P] Implement geolocation permission handling in source/front/mobile-driver/services/geolocation.ts
- [ ] T032 Create MapService for station data fetching in source/front/mobile-driver/services/mapService.ts
- [ ] T033 Create useMapStore for map state management in source/front/mobile-driver/store/useMapStore.ts
- [ ] T034 Initialize react-native-maps MapView in source/front/mobile-driver/app/index.tsx
- [ ] T035 Add markers for stations within search radius in source/front/mobile-driver/app/index.tsx
- [ ] T036 Implement map cluster badges (50m radius) in source/front/mobile-driver/app/index.tsx
- [ ] T037 Add station preview bottom sheet when marker tapped in source/front/mobile-driver/app/index.tsx
- [ ] T038 Implement map interaction handlers (pan, zoom) in source/front/mobile-driver/app/index.tsx
- [ ] T039 Handle map state in Zustand store in source/front/mobile-driver/store/useMapStore.ts
- [ ] T040 Add pull-to-refresh to map in source/front/mobile-driver/app/index.tsx
- [ ] T041 [P] Initialize Leaflet map in source/front/web-driver/src/pages/index.tsx
- [ ] T042 [P] Add OpenStreetMap tile layer in source/front/web-driver/src/pages/index.tsx
- [ ] T043 [P] Add markers for stations within search radius in source/front/web-driver/src/pages/index.tsx
- [ ] T044 [P] Add station preview modal when marker tapped in source/front/web-driver/src/pages/index.tsx
- [ ] T045 Implement responsive map sizing in source/front/web-driver/src/pages/index.tsx
- [ ] T046 Add pull-to-refresh to web map in source/front/web-driver/src/pages/index.tsx
- [ ] T047 Test performance with 1000+ markers in source/front/mobile-driver/app/index.tsx
- [ ] T048 Verify no marker flickering or jitter in source/front/mobile-driver/app/index.tsx
- [ ] T049 Test performance with 1000+ markers in source/front/web-driver/src/pages/index.tsx
- [ ] T050 Verify no marker flickering in source/front/web-driver/src/pages/index.tsx

**Deliverables**: Map rendering working on both platforms, markers show correct coordinates, pull-to-refresh functional, 60fps performance

---

## Phase 4: Station List & Search (3 days)

**Purpose**: Implement station list with pagination and search functionality

- [ ] T051 Create StationListService for API data fetching in source/front/mobile-driver/services/stationListService.ts
- [ ] T052 Create useStationStore for station data management in source/front/mobile-driver/store/useStationStore.ts
- [ ] T053 Implement pagination (page, per_page parameters) in source/front/mobile-driver/app/stations.tsx
- [ ] T054 Add pull-to-refresh functionality in source/front/mobile-driver/app/stations.tsx
- [ ] T055 Implement search bar with debouncing (300ms) in source/front/mobile-driver/app/stations.tsx
- [ ] T056 Connect search to OSM Nominatim geocoding API in source/front/mobile-driver/services/geocodingService.ts
- [ ] T057 Display search results with distance information in source/front/mobile-driver/app/stations.tsx
- [ ] T058 Handle empty search results (show empty state) in source/front/mobile-driver/app/stations.tsx
- [ ] T059 Show loading skeletons while fetching in source/front/mobile-driver/app/stations.tsx
- [ ] T060 Add haptic feedback on search button in source/front/mobile-driver/app/stations.tsx
- [ ] T061 Test search performance (<500ms target) in source/front/mobile-driver/app/stations.tsx
- [ ] T062 Test edge cases (invalid input, network error) in source/front/mobile-driver/app/stations.tsx
- [ ] T063 Create StationList component for web app in source/front/web-driver/src/pages/stations.tsx
- [ ] T064 Implement pagination controls in source/front/web-driver/src/pages/stations.tsx
- [ ] T065 Add pull-to-refresh functionality in source/front/web-driver/src/pages/stations.tsx
- [ ] T066 Implement search input with debouncing in source/front/web-driver/src/pages/stations.tsx
- [ ] T067 Connect search to OSM Nominatim API in source/front/web-driver/src/pages/stations.tsx
- [ ] T068 Display results with distance in source/front/web-driver/src/pages/stations.tsx
- [ ] T069 Handle empty results in source/front/web-driver/src/pages/stations.tsx
- [ ] T070 Test responsive design on different screen sizes in source/front/web-driver/src/pages/stations.tsx

**Deliverables**: Station list with pagination working, search returns results within 500ms, empty states display correctly, skeletons shown during loading, pull-to-refresh functional

---

## Phase 5: Station Details (3 days)

**Purpose**: Implement station detail screens with charger information

- [ ] T071 Create StationDetailService for station data fetching in source/front/mobile-driver/services/stationDetailService.ts
- [ ] T072 Create useStationStore update methods in source/front/mobile-driver/store/useStationStore.ts
- [ ] T073 Display station name, address, opening hours, amenities in source/front/mobile-driver/app/station/[id].tsx
- [ ] T074 Show charger information (type, connector count, availability) in source/front/mobile-driver/app/station/[id].tsx
- [ ] T075 Display pricing information in source/front/mobile-driver/app/station/[id].tsx
- [ ] T076 Add navigation button to external mapping app in source/front/mobile-driver/app/station/[id].tsx
- [ ] T077 Add map button to show station location in source/front/mobile-driver/app/station/[id].tsx
- [ ] T078 Load station images lazily (only when detail screen is visible) in source/front/mobile-driver/app/station/[id].tsx
- [ ] T079 Handle errors with contextual error UI in source/front/mobile-driver/app/station/[id].tsx
- [ ] T080 Add pull-to-refresh to detail screen in source/front/mobile-driver/app/station/[id].tsx
- [ ] T081 Test all UI elements render correctly in source/front/mobile-driver/app/station/[id].tsx
- [ ] T082 Create StationDetail page for web app in source/front/web-driver/src/pages/station/[id].tsx
- [ ] T083 Implement same UI as mobile (responsive) in source/front/web-driver/src/pages/station/[id].tsx
- [ ] T084 Add navigation and map buttons in source/front/web-driver/src/pages/station/[id].tsx
- [ ] T085 Test on different screen sizes in source/front/web-driver/src/pages/station/[id].tsx

**Deliverables**: Station detail screens complete on both platforms, charger information displays correctly, navigation button functional with error recovery, images load when visible, all edge cases handled

---

## Phase 6: Offline Support & Persistence (2 days)

**Purpose**: Implement offline caching for last 50 stations and theme persistence

- [ ] T086 Implement AsyncStorage for theme persistence in source/front/mobile-driver/store/useThemeStore.ts
- [ ] T087 Create offline cache service for last 50 stations in source/front/mobile-driver/services/offlineCacheService.ts
- [ ] T088 Save recently viewed stations to cache in source/front/mobile-driver/services/offlineCacheService.ts
- [ ] T089 Load cached stations when offline in source/front/mobile-driver/services/offlineCacheService.ts
- [ ] T090 Show cached data with offline indicator in source/front/mobile-driver/app/index.tsx
- [ ] T091 Update cache when network available in source/front/mobile-driver/services/offlineCacheService.ts
- [ ] T092 Mark cache as stale when network returns in source/front/mobile-driver/services/offlineCacheService.ts
- [ ] T093 Test offline scenarios (network down, slow network) in source/front/mobile-driver/services/offlineCacheService.ts
- [ ] T094 Verify cache invalidation in source/front/mobile-driver/services/offlineCacheService.ts
- [ ] T095 Implement localStorage for theme persistence in source/front/web-driver/src/store/useThemeStore.ts
- [ ] T096 Create offline cache (same logic as mobile) in source/front/web-driver/src/services/offlineCacheService.ts
- [ ] T097 Save recently viewed stations in source/front/web-driver/src/services/offlineCacheService.ts
- [ ] T098 Load cached data when offline in source/front/web-driver/src/services/offlineCacheService.ts
- [ ] T099 Test offline scenarios in source/front/web-driver/src/services/offlineCacheService.ts

**Deliverables**: Theme preference persists across app restarts, last 50 stations cached, offline mode works correctly, cache refreshes when network returns

---

## Phase 7: Error Handling & UX (2 days)

**Purpose**: Implement comprehensive error handling with recovery actions

- [ ] T100 Create ErrorBoundary component using @bornemap/ui in source/front/mobile-driver/components/ErrorBoundary.tsx
- [ ] T101 Create error screens for network errors with retry button in source/front/mobile-driver/components/ErrorScreen.tsx
- [ ] T102 Create error screens for geocoding failures with fallback in source/front/mobile-driver/components/ErrorScreen.tsx
- [ ] T103 Create error screens for API errors with retry button in source/front/mobile-driver/components/ErrorScreen.tsx
- [ ] T104 Create error screens for invalid data errors in source/front/mobile-driver/components/ErrorScreen.tsx
- [ ] T105 Implement error messages with copy-to-clipboard for addresses in source/front/mobile-driver/components/ErrorScreen.tsx
- [ ] T106 Add haptic feedback on all primary actions in source/front/mobile-driver/app/index.tsx
- [ ] T107 Add haptic feedback on all primary actions in source/front/mobile-driver/app/stations.tsx
- [ ] T108 Test error recovery paths in source/front/mobile-driver/app/index.tsx
- [ ] T109 Verify all error screens use skeleton/empty states in source/front/mobile-driver/components/ErrorScreen.tsx
- [ ] T110 Test with network errors in source/front/mobile-driver/app/index.tsx
- [ ] T111 Test with invalid responses in source/front/mobile-driver/app/index.tsx
- [ ] T112 Create error components for web app in source/front/web-driver/src/components/ErrorScreen.tsx
- [ ] T113 Implement same error handling patterns in source/front/web-driver/src/pages/index.tsx
- [ ] T114 Test responsive error screens in source/front/web-driver/src/components/ErrorScreen.tsx

**Deliverables**: ErrorBoundary catches runtime errors, all error paths have recovery actions, error messages are clear and actionable, no blank error screens

---

## Phase 8: Theme Implementation (2 days)

**Purpose**: Implement dark mode with persistent theme preference

- [ ] T115 Verify ThemeProvider works with @bornemap/ui in source/front/mobile-driver/theme/ThemeProvider.tsx
- [ ] T116 Test dark mode toggle functionality in source/front/mobile-driver/app/index.tsx
- [ ] T117 Verify theme persistence (AsyncStorage) in source/front/mobile-driver/store/useThemeStore.ts
- [ ] T118 Test theme transitions (smooth 300ms) in source/front/mobile-driver/app/index.tsx
- [ ] T119 Verify all screens render correctly in both themes in source/front/mobile-driver/app/index.tsx
- [ ] T120 Check WCAG AA contrast ratios in source/front/mobile-driver/app/index.tsx
- [ ] T121 Test with real devices (iOS and Android) in source/front/mobile-driver/app/index.tsx
- [ ] T122 Handle system theme changes in source/front/mobile-driver/app/index.tsx
- [ ] T123 Verify theme implementation with @bornemap/ui in source/front/web-driver/src/components/ThemeProvider.tsx
- [ ] T124 Test dark mode toggle functionality in source/front/web-driver/src/App.tsx
- [ ] T125 Verify theme persistence (localStorage) in source/front/web-driver/src/store/useThemeStore.ts
- [ ] T126 Test theme transitions in source/front/web-driver/src/App.tsx
- [ ] T127 Verify all screens render correctly in both themes in source/front/web-driver/src/App.tsx
- [ ] T128 Check contrast ratios in source/front/web-driver/src/App.tsx
- [ ] T129 Test responsive design in both themes in source/front/web-driver/src/App.tsx

**Deliverables**: Dark mode works perfectly on all screens, theme preference persists, smooth theme transitions, WCAG AA contrast thresholds met

---

## Phase 9: Testing & Quality Assurance (3 days)

**Purpose**: Comprehensive testing to ensure quality and meet success metrics

### Unit Tests (Mobile)

- [ ] T130 [P] Create test files for mobile components in source/front/mobile-driver/__tests__/components/
- [ ] T131 [P] Create test files for mobile hooks in source/front/mobile-driver/__tests__/hooks/
- [ ] T132 [P] Test Zustand stores (theme, station, map) in source/front/mobile-driver/__tests__/store/
- [ ] T133 [P] Test React Query hooks in source/front/mobile-driver/__tests__/hooks/
- [ ] T134 [P] Test service layer (API calls, geocoding) in source/front/mobile-driver/__tests__/services/
- [ ] T135 [P] Test error handlers in source/front/mobile-driver/__tests__/services/
- [ ] T136 [P] Create test files for web components in source/front/web-driver/src/__tests__/components/
- [ ] T137 [P] Create test files for web hooks in source/front/web-driver/src/__tests__/hooks/
- [ ] T138 [P] Test Zustand stores (theme, station, map) in source/front/web-driver/src/__tests__/store/
- [ ] T139 [P] Test React Query hooks in source/front/web-driver/src/__tests__/hooks/
- [ ] T140 [P] Test service layer (API calls, geocoding) in source/front/web-driver/src/__tests__/services/
- [ ] T141 [P] Test error handlers in source/front/web-driver/src/__tests__/services/
- [ ] T142 Achieve 80%+ coverage on critical paths (test scripts)

### Integration Tests

- [ ] T143 [P] Test station list loading in source/front/mobile-driver/__tests__/integration/
- [ ] T144 [P] Test search functionality in source/front/mobile-driver/__tests__/integration/
- [ ] T145 [P] Test navigation flows in source/front/mobile-driver/__tests__/integration/
- [ ] T146 [P] Test station detail page in source/front/mobile-driver/__tests__/integration/
- [ ] T147 [P] Test pagination in source/front/mobile-driver/__tests__/integration/
- [ ] T148 [P] Test pull-to-refresh in source/front/mobile-driver/__tests__/integration/
- [ ] T149 [P] Test offline scenarios in source/front/mobile-driver/__tests__/integration/

### Manual Testing

- [ ] T150 Test on iOS device (iPhone 13) in source/front/mobile-driver/app/index.tsx
- [ ] T151 Test on Android device (Samsung Galaxy) in source/front/mobile-driver/app/index.tsx
- [ ] T152 Test on different screen sizes (mobile, tablet, desktop) in source/front/web-driver/src/pages/index.tsx
- [ ] T153 Test pull-to-refresh gestures in source/front/mobile-driver/app/index.tsx
- [ ] T154 Test bottom sheet gestures (swipe to dismiss) in source/front/mobile-driver/app/index.tsx
- [ ] T155 Test dark mode on all screens in source/front/mobile-driver/app/index.tsx
- [ ] T156 Test haptic feedback on primary actions in source/front/mobile-driver/app/index.tsx
- [ ] T160 [P] Verify all primary CTAs have haptic feedback in source/front/mobile-driver/app/stations.tsx
- [ ] T157 Test map interactions (pan, zoom, marker tap) in source/front/mobile-driver/app/index.tsx
- [ ] T158 Test error recovery flows in source/front/mobile-driver/app/index.tsx
- [ ] T159 Test navigation to external apps in source/front/mobile-driver/app/station/[id].tsx

### Performance Testing

- [ ] T160 Measure first screen load time (<3s target) in source/front/mobile-driver/app/index.tsx
- [ ] T161 Measure station list fetch time (<200ms target) in source/front/mobile-driver/app/stations.tsx
- [ ] T162 Measure search query time (<500ms target) in source/front/mobile-driver/app/stations.tsx
- [ ] T163 Measure station detail load time (<200ms target) in source/front/mobile-driver/app/station/[id].tsx
- [ ] T164 Test with 1000+ stations on map (no jitter) in source/front/mobile-driver/app/index.tsx
- [ ] T165 Measure bundle size (target: <5MB for mobile) in source/front/mobile-driver/
- [ ] T166 Measure bundle size (target: <200KB for web) in source/front/web-driver/
- [ ] T167 Profile memory usage in source/front/mobile-driver/app/index.tsx
- [ ] T168 [P] Verify map interaction latency is <16.67ms (60fps) in source/front/mobile-driver/app/index.tsx
- [ ] T169 [P] Verify map interaction latency is <16.67ms (60fps) in source/front/web-driver/src/pages/index.tsx

**Deliverables**: Test suite passing, manual testing checklist passed, performance targets met, no crashes on real devices, all critical paths tested

---

## Phase 10: Documentation & Deployment Prep (1 day)

**Purpose**: Create documentation and prepare for deployment

### Documentation

- [ ] T168 Create README for mobile-driver app in source/front/mobile-driver/README.md
- [ ] T169 Create README for web-driver app in source/front/web-driver/README.md
- [ ] T170 Document build commands in source/front/mobile-driver/README.md
- [ ] T171 Document setup instructions in source/front/mobile-driver/README.md
- [ ] T172 Document API integration in source/front/mobile-driver/README.md
- [ ] T173 Document configuration (env variables) in source/front/mobile-driver/.env.example
- [ ] T174 Document build commands in source/front/web-driver/README.md
- [ ] T175 Document setup instructions in source/front/web-driver/README.md
- [ ] T176 Document API integration in source/front/web-driver/README.md
- [ ] T177 Document configuration (env variables) in source/front/web-driver/.env.example

### Deployment Prep

- [ ] T178 Test iOS build process (eas build) in source/front/mobile-driver/
- [ ] T179 Test Android build process (eas build) in source/front/mobile-driver/
- [ ] T180 Test web build process (pnpm build) in source/front/web-driver/
- [ ] T181 Verify production builds work correctly

**Deliverables**: READMEs complete, build scripts working, production builds tested

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Core Navigation (Phase 2)**: Depends on Setup completion (T016) - BLOCKS all user stories
- **Map Integration (Phase 3)**: Depends on Core Navigation (T026) - Can start after Phase 2
- **Station List & Search (Phase 4)**: Depends on Core Navigation (T027) - Can start after Phase 2
- **Station Details (Phase 5)**: Depends on Core Navigation (T027) - Can start after Phase 2
- **Offline Support (Phase 6)**: Depends on Core Navigation (T027) - Can start after Phase 2
- **Error Handling (Phase 7)**: Depends on Core Navigation (T027) - Can start after Phase 2
- **Theme Implementation (Phase 8)**: Depends on Core Navigation (T027) - Can start after Phase 2
- **Testing (Phase 9)**: Depends on all implementation phases complete
- **Documentation (Phase 10)**: Depends on all previous phases complete

### User Story Mapping

- **User Story 1 (Discover Stations via Map) [P1]**: Covered by Phases 2, 3 (T017-T050)
- **User Story 2 (Search Stations by Location or Name) [P1]**: Covered by Phases 2, 4 (T017-T070)
- **User Story 3 (View Station Details & Chargers) [P1]**: Covered by Phases 2, 5 (T017-T085)
- **User Story 4 (Navigate to Station) [P2]**: Covered by Phase 5 (T076, T084)
- **User Story 5 (Switch Between Light and Dark Mode) [P2]**: Covered by Phase 8 (T115-T129)
- **User Story 6 (Refresh Data and Load More) [P2]**: Covered by Phase 2 (T024, T025), Phase 4 (T054, T065)
- **User Story 7 (Responsive Web Version) [P3]**: Covered by Phase 2 (T030), Phase 3 (T041-T046), Phase 4 (T063-T070), Phase 5 (T082-T085)

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel (T004, T005, T006, T007, T008, T009, T010, T011, T012)
- All Core Navigation tasks marked [P] can run in parallel (T017-T019, T021-T022)
- All Map Integration tasks marked [P] can run in parallel (T031-T040, T041-T050)
- All Station List tasks marked [P] can run in parallel (T051-T062, T063-T070)
- All Station Details tasks marked [P] can run in parallel (T071-T081, T082-T085)
- All Offline Support tasks marked [P] can run in parallel (T086-T094, T095-T099)
- All Error Handling tasks marked [P] can run in parallel (T100-T109, T112-T114)
- All Theme tasks marked [P] can run in parallel (T115-T122, T123-T129)
- All Unit Test tasks marked [P] can run in parallel (T130-T141)
- All Integration Test tasks marked [P] can run in parallel (T143-T149)

### Implementation Strategy

### MVP First (User Stories 1-3 Only)

1. Complete Phase 1: Setup (T001-T016)
2. Complete Phase 2: Core Navigation (T017-T030)
3. Complete Phase 3: Map Integration (T031-T050)
4. Complete Phase 4: Station List & Search (T051-T070)
5. Complete Phase 5: Station Details (T071-T085)
6. **STOP and VALIDATE**: Test User Stories 1-3 independently
7. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Core Navigation → Foundation ready
2. Add User Story 1 (Map Integration) → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 (Station List & Search) → Test independently → Deploy/Demo
4. Add User Story 3 (Station Details) → Test independently → Deploy/Demo
5. Add User Stories 4-7 (Navigation, Dark Mode, Offline, Web) → Test independently → Deploy/Demo
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Core Navigation together
2. Once Core Navigation is done:
   - Developer A: Map Integration (T031-T050)
   - Developer B: Station List & Search (T051-T070)
   - Developer C: Station Details (T071-T085)
3. Stories complete and integrate independently
4. Remaining developers work on Offline Support, Error Handling, Theme, Testing

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before implementing (per constitution "Test-First" principle)
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence

---

## MVP Scope Summary

**MVP Minimum Viable Product**: User Stories 1-3 (Phases 1-5)

**Scope**:
- Mobile driver app with map, station list, and station details
- Web driver app with same functionality (responsive)
- Dark mode support
- Skeleton screens
- Pull-to-refresh
- Theme persistence

**Timeline**: 13 days (Phases 1-5)

**Out of MVP Scope**:
- Offline caching (Phase 6)
- Navigation to station (Phase 5 partial - button only)
- Comprehensive error handling (Phase 7)
- Web app specific optimization (Phase 3-5)
- Full testing suite (Phase 9)
- Documentation (Phase 10)

**Post-MVP** (Phases 6-10):
- Offline support (cache last 50 stations)
- Complete navigation functionality
- Comprehensive error handling
- Web app responsive optimizations
- Full test suite (80%+ coverage)
- Documentation and deployment prep
