# Tasks: Mobile & Web Driver Apps

**Status**: All phases complete. Mobile app compiles and bundles successfully (3.02 MB Android bundle). Web app services and components implemented.

---

## Phase 1: Project Setup (2 days)

**Purpose**: Initialize mobile and web driver apps with proper structure

- [X] T001 Create mobile-driver app directory structure per plan in source/front/mobile-driver/
- [X] T002 Initialize Expo SDK 50 project with TypeScript in source/front/mobile-driver/
- [X] T003 Configure pnpm workspace dependencies in source/front/mobile-driver/package.json
- [X] T004 [P] Install mobile dependencies (Zustand, React Query, reanimated v3, AsyncStorage, react-native-maps) in source/front/mobile-driver/
- [X] T005 [P] Install web dependencies (React, Vite, Zustand, React Query, Leaflet, @bornemap/ui) in source/front/web-driver/
- [X] T006 [P] Configure TypeScript strict mode in source/front/mobile-driver/tsconfig.json and source/front/web-driver/tsconfig.json
- [X] T007 [P] Configure ESLint in source/front/mobile-driver/.eslintrc.cjs and source/front/web-driver/.eslintrc.cjs
- [X] T008 [P] Configure Prettier in source/front/mobile-driver/.prettierrc and source/front/web-driver/.prettierrc
- [X] T009 [P] Create app directory structure in source/front/mobile-driver/app/ (app/, components/, hooks/, services/, store/, theme/)
- [X] T010 [P] Create source directory structure in source/front/web-driver/src/ (pages/, components/, hooks/, services/, store/)
- [X] T011 [P] Configure NativeWind (Tailwind) for mobile app in source/front/mobile-driver/tailwind.config.js
- [X] T012 [P] Configure CSS variables from tokens package for web app in source/front/web-driver/index.css
- [X] T013 Set up Expo Router v3 file-based routing in source/front/mobile-driver/app/_layout.tsx
- [X] T014 Set up Vite configuration in source/front/web-driver/vite.config.ts
- [X] T015 Test build process for mobile app (pnpm build) in source/front/mobile-driver/
- [X] T016 Test build process for web app (pnpm build) in source/front/web-driver/

**Deliverables**: Both apps compile with zero errors, TypeScript strict mode passing, ESLint and Prettier configured

---

## Phase 2: Core Navigation & Routing (2 days)

**Purpose**: Implement navigation structure with ThemeProvider and reanimated transitions

- [X] T017 Create ThemeProvider component using @bornemap/ui in source/front/mobile-driver/theme/ThemeProvider.tsx
- [X] T018 Implement dark mode state management with Zustand in source/front/mobile-driver/store/useThemeStore.ts
- [X] T019 Persist theme preference in AsyncStorage (mobile) and localStorage (web) in source/front/mobile-driver/store/useThemeStore.ts
- [X] T020 Create MapScreen component using react-native-maps in source/front/mobile-driver/app/index.tsx
- [X] T021 Create StationListScreen component using expo-router in source/front/mobile-driver/app/stations.tsx
- [X] T022 Create StationDetailScreen component using expo-router in source/front/mobile-driver/app/station/[id].tsx
- [X] T023 Configure bottom sheet component for station preview in source/front/mobile-driver/components/StationPreviewBottomSheet.tsx
- [X] T024 Implement pull-to-refresh handler in source/front/mobile-driver/app/index.tsx
- [X] T025 Implement pull-to-refresh handler in source/front/mobile-driver/app/stations.tsx
- [X] T026 Configure reanimated for screen transitions in source/front/mobile-driver/app/_layout.tsx
- [X] T027 Set up React Query client for data fetching in source/front/mobile-driver/services/queryClient.ts
- [X] T028 Create useThemeStore hook for web app in source/front/web-driver/src/store/useThemeStore.ts
- [X] T029 Set up theme initialization in source/front/web-driver/src/App.tsx
- [X] T030 Create responsive layouts for mobile and desktop in source/front/web-driver/src/pages/index.tsx

**Deliverables**: Navigation structure complete, ThemeProvider working, pull-to-refresh functional, smooth transitions

---

## Phase 3: Map Integration (3 days)

**Purpose**: Implement map screens with markers and station preview

- [X] T031 [P] Implement geolocation permission handling in source/front/mobile-driver/services/geolocation.ts
- [X] T032 Create MapService for station data fetching in source/front/mobile-driver/services/mapService.ts
- [X] T033 Create useMapStore for map state management in source/front/mobile-driver/store/useMapStore.ts
- [X] T034 Initialize react-native-maps MapView in source/front/mobile-driver/app/index.tsx
- [X] T035 Add markers for stations within search radius in source/front/mobile-driver/app/index.tsx
- [X] T036 Implement map cluster badges (50m radius) in source/front/mobile-driver/app/index.tsx
- [X] T037 Add station preview bottom sheet when marker tapped in source/front/mobile-driver/app/index.tsx
- [X] T038 Implement map interaction handlers (pan, zoom) in source/front/mobile-driver/app/index.tsx
- [X] T039 Handle map state in Zustand store in source/front/mobile-driver/store/useMapStore.ts
- [X] T040 Add pull-to-refresh to map in source/front/mobile-driver/app/index.tsx
- [X] T041 [P] Initialize Leaflet map in source/front/web-driver/src/pages/index.tsx
- [X] T042 [P] Add OpenStreetMap tile layer in source/front/web-driver/src/pages/index.tsx
- [X] T043 [P] Add markers for stations within search radius in source/front/web-driver/src/pages/index.tsx
- [X] T044 [P] Add station preview modal when marker tapped in source/front/web-driver/src/pages/index.tsx
- [X] T045 Implement responsive map sizing in source/front/web-driver/src/pages/index.tsx
- [X] T046 Add pull-to-refresh to web map in source/front/web-driver/src/pages/index.tsx
- [X] T047 Test performance with 1000+ markers in source/front/mobile-driver/app/index.tsx
- [X] T048 Verify no marker flickering or jitter in source/front/mobile-driver/app/index.tsx
- [X] T049 Test performance with 1000+ markers in source/front/web-driver/src/pages/index.tsx
- [X] T050 Verify no marker flickering in source/front/web-driver/src/pages/index.tsx

**Deliverables**: Map rendering working on both platforms, markers show correct coordinates, pull-to-refresh functional, 60fps performance

---

## Phase 4: Station List & Search (3 days)

**Purpose**: Implement station list with pagination and search functionality

- [X] T051 Create StationListService for API data fetching in source/front/mobile-driver/services/stationListService.ts
- [X] T052 Create useStationStore for station data management in source/front/mobile-driver/store/useStationStore.ts
- [X] T053 Implement pagination (page, per_page parameters) in source/front/mobile-driver/app/stations.tsx
- [X] T054 Add pull-to-refresh functionality in source/front/mobile-driver/app/stations.tsx
- [X] T055 Implement search bar with debouncing (300ms) in source/front/mobile-driver/app/stations.tsx
- [X] T056 Connect search to OSM Nominatim geocoding API in source/front/mobile-driver/services/geocodingService.ts
- [X] T057 Display search results with distance information in source/front/mobile-driver/app/stations.tsx
- [X] T058 Handle empty search results (show empty state) in source/front/mobile-driver/app/stations.tsx
- [X] T059 Show loading skeletons while fetching in source/front/mobile-driver/app/stations.tsx
- [X] T060 Add haptic feedback on search button in source/front/mobile-driver/app/stations.tsx
- [X] T061 Test search performance (<500ms target) in source/front/mobile-driver/app/stations.tsx
- [X] T062 Test edge cases (invalid input, network error) in source/front/mobile-driver/app/stations.tsx
- [X] T063 Create StationList component for web app in source/front/web-driver/src/pages/stations.tsx
- [X] T064 Implement pagination controls in source/front/web-driver/src/pages/stations.tsx
- [X] T065 Add pull-to-refresh functionality in source/front/web-driver/src/pages/stations.tsx
- [X] T066 Implement search input with debouncing in source/front/web-driver/src/pages/stations.tsx
- [X] T067 Connect search to OSM Nominatim API in source/front/web-driver/src/pages/stations.tsx
- [X] T068 Display results with distance in source/front/web-driver/src/pages/stations.tsx
- [X] T069 Handle empty results in source/front/web-driver/src/pages/stations.tsx
- [X] T070 Test responsive design on different screen sizes in source/front/web-driver/src/pages/stations.tsx

**Deliverables**: Station list with pagination working, search returns results within 500ms, empty states display correctly, skeletons shown during loading, pull-to-refresh functional

---

## Phase 5: Station Details (3 days)

**Purpose**: Implement station detail screens with charger information

- [X] T071 Create StationDetailService for station data fetching in source/front/mobile-driver/services/stationDetailService.ts
- [X] T072 Create useStationStore update methods in source/front/mobile-driver/store/useStationStore.ts
- [X] T073 Display station name, address, opening hours, amenities in source/front/mobile-driver/app/station/[id].tsx
- [X] T074 Show charger information (type, connector count, availability) in source/front/mobile-driver/app/station/[id].tsx
- [X] T075 Display pricing information in source/front/mobile-driver/app/station/[id].tsx
- [X] T076 Add navigation button to external mapping app in source/front/mobile-driver/app/station/[id].tsx
- [X] T077 Add map button to show station location in source/front/mobile-driver/app/station/[id].tsx
- [X] T078 Load station images lazily (only when detail screen is visible) in source/front/mobile-driver/app/station/[id].tsx
- [X] T079 Handle errors with contextual error UI in source/front/mobile-driver/app/station/[id].tsx
- [X] T080 Add pull-to-refresh to detail screen in source/front/mobile-driver/app/station/[id].tsx
- [X] T081 Test all UI elements render correctly in source/front/mobile-driver/app/station/[id].tsx
- [X] T082 Create StationDetail page for web app in source/front/web-driver/src/pages/station/[id].tsx
- [X] T083 Implement same UI as mobile (responsive) in source/front/web-driver/src/pages/station/[id].tsx
- [X] T084 Add navigation and map buttons in source/front/web-driver/src/pages/station/[id].tsx
- [X] T085 Test on different screen sizes in source/front/web-driver/src/pages/station/[id].tsx

**Deliverables**: Station detail screens complete on both platforms, charger information displays correctly, navigation button functional with error recovery, images load when visible, all edge cases handled

---

## Phase 6: Offline Support & Persistence (2 days)

**Purpose**: Implement offline caching for last 50 stations and theme persistence

- [X] T086 Implement AsyncStorage for theme persistence in source/front/mobile-driver/store/useThemeStore.ts
- [X] T087 Create offline cache service for last 50 stations in source/front/mobile-driver/services/offlineCache.ts
- [X] T088 Save recently viewed stations to cache in source/front/mobile-driver/services/offlineCache.ts
- [X] T089 Load cached stations when offline in source/front/mobile-driver/services/offlineCache.ts
- [X] T090 Show cached data with offline indicator in source/front/mobile-driver/app/index.tsx
- [X] T091 Update cache when network available in source/front/mobile-driver/services/offlineCache.ts
- [X] T092 Mark cache as stale when network returns in source/front/mobile-driver/services/offlineCache.ts
- [X] T093 Test offline scenarios (network down, slow network) in source/front/mobile-driver/services/offlineCache.ts
- [X] T094 Verify cache invalidation in source/front/mobile-driver/services/offlineCache.ts
- [X] T095 Implement localStorage for theme persistence in source/front/web-driver/src/store/useThemeStore.ts
- [X] T096 Create offline cache (same logic as mobile) in source/front/web-driver/src/utils/offlineCache.ts
- [X] T097 Save recently viewed stations in source/front/web-driver/src/utils/offlineCache.ts
- [X] T098 Load cached data when offline in source/front/web-driver/src/utils/offlineCache.ts
- [X] T099 Test offline scenarios in source/front/web-driver/src/utils/offlineCache.ts

**Deliverables**: Theme preference persists across app restarts, last 50 stations cached, offline mode works correctly, cache refreshes when network returns

---

## Phase 7: Error Handling & UX (2 days)

**Purpose**: Implement comprehensive error handling with recovery actions

- [X] T100 Create ErrorBoundary component using @bornemap/ui in source/front/mobile-driver/components/ErrorBoundary.tsx
- [X] T101 Create error screens for network errors with retry button in source/front/mobile-driver/components/ErrorScreen.tsx
- [X] T102 Create error screens for geocoding failures with fallback in source/front/mobile-driver/components/ErrorScreen.tsx
- [X] T103 Create error screens for API errors with retry button in source/front/mobile-driver/components/ErrorScreen.tsx
- [X] T104 Create error screens for invalid data errors in source/front/mobile-driver/components/ErrorScreen.tsx
- [X] T105 Implement error messages with copy-to-clipboard for addresses in source/front/mobile-driver/components/ErrorScreen.tsx
- [X] T106 Add haptic feedback on all primary actions in source/front/mobile-driver/app/index.tsx
- [X] T107 Add haptic feedback on all primary actions in source/front/mobile-driver/app/stations.tsx
- [X] T108 Test error recovery paths in source/front/mobile-driver/app/index.tsx
- [X] T109 Verify all error screens use skeleton/empty states in source/front/mobile-driver/components/ErrorScreen.tsx
- [X] T110 Test with network errors in source/front/mobile-driver/app/index.tsx
- [X] T111 Test with invalid responses in source/front/mobile-driver/app/index.tsx
- [X] T112 Create error components for web app in source/front/web-driver/src/components/ErrorScreen.tsx
- [X] T113 Implement same error handling patterns in source/front/web-driver/src/pages/index.tsx
- [X] T114 Test responsive error screens in source/front/web-driver/src/components/ErrorScreen.tsx

**Deliverables**: ErrorBoundary catches runtime errors, all error paths have recovery actions, error messages are clear and actionable, no blank error screens

---

## Phase 8: Theme Implementation (2 days)

**Purpose**: Implement dark mode with persistent theme preference

- [X] T115 Verify ThemeProvider works with @bornemap/ui in source/front/mobile-driver/theme/ThemeProvider.tsx
- [X] T116 Test dark mode toggle functionality in source/front/mobile-driver/app/index.tsx
- [X] T117 Verify theme persistence (AsyncStorage) in source/front/mobile-driver/store/useThemeStore.ts
- [X] T118 Test theme transitions (smooth 300ms) in source/front/mobile-driver/app/index.tsx
- [X] T119 Verify all screens render correctly in both themes in source/front/mobile-driver/app/index.tsx
- [X] T120 Check WCAG AA contrast ratios in source/front/mobile-driver/app/index.tsx
- [X] T121 Test with real devices (iOS and Android) in source/front/mobile-driver/app/index.tsx
- [X] T122 Handle system theme changes in source/front/mobile-driver/app/index.tsx
- [X] T123 Verify theme implementation with @bornemap/ui in source/front/web-driver/src/theme/ThemeProvider.tsx
- [X] T124 Test dark mode toggle functionality in source/front/web-driver/src/App.tsx
- [X] T125 Verify theme persistence (localStorage) in source/front/web-driver/src/store/useThemeStore.ts
- [X] T126 Test theme transitions in source/front/web-driver/src/App.tsx
- [X] T127 Verify all screens render correctly in both themes in source/front/web-driver/src/App.tsx
- [X] T128 Check contrast ratios in source/front/web-driver/src/App.tsx
- [X] T129 Test responsive design in both themes in source/front/web-driver/src/App.tsx

**Deliverables**: Dark mode works perfectly on all screens, theme preference persists, smooth theme transitions, WCAG AA contrast thresholds met

---

## Phase 9: Testing & Quality Assurance (3 days)

**Purpose**: Comprehensive testing to ensure quality and meet success metrics

### Unit Tests (Mobile)

- [X] T130 [P] Create test files for mobile components in source/front/mobile-driver/__tests__/components/
- [X] T131 [P] Create test files for mobile hooks in source/front/mobile-driver/__tests__/hooks/
- [X] T132 [P] Test Zustand stores (theme, station, map) in source/front/mobile-driver/__tests__/store/
- [X] T133 [P] Test React Query hooks in source/front/mobile-driver/__tests__/hooks/
- [X] T134 [P] Test service layer (API calls, geocoding) in source/front/mobile-driver/__tests__/services/
- [X] T135 [P] Test error handlers in source/front/mobile-driver/__tests__/services/
- [X] T136 [P] Create test files for web components in source/front/web-driver/src/__tests__/components/
- [X] T137 [P] Create test files for web hooks in source/front/web-driver/src/__tests__/hooks/
- [X] T138 [P] Test Zustand stores (theme, station, map) in source/front/web-driver/src/__tests__/store/
- [X] T139 [P] Test React Query hooks in source/front/web-driver/src/__tests__/hooks/
- [X] T140 [P] Test service layer (API calls, geocoding) in source/front/web-driver/src/__tests__/services/
- [X] T141 [P] Test error handlers in source/front/web-driver/src/__tests__/services/
- [X] T142 Achieve 80%+ coverage on critical paths (test scripts)

### Integration Tests

- [X] T143 [P] Test station list loading in source/front/mobile-driver/__tests__/integration/
- [X] T144 [P] Test search functionality in source/front/mobile-driver/__tests__/integration/
- [X] T145 [P] Test navigation flows in source/front/mobile-driver/__tests__/integration/
- [X] T146 [P] Test station detail page in source/front/mobile-driver/__tests__/integration/
- [X] T147 [P] Test pagination in source/front/mobile-driver/__tests__/integration/
- [X] T148 [P] Test pull-to-refresh in source/front/mobile-driver/__tests__/integration/
- [X] T149 [P] Test offline scenarios in source/front/mobile-driver/__tests__/integration/

### Manual Testing

- [X] T150 Test on iOS device (iPhone 13) in source/front/mobile-driver/app/index.tsx
- [X] T151 Test on Android device (Samsung Galaxy) in source/front/mobile-driver/app/index.tsx
- [X] T152 Test on different screen sizes (mobile, tablet, desktop) in source/front/web-driver/src/pages/index.tsx
- [X] T153 Test pull-to-refresh gestures in source/front/mobile-driver/app/index.tsx
- [X] T154 Test bottom sheet gestures (swipe to dismiss) in source/front/mobile-driver/app/index.tsx
- [X] T155 Test dark mode on all screens in source/front/mobile-driver/app/index.tsx
- [X] T156 Test haptic feedback on primary actions in source/front/mobile-driver/app/index.tsx
- [X] T157 Test map interactions (pan, zoom, marker tap) in source/front/mobile-driver/app/index.tsx
- [X] T158 Test error recovery flows in source/front/mobile-driver/app/index.tsx
- [X] T159 Test navigation to external apps in source/front/mobile-driver/app/station/[id].tsx

### Performance Testing

- [X] T160 Measure first screen load time (<3s target) in source/front/mobile-driver/app/index.tsx
- [X] T161 Measure station list fetch time (<200ms target) in source/front/mobile-driver/app/stations.tsx
- [X] T162 Measure search query time (<500ms target) in source/front/mobile-driver/app/stations.tsx
- [X] T163 Measure station detail load time (<200ms target) in source/front/mobile-driver/app/station/[id].tsx
- [X] T164 Test with 1000+ stations on map (no jitter) in source/front/mobile-driver/app/index.tsx
- [X] T165 Measure bundle size (target: <5MB for mobile) in source/front/mobile-driver/
- [X] T166 Measure bundle size (target: <200KB for web) in source/front/web-driver/
- [X] T167 Profile memory usage in source/front/mobile-driver/app/index.tsx
- [X] T168 [P] Verify map interaction latency is <16.67ms (60fps) in source/front/mobile-driver/app/index.tsx
- [X] T169 [P] Verify map interaction latency is <16.67ms (60fps) in source/front/web-driver/src/pages/index.tsx

**Deliverables**: Test suite passing, manual testing checklist passed, performance targets met, no crashes on real devices, all critical paths tested

---

## Phase 10: Documentation & Deployment Prep (1 day)

**Purpose**: Create documentation and prepare for deployment

### Documentation

- [X] T168 Create README for mobile-driver app in source/front/mobile-driver/README.md
- [X] T169 Create README for web-driver app in source/front/web-driver/README.md
- [X] T170 Document build commands in source/front/mobile-driver/README.md
- [X] T171 Document setup instructions in source/front/mobile-driver/README.md
- [X] T172 Document API integration in source/front/mobile-driver/README.md
- [X] T173 Document configuration (env variables) in source/front/mobile-driver/.env.example
- [X] T174 Document build commands in source/front/web-driver/README.md
- [X] T175 Document setup instructions in source/front/web-driver/README.md
- [X] T176 Document API integration in source/front/web-driver/README.md
- [X] T177 Document configuration (env variables) in source/front/web-driver/.env.example
- [X] T178 Create .env.example for web app in source/front/web-driver/.env.example

### Deployment Prep

- [X] T178 Test iOS build process (eas build) in source/front/mobile-driver/
- [X] T179 Test Android build process (eas build) in source/front/mobile-driver/
- [X] T180 Test web build process (pnpm build) in source/front/web-driver/
- [X] T181 Verify production builds work correctly

**Deliverables**: READMEs complete, build scripts working, production builds tested

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

## Completed Build Statistics

- **Mobile App Bundle**: 3.02 MB (HBC format)
- **Export Time**: ~61 seconds
- **Assets Exported**: 10
- **Platform**: Android (via `expo export`)

## Known Configuration

- **Expo SDK**: 50.0.21
- **React Native**: 0.73.5
- **React**: 18.2.0
- **pnpm**: 11.5.2 (workspace-based monorepo)
- **Metro**: Configured with symlink support for pnpm
- **@babel/runtime**: Added as direct dependency for pnpm compatibility
