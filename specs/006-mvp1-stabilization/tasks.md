# Tasks: MVP-1 Stabilization Sprint

**Input**: Design documents from `specs/006-mvp1-stabilization/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: Tests are NOT included (stabilization sprint focuses on optimization and polish, not new features)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Backend**: `source/services/driver-service/src/`, `source/services/admin-service/src/`, `source/services/shared/`
- **Frontend**: `source/front/mobile-driver/`, `source/front/web-driver/src/`
- **Tests**: `source/services/*/tests/`, `source/front/mobile-driver/__tests__/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Performance optimization and testing infrastructure setup

- [X] T001 Profile existing backend performance with React DevTools and Chrome DevTools
T- [X] T002 [P] Configure performance profiling scripts for iOS (Xcode Instruments)
T- [X] T003 [P] Configure performance profiling scripts for Android (Android Profiler)
T- [X] T004 [P] Create performance benchmarking utility for API endpoints
T- [X] T005 [P] Setup React Native bundle size analysis script
T- [X] T006 [P] Configure automated accessibility testing (WCAG AA)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core performance optimizations that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Backend Query Optimization

- [X] T007 [P] Implement ETag support for stations list endpoint in source/services/driver-service/src/routes/stations.rs
- [X] T008 [P] Strip null fields from stations response JSON in source/services/driver-service/src/routes/stations.rs
- [X] T009 Optimize nearby stations query with LIMIT/OFFSET pagination in source/services/driver-service/src/routes/stations.rs
- [ ] T010 [P] Add request timeout configuration (30s) to driver-service in source/services/driver-service/src/main.rs
- [ ] T011 [P] Optimize station detail query to use Eager Loading in source/services/shared/ev-db/src/queries/stations.rs
- [ ] T012 Optimize PostGIS ST_DWithin query with GIST index validation in source/services/shared/ev-db/src/queries/stations.rs
- [ ] T013 [P] Add response time header (X-Response-Time) to driver-service endpoints in source/services/driver-service/src/middleware/mod.rs
- [ ] T014 [P] Optimize event batching query with upsert logic in source/services/admin-service/src/routes/events.rs
- [ ] T015 Optimize analytics database batch insert with transaction in source/services/admin-service/src/routes/events.rs

### Frontend Performance Optimization

- [ ] T016 [P] Implement React.memo for station marker components in source/front/mobile-driver/app/index.tsx
- [ ] T017 [P] Add useMemo for marker data transformation in source/front/mobile-driver/app/index.tsx
- [ ] T018 [P] Optimize React Query caching strategy with staleTime in source/front/mobile-driver/src/services/queryClient.ts
- [ ] T019 [P] Implement React.memo for station list items in source/front/mobile-driver/app/stations.tsx
- [ ] T020 [P] Add useMemo for filtered station list in source/front/mobile-driver/app/stations.tsx
- [ ] T021 [P] Implement lazy loading for station detail screen in source/front/mobile-driver/app/station/[id].tsx
- [ ] T022 [P] Optimize React Query queries with queryKey optimization in source/front/mobile-driver/src/services/queryClient.ts
- [ ] T023 [P] Create code splitting configuration with expo-router in source/front/mobile-driver/app/_layout.tsx
- [ ] T024 [P] Implement React.memo for web station components in source/front/web-driver/src/pages/stations.tsx
- [ ] T025 [P] Add useMemo for filtered stations in source/front/web-driver/src/pages/stations.tsx
- [ ] T026 [P] Implement lazy loading for web station detail in source/front/web-driver/src/pages/station/[id].tsx
- [ ] T027 [P] Implement React.memo for station detail in source/front/web-driver/src/pages/station/[id].tsx

### Test Infrastructure

- [ ] T028 [P] Create automated performance regression tests in source/front/mobile-driver/src/__tests__/performance.test.ts
- [ ] T029 [P] Create automated performance regression tests in source/front/web-driver/src/__tests__/performance.test.ts
- [ ] T030 [P] Create API performance benchmarking script for React Native
- [ ] T031 [P] Create API performance benchmarking script for Web

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Fast Interaction Response (Priority: P1) 🎯 MVP

**Goal**: Reduce all critical user interactions to under 300ms (p95) including screen transitions, map interactions, and data loading

**Independent Test**: Measure response times for all critical user actions (screen transitions, map interactions, data loading) using automated performance profiling tools and verify all metrics meet the <300ms requirement

### Backend Performance Optimization

- [ ] T032 [P] [US1] Optimize station list query with column selection in source/services/driver-service/src/routes/stations.rs
- [ ] T033 [P] [US1] Optimize nearby stations query with column projection in source/services/driver-service/src/routes/stations.rs
- [ ] T034 [P] [US1] Add response time tracking middleware in source/services/driver-service/src/middleware/mod.rs
- [ ] T035 [P] [US1] Optimize JSON serialization with serde_json to_skip_serializing_if in source/services/shared/ev-core/src/lib.rs
- [ ] T036 [P] [US1] Implement caching headers for stations list in source/services/driver-service/src/routes/stations.rs
- [ ] T037 [P] [US1] Optimize database pool connection handling in source/services/shared/ev-db/src/pool.rs
- [ ] T038 [P] [US1] Optimize event batch insertion query in source/services/admin-service/src/routes/events.rs

### Frontend Performance Optimization

T- [X] T039 [P] [US1] Implement React.memo for map marker component in source/front/mobile-driver/app/index.tsx
T- [X] T040 [P] [US1] Optimize map rendering with useAnimationFrame hook in source/front/mobile-driver/app/index.tsx
T- [X] T041 [P] [US1] Implement pagination in stations list with lazy loading in source/front/mobile-driver/app/stations.tsx
T- [X] T042 [P] [US1] Optimize React Query client with query retry and refetch settings in source/front/mobile-driver/src/services/queryClient.ts
T- [X] T043 [P] [US1] Optimize data transformation with useMemo in source/front/mobile-driver/src/services/queryClient.ts
T- [X] T044 [P] [US1] Implement debouncing for search input in source/front/mobile-driver/app/stations.tsx
T- [X] T045 [P] [US1] Optimize map marker clustering algorithm in source/front/mobile-driver/src/utils/mapCluster.ts
T- [X] T046 [P] [US1] Implement React.memo for web station list in source/front/web-driver/src/pages/stations.tsx
- [ ] T047 [P] [US1] Optimize React Query for web app in source/front/web-driver/src/services/queryClient.ts
- [ ] T048 [P] [US1] Implement pagination with lazy loading for web app in source/front/web-driver/src/pages/stations.tsx
- [ ] T049 [P] [US1] Optimize data fetching with useMemo in source/front/web-driver/src/pages/stations.tsx

### Performance Testing

- [ ] T050 [P] [US1] Create automated performance tests for stations list endpoint in source/front/mobile-driver/src/__tests__/api.performance.test.ts
- [ ] T051 [P] [US1] Create automated performance tests for nearby stations endpoint in source/front/mobile-driver/src/__tests__/api.performance.test.ts
T- [X] T052 [P] [US1] Create performance regression tests for mobile app in source/front/mobile-driver/src/__tests__/performance.test.ts
T- [X] T053 [P] [US1] Create performance regression tests for web app in source/front/web-driver/src/__tests__/performance.test.ts
T- [X] T054 [P] [US1] Create screen transition performance tests in source/front/mobile-driver/src/__tests__/transition.performance.test.ts

**Checkpoint**: At this point, User Story 1 should be fully functional with all interactions under 300ms (p95)

---

## Phase 4: User Story 2 - Stable Map Rendering (Priority: P1)

**Goal**: Render 1000+ charging station markers without jitter or flickering during panning, zooming, or marker clustering

**Independent Test**: Load a dataset with 1000+ stations and measure rendering stability using performance profiling tools, verifying no frame drops, jitters, or marker flashing occur during panning, zooming, or marker clustering

### Map Rendering Optimization

T- [X] T055 [P] [US2] Implement marker clustering library with 50m radius in source/front/mobile-driver/src/utils/mapCluster.ts
- [X] T056 [P] [US2] Optimize marker rendering with requestAnimationFrame in source/front/mobile-driver/app/index.tsx
- [X] T057 [P] [US2] Implement React.memo for cluster markers in source/front/mobile-driver/app/index.tsx
T- [X] T058 [P] [US2] Optimize map state management with useMapState hook in source/front/mobile-driver/src/hooks/useMapState.ts
T- [X] T059 [P] [US2] Implement virtualized marker list for large datasets in source/front/mobile-driver/src/utils/virtualizeMarkers.ts
T- [X] T060 [P] [US2] Optimize map transitions with animated values in source/front/mobile-driver/app/index.tsx
T- [X] T061 [P] [US2] Implement marker animation with reanimated v3 in source/front/mobile-driver/src/components/AnimatedMarker.tsx

### Mobile Performance Testing

T- [X] T062 [P] [US2] Create map performance benchmark tests in source/front/mobile-driver/src/__tests__/map.performance.test.ts
T- [X] T063 [P] [US2] Profile marker clustering performance (1000+ markers) in source/front/mobile-driver/src/__tests__/map.performance.test.ts
- [X] T064 [P] [US2] Test map panning performance (60fps) in source/front/mobile-driver/src/__tests__/map.performance.test.ts
- [X] T065 [P] [US2] Test map zoom performance in source/front/mobile-driver/src/__tests__/map.performance.test.ts
- [X] T066 [P] [US2] Test marker clustering performance (1000+ markers) in source/front/mobile-driver/src/__tests__/map.performance.test.ts

**Checkpoint**: Map with 1000+ markers renders smoothly at 60fps without jitter or flickering

---

## Phase 5: User Story 3 - Consistent Error Recovery (Priority: P2)

**Goal**: Provide clear error messages with recovery options for network or system issues so users can continue using the app

**Independent Test**: Simulate various failure scenarios (network errors, database failures, invalid inputs) and verify all error paths show user-friendly messages with actionable recovery buttons

### Backend Error Handling

T- [X] T067 [P] [US3] Implement error response middleware with user-friendly messages in source/services/driver-service/src/middleware/error.rs
T- [X] T068 [P] [US3] Implement exponential backoff retry logic for transient failures in source/services/shared/ev-db/src/error.rs
T- [X] T069 [P] [US3] Add error logging with request ID correlation in source/services/driver-service/src/middleware/error.rs
T- [X] T070 [P] [US3] Implement error response validation in source/services/driver-service/src/routes/stations.rs
T- [X] T071 [P] [US3] Add error context to analytics database events in source/services/admin-service/src/routes/events.rs
T- [X] T072 [P] [US3] Implement batch event error handling in source/services/admin-service/src/routes/events.rs

### Frontend Error Recovery UI

T- [X] T073 [P] [US3] Create reusable ErrorState component in source/front/mobile-driver/src/components/error/ErrorState.tsx
T- [X] T074 [P] [US3] Implement network error handling with retry in source/front/mobile-driver/app/stations.tsx
- [X] T075 [P] [US3] Implement server error handling with retry in source/front/mobile-driver/app/station/[id].tsx
- [X] T076 [P] [US3] Implement error recovery in map location services in source/front/mobile-driver/src/services/geolocation.ts
- [X] T077 [P] [US3] Create web ErrorState component in source/front/web-driver/src/components/error/ErrorState.tsx
- [X] T078 [P] [US3] Implement error recovery for web app in source/front/web-driver/src/pages/stations.tsx
- [X] T079 [P] [US3] Implement error recovery for web app in source/front/web-driver/src/pages/station/[id].tsx
- [X] T080 [P] [US3] Implement error boundaries for React Native in source/front/mobile-driver/src/components/ErrorBoundary.tsx
- [X] T081 [P] [US3] Implement error boundaries for React in source/front/web-driver/src/ErrorBoundary.tsx

### Error Recovery Testing

- [X] T082 [P] [US3] Create network error handling tests in source/front/mobile-driver/src/__tests__/error-handling.test.tsx
- [X] T083 [P] [US3] Create server error handling tests in source/front/mobile-driver/src/__tests__/error-handling.test.tsx
- [X] T084 [P] [US3] Create error recovery UX tests in source/front/mobile-driver/src/__tests__/error-recovery.test.tsx
- [X] T085 [P] [US3] Test error recovery on mobile app in source/front/mobile-driver/src/__tests__/error-recovery.test.tsx

**Checkpoint**: All error paths show user-friendly messages with actionable recovery buttons

---

## Phase 6: User Story 4 - Perfect Dark Mode (Priority: P2)

**Goal**: Perfect dark mode implementation across all screens with WCAG AA contrast compliance

**Independent Test**: Test every screen in both light and dark modes and verify color contrast meets accessibility standards, text is readable, and no elements appear stretched, inverted, or broken

### Dark Mode Implementation

- [X] T086 [P] [US4] Implement dark mode theme provider in source/front/mobile-driver/src/providers/ThemeProvider.tsx
- [X] T087 [P] [US4] Add dark mode toggle component in source/front/mobile-driver/src/components/settings/DarkModeToggle.tsx
- [X] T088 [P] [US4] Implement dark mode persistence with AsyncStorage in source/front/mobile-driver/src/store/useThemeStore.ts
- [X] T089 [P] [US4] Update all screens to use theme provider in source/front/mobile-driver/app/index.tsx
- [X] T090 [P] [US4] Update all screens to use theme provider in source/front/mobile-driver/app/stations.tsx
- [X] T091 [P] [US4] Update all screens to use theme provider in source/front/mobile-driver/app/station/[id].tsx
- [X] T092 [P] [US4] Update all screens to use theme provider in source/front/web-driver/src/pages/stations.tsx
- [X] T093 [P] [US4] Update all screens to use theme provider in source/front/web-driver/src/pages/station/[id].tsx
- [X] T094 [P] [US4] Implement dark mode transitions with reanimated v3 in source/front/mobile-driver/src/components/AnimatedTheme.tsx
- [X] T095 [P] [US4] Create web dark mode implementation in source/front/web-driver/src/components/ThemeProvider.tsx

### Accessibility Testing

- [X] T096 [P] [US4] Create automated WCAG AA accessibility tests in source/front/mobile-driver/src/__tests__/accessibility.test.ts
- [X] T097 [P] [US4] Create manual WCAG AA checklist for all screens in source/front/mobile-driver/src/__tests__/accessibility.manual.test.ts
- [X] T098 [P] [US4] Test contrast ratios on all screens in source/front/mobile-driver/src/__tests__/accessibility.manual.test.ts
- [X] T099 [P] [US4] Test screen reader compatibility in source/front/mobile-driver/src/__tests__/accessibility.manual.test.ts
- [X] T100 [P] [US4] Test keyboard navigation on all screens in source/front/mobile-driver/src/__tests__/accessibility.manual.test.ts

**Checkpoint**: Dark mode works perfectly on all screens with WCAG AA contrast compliance

---

## Phase 7: User Story 5 - Smooth Loading States (Priority: P3)

**Goal**: Display animated skeleton screens instead of spinning loaders on all screens

**Independent Test**: Monitor all screens during data loading and verify skeleton placeholders animate smoothly, never flicker, and are consistent with the actual content layout

### Skeleton Screen Implementation

- [X] T101 [P] [US5] Create SkeletonBox component in source/front/mobile-driver/src/components/skeleton/SkeletonBox.tsx
- [X] T102 [P] [US5] Create SkeletonGroup component in source/front/mobile-driver/src/components/skeleton/SkeletonGroup.tsx
- [X] T103 [P] [US5] Create StationListItemSkeleton in source/front/mobile-driver/src/components/skeleton/StationListItemSkeleton.tsx
- [X] T104 [P] [US5] Create StationDetailSkeleton in source/front/mobile-driver/src/components/skeleton/StationDetailSkeleton.tsx
- [X] T105 [P] [US5] Create SearchBarSkeleton in source/front/mobile-driver/src/components/skeleton/SearchBarSkeleton.tsx
- [X] T106 [P] [US5] Create EmptyState component in source/front/mobile-driver/src/components/EmptyState.tsx
- [X] T107 [P] [US5] Create ErrorState component (for loading states) in source/front/mobile-driver/src/components/error/LoadingErrorState.tsx
- [X] T108 [P] [US5] Implement skeleton screen transitions with reanimated v3 in source/front/mobile-driver/src/components/skeleton/AnimatedSkeleton.tsx
- [X] T109 [P] [US5] Create web skeleton components in source/front/web-driver/src/components/skeleton/
- [X] T110 [P] [US5] Create web empty state components in source/front/web-driver/src/components/EmptyState.tsx

### Skeleton Screen Testing

- [X] T111 [P] [US5] Create skeleton screen animation tests in source/front/mobile-driver/src/__tests__/skeleton.animation.test.tsx
- [X] T112 [P] [US5] Test skeleton flickering prevention in source/front/mobile-driver/src/__tests__/skeleton.animation.test.tsx
- [X] T113 [P] [US5] Test skeleton layout consistency in source/front/mobile-driver/src/__tests__/skeleton.animation.test.tsx
- [X] T114 [P] [US5] Test skeleton screen transitions in source/front/mobile-driver/src/__tests__/skeleton.animation.test.tsx
- [X] T115 [P] [US5] Test web skeleton screens in source/front/web-driver/src/__tests__/skeleton.animation.test.ts

**Checkpoint**: All screens show animated skeleton screens instead of spinners

---

## Phase 8: User Story 6 - Reliable Event Tracking (Priority: P3)

**Goal**: Log all user interactions accurately to the analytics database with correct timestamps and event types

**Independent Test**: Perform all key user actions and verify events are logged with correct timestamps, user IDs, and event types in the analytics database

### Event Tracking Implementation

- [X] T116 [P] [US6] Implement event logging utility in source/front/mobile-driver/src/services/eventTracking.ts
- [X] T117 [P] [US6] Implement event batching queue in source/front/mobile-driver/src/services/eventTracking.ts
- [X] T118 [P] [US6] Implement batch event sending to backend in source/front/mobile-driver/src/services/eventTracking.ts
- [X] T119 [P] [US6] Implement retry logic for failed event batches in source/front/mobile-driver/src/services/eventTracking.ts
- [X] T120 [P] [US6] Implement event tracking for app launch in source/front/mobile-driver/src/services/eventTracking.ts
- [X] T121 [P] [US6] Implement event tracking for station view in source/front/mobile-driver/src/services/eventTracking.ts
- [X] T122 [P] [US6] Implement event tracking for search actions in source/front/mobile-driver/src/services/eventTracking.ts
- [X] T123 [P] [US6] Implement event tracking for map interactions in source/front/mobile-driver/src/services/eventTracking.ts
- [X] T124 [P] [US6] Implement web event tracking in source/front/web-driver/src/services/eventTracking.ts
- [X] T125 [P] [US6] Add event tracking for error scenarios in source/front/mobile-driver/src/services/eventTracking.ts
- [X] T126 [P] [US6] Add event tracking metadata (device info, timestamp) in source/front/mobile-driver/src/services/eventTracking.ts

### Event Tracking Testing

- [X] T127 [P] [US6] Create event tracking unit tests in source/front/mobile-driver/src/__tests__/eventTracking.test.ts
- [X] T128 [P] [US6] Create batch event tracking tests in source/front/mobile-driver/src/__tests__/eventTracking.test.ts
- [X] T129 [P] [US6] Create event tracking integration tests in source/front/mobile-driver/src/__tests__/eventTracking.integration.test.ts
- [X] T130 [P] [US6] Test event tracking reliability in source/front/mobile-driver/src/__tests__/eventTracking.integration.test.ts
- [X] T131 [P] [US6] Verify analytics database integrity in source/front/mobile-driver/src/__tests__/eventTracking.integration.test.ts

**Checkpoint**: 100% of user interactions are logged to analytics database with correct data

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [X] T132 [P] Verify zero console errors across all platforms
- [X] T133 [P] Run accessibility audit on all screens
- [X] T134 [P] Test battery drain on iPhone 14 Pro (target: <5% per hour)
- [X] T135 [P] Test battery drain on Android 10+ (target: <5% per hour)
- [X] T136 [P] Test on iPhone 12 Pro (target: <8% per hour, stable performance)
- [X] T137 [P] Optimize React Native bundle size (target: <100MB)
- [X] T138 [P] Run performance profiling on all critical paths
- [X] T139 [P] Optimize PostGIS queries based on profiling results
- [X] T140 [P] Verify all screen transitions are under 200ms
- [X] T141 [P] Test all haptic feedback on CTAs
- [X] T142 [P] Verify all empty states are fully designed
- [X] T143 [P] Test all error states have recovery actions
- [X] T144 [P] Verify all dark mode transitions are smooth
- [X] T145 [P] Test web app for memory leaks
- [X] T146 [P] Create performance report with benchmark results
- [X] T147 [P] Update documentation with optimization findings

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-8)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Phase 9)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational (Phase 2) - May integrate with US1 but should be independently testable
- **User Story 3 (P2)**: Can start after Foundational (Phase 2) - May integrate with US1/US2 but should be independently testable
- **User Story 4 (P2)**: Can start after Foundational (Phase 2) - Independent of other stories
- **User Story 5 (P3)**: Can start after Foundational (Phase 2) - Independent of other stories
- **User Story 6 (P3)**: Can start after Foundational (Phase 2) - Independent of other stories

### Within Each User Story

- Backend tasks before frontend tasks
- Core implementation before testing
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- All tests marked [P] can run in parallel
- Models within a story marked [P] can run in parallel
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Launch all backend optimization tasks for User Story 1 together:
Task: "T032 Optimize station list query with column selection"
Task: "T033 Optimize nearby stations query with column projection"
Task: "T034 Add response time tracking middleware"
Task: "T035 Optimize JSON serialization"
Task: "T036 Implement caching headers"

# Launch all frontend optimization tasks for User Story 1 together:
Task: "T039 Implement React.memo for map marker component"
Task: "T040 Optimize map rendering with requestAnimationFrame"
Task: "T041 Implement pagination in stations list"
Task: "T042 Optimize React Query client"
Task: "T043 Optimize data transformation with useMemo"

# Launch all performance tests for User Story 1 together:
Task: "T050 Create automated performance tests for stations list endpoint"
Task: "T051 Create automated performance tests for nearby stations endpoint"
Task: "T052 Create performance regression tests for mobile app"
Task: "T053 Create performance regression tests for web app"
Task: "T054 Create screen transition performance tests"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test User Story 1 independently (all interactions <300ms)
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 → Test independently → Deploy/Demo
4. Add User Story 3 → Test independently → Deploy/Demo
5. Add User Story 4 → Test independently → Deploy/Demo
6. Add User Story 5 → Test independently → Deploy/Demo
7. Add User Story 6 → Test independently → Deploy/Demo
8. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (Fast Interaction Response)
   - Developer B: User Story 2 (Stable Map Rendering)
   - Developer C: User Story 3 (Consistent Error Recovery)
   - Developer D: User Story 4 (Perfect Dark Mode)
   - Developer E: User Story 5 (Smooth Loading States)
   - Developer F: User Story 6 (Reliable Event Tracking)
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Tests are NOT included (stabilization sprint focuses on optimization, not testing new features)
- Performance testing is included in each user story phase
- Accessibility testing is included in User Story 4 phase
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Use performance profiling tools (Xcode Instruments, Android Profiler) to measure improvements

---

## Success Criteria Validation

- **SC-001** (<300ms response): ✅ Covered by User Story 1 tasks T032-T054
- **SC-002** (1000+ markers no jitter): ✅ Covered by User Story 2 tasks T055-T066
- **SC-003** (zero console errors): ✅ Covered by Phase 9 task T132
- **SC-004** (<5% battery): ✅ Covered by Phase 9 tasks T134-T136
- **SC-005** (WCAG AA dark mode): ✅ Covered by User Story 4 tasks T086-T100
- **SC-006** (<100MB app size): ✅ Covered by Phase 9 task T137
- **SC-007** (100% event tracking): ✅ Covered by User Story 6 tasks T116-T131
- **SC-008** (iOS 12/13/14+ and Android 10+): ✅ Covered by Phase 9 tasks T134-T136

**All success criteria have corresponding tasks!**
