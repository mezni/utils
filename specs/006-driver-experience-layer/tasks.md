# Tasks: Driver Experience Layer (UX + Product Polish)

**Input**: Design documents from `specs/006-driver-experience-layer/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

## Path Conventions

- **domain-types**: apps/packages/domain-types/src/
- **driver-service**: services/driver-service/src/
- **auth-service**: services/auth-service/src/
- **admin-service**: services/admin-service/src/
- **Frontend**: apps/mobile/ or apps/web/
- **ui-kit**: apps/packages/ui-kit/src/
- **CI gates**: .specify/ci-gates/

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create shared DTOs, CI gates, and ui-kit components that all downstream stories depend on.

- [ ] T001 Create domain-types DTOs for favorites in apps/packages/domain-types/src/favorites/ (AddFavoriteRequest, RemoveFavoriteRequest, FavoriteItem, FavoritesListResponse)
- [ ] T002 Create domain-types DTOs for search in apps/packages/domain-types/src/search/ (SearchResult, SearchResponse, SearchQuery)
- [ ] T003 Create domain-types DTOs for preferences in apps/packages/domain-types/src/preferences/ (Preferences, PreferencesResponse, UpdatePreferencesRequest)
- [ ] T004 Create domain-types DTOs for telemetry events in apps/packages/domain-types/src/telemetry/ (FAVORITE_ADDED, FAVORITE_REMOVED, SEARCH_EXECUTED, SEARCH_SELECTED, FILTER_CHANGED, OFFLINE_MODE_ENTERED)
- [ ] T005 [P] Create preferences isolation CI gate in .specify/ci-gates/023-preferences-isolation.sh
- [ ] T006 [P] Create offline storage CI gate in .specify/ci-gates/024-offline-storage.sh
- [ ] T007 [P] Create search safety CI gate in .specify/ci-gates/025-search-safety.sh
- [ ] T008 [P] Create UI boundary CI gate in .specify/ci-gates/026-ui-boundary.sh
- [ ] T009 [P] Create performance regression CI gate in .specify/ci-gates/027-performance-regression.sh

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Database setup, telemetry extension, and shared ui-kit components.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T010 Enable pg_trgm extension in driver-service migrations
- [ ] T011 Add pg_trgm GiST index on gis.stations(name, address) in driver-service migrations
- [ ] T012 [P] Extend telemetry event enum in driver-service with 6 new event types (driver-service/src/domain/event.rs)
- [ ] T013 [P] Create skeleton ui-kit components (SkeletonLoader, SkeletonCard) in apps/packages/ui-kit/src/components/

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Driver Favorites System

**Goal**: Drivers can save, view, and remove favorite stations. Favorites stored as dedicated section in users.preferences JSONB. driver-service owns the API.

**Independent Test**: POST a favorite → GET list returns it → DELETE it → GET list confirms removal. Test offline: favorite while offline → reconnect → verify persisted.

### Implementation for User Story 1

- [ ] T014 [P] [US1] Create favorites API contract in services/driver-service/src/api/favorites.rs (POST/GET/DELETE handlers)
- [ ] T015 [P] [US1] Create favorite button UI component (FavoriteButton) in apps/packages/ui-kit/src/components/
- [ ] T016 [P] [US1] Create favorites list UI component (FavoritesList) in apps/mobile/src/screens/
- [ ] T017 [US1] Wire favorites routes in driver-service/src/api/mod.rs
- [ ] T018 [US1] Implement optimistic UI for favorite toggle (heart fills within 150ms before server confirm)
- [ ] T019 [US1] Implement optimistic rollback on favorite API failure (revert heart state)
- [ ] T020 [US1] Implement favorites telemetry emission (FAVORITE_ADDED, FAVORITE_REMOVED)

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - User Preferences System

**Goal**: Drivers can customize preferred charger type, map filters, and default region. Stored in preferences section of users.preferences JSONB.

**Independent Test**: PUT preferences → GET returns them → PATCH one field → GET shows partial update. Verify map reflects saved filter on reload.

### Implementation for User Story 2

- [ ] T021 [P] [US2] Implement preferences GET handler in auth-service/src/api/preferences.rs
- [ ] T022 [P] [US2] Implement preferences PUT handler in auth-service/src/api/preferences.rs
- [ ] T023 [P] [US2] Implement preferences PATCH handler (partial update) in auth-service/src/api/preferences.rs
- [ ] T024 [US2] Wire preferences routes in auth-service/src/api/mod.rs
- [ ] T025 [P] [US2] Create preferences UI (SettingsScreen) in apps/mobile/src/screens/
- [ ] T026 [US2] Apply saved preferences to map on app startup (connector_type filter, region center)
- [ ] T027 [US2] Emit FILTER_CHANGED telemetry event on preference update

**Checkpoint**: At this point, User Story 2 should be fully functional and testable independently

---

## Phase 5: User Story 3 - Offline Cache Layer

**Goal**: App functions gracefully offline - cached stations, favorites, and preferences accessible without backend dependency.

**Independent Test**: Load app online → favorite a station → enable airplane mode → verify favorites accessible, cached map tiles display → reconnect → verify sync.

### Implementation for User Story 3

- [ ] T028 [P] [US3] Implement AsyncStorage/IndexedDB cache layer in apps/mobile/src/services/cache.ts
- [ ] T029 [P] [US3] Implement stale-while-revalidate pattern for cache reads
- [ ] T030 [US3] Implement offline sync queue with timestamp tracking in apps/mobile/src/services/sync.ts
- [ ] T031 [US3] Implement last-write-wins conflict resolution on reconnect
- [ ] T032 [US3] Implement cached favorites access when offline
- [ ] T033 [US3] Implement cached map tiles (previously viewed only) display when offline
- [ ] T034 [US3] Implement offline search against local cache
- [ ] T035 [US3] Implement offline-first app launch with friendly retry message
- [ ] T036 [US3] Emit OFFLINE_MODE_ENTERED telemetry event on connectivity loss

**Checkpoint**: At this point, User Story 3 should be fully functional and testable independently

---

## Phase 6: User Story 4 - Map UX Upgrade

**Goal**: Map is fast, smooth, and informative - clustering, preview cards, color-coded markers.

**Independent Test**: Open map at country zoom → verify clusters → zoom in → clusters break apart → tap station → preview card appears → pan → markers load progressively.

### Implementation for User Story 4

- [ ] T037 [P] [US4] Implement station marker clustering in apps/mobile/src/components/map/
- [ ] T038 [P] [US4] Implement station preview card (name, connectors, availability, distance) in apps/packages/ui-kit/src/components/
- [ ] T039 [P] [US4] Implement custom markers by connector type (color-coded) and availability (green/orange/red)
- [ ] T040 [US4] Implement progressive marker loading on pan/zoom (only render viewport markers)

**Checkpoint**: At this point, User Story 4 should be fully functional and testable independently

---

## Phase 7: User Story 5 - Station Search with Fuzzy Matching

**Goal**: Drivers search stations by name/address with fuzzy matching. Online via driver-service → Postgres trigram. Offline via local cache.

**Independent Test**: Search "fast charg" → returns "Fast Charging Hub". Search typo → returns relevant results. Go offline → search against cache.

### Implementation for User Story 5

- [ ] T041 [P] [US5] Implement trigram search query in driver-service/src/db/queries.rs (pg_trgm similarity)
- [ ] T042 [P] [US5] Implement search GET handler in driver-service/src/api/search.rs
- [ ] T043 [US5] Wire search route in driver-service/src/api/mod.rs
- [ ] T044 [P] [US5] Create search UI (SearchBar, SearchResults) in apps/mobile/src/components/search/
- [ ] T045 [US5] Implement offline search against cached station data
- [ ] T046 [US5] Emit SEARCH_EXECUTED and SEARCH_SELECTED telemetry events

**Checkpoint**: At this point, User Story 5 should be fully functional and testable independently

---

## Phase 8: User Story 6 - Skeleton Loaders & Optimistic UI

**Goal**: Every screen transition shows skeleton placeholders within 150ms. Optimistic UI updates apply immediately.

**Independent Test**: Navigate to any data screen → skeleton within 150ms → content replaces skeleton. Tap favorite → heart fills within 150ms before server response.

### Implementation for User Story 6

- [ ] T047 [P] [US6] Integrate SkeletonLoader on station list screen apps/mobile/src/screens/
- [ ] T048 [P] [US6] Integrate SkeletonLoader on favorites list screen
- [ ] T049 [P] [US6] Integrate SkeletonLoader on search results screen
- [ ] T050 [US6] Implement optimistic UI for favorite toggle (already in US1, verify integration)
- [ ] T051 [US6] Implement optimistic rollback on all server failures
- [ ] T052 [US6] Implement background data refresh with stale-while-revalidate

**Checkpoint**: At this point, User Story 6 should be fully functional and testable independently

---

## Phase 9: User Story 7 - Session Continuity

**Goal**: App remembers last map position, filters, and section. Authentication remains Keycloak-managed.

**Independent Test**: Set CCS filter → navigate to Favorites → close app → reopen → map centers same position, CCS filter active, Favorites section shown.

### Implementation for User Story 7

- [ ] T053 [P] [US7] Implement UI session state capture (map position, filters, last section) in apps/mobile/src/services/session.ts
- [ ] T054 [US7] Implement session state persistence to AsyncStorage/IndexedDB
- [ ] T055 [US7] Implement session state restoration on app launch
- [ ] T056 [US7] Implement 30-minute session expiry for UI state (not auth)

**Checkpoint**: At this point, User Story 7 should be fully functional and testable independently

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Integration tests, performance benchmarks, CI gate verification, telemetry verification.

- [ ] T057 [P] Write favorites API integration tests for driver-service/tests/
- [ ] T058 [P] Write search API integration tests for driver-service/tests/
- [ ] T059 [P] Write preferences API integration tests for auth-service/tests/
- [ ] T060 [P] Write offline cache unit tests for apps/mobile/src/services/cache.test.ts
- [ ] T061 [P] Implement search response time benchmark (P95 < 1s) in .specify/benchmarks/
- [ ] T062 [P] Implement map rendering latency benchmark (60fps) in .specify/benchmarks/
- [ ] T063 [P] Write telemetry event verification test (6 new event types in analytics_db)
- [ ] T064 [P] Verify all 5 CI gates pass (023-027)
- [ ] T065 Update AGENTS.md with Sprint 5 context and CI gate references

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-9)**: All depend on Foundational phase completion
- **Polish (Phase 10)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (Favorites)**: Can start after Foundational - No dependencies on other stories
- **User Story 2 (Preferences)**: Can start after Foundational - No dependencies on other stories
- **User Story 3 (Offline)**: Can start after Foundational - No dependencies on other stories
- **User Story 4 (Map UX)**: Can start after Foundational - No dependencies on other stories
- **User Story 5 (Search)**: Can start after Foundational - No dependencies on other stories
- **User Story 6 (Skeletons)**: Can start after Foundational - Depends partially on US4/5 for screen integration
- **User Story 7 (Session)**: Can start after Foundational - No dependencies on other stories

**All user stories are independently testable and can be implemented in parallel after Foundational phase completes.**

### Within Each User Story

- Parallelizable tasks (marked [P]) can be executed simultaneously
- Non-parallelizable tasks depend on previous tasks within the story
- Stories are fully independent of each other

### Parallel Opportunities

- All Setup tasks (Phase 1) marked [P] can run in parallel
- All Foundational tasks (Phase 2) marked [P] can run in parallel
- Within each user story: All [P] tasks can run in parallel
- Once Foundational phase completes, ALL 7 user stories can start in parallel
- Polish tasks (Phase 10) all marked [P] can run in parallel

## Task Summary

**Total Tasks**: 65

**Task Count per User Story**:
- Setup: 9 tasks
- Foundational: 4 tasks
- User Story 1 (Favorites): 7 tasks
- User Story 2 (Preferences): 7 tasks
- User Story 3 (Offline): 9 tasks
- User Story 4 (Map UX): 4 tasks
- User Story 5 (Search): 6 tasks
- User Story 6 (Skeletons): 6 tasks
- User Story 7 (Session): 4 tasks
- Polish: 9 tasks

**Parallel Opportunities**:
- Phase 1: 5 parallelizable tasks
- Phase 2: 2 parallelizable tasks
- Phase 3: 3 parallelizable tasks
- Phase 4: 3 parallelizable tasks
- Phase 5: 2 parallelizable tasks
- Phase 6: 3 parallelizable tasks
- Phase 7: 2 parallelizable tasks
- Phase 8: 3 parallelizable tasks
- Phase 9: 1 parallelizable task
- Phase 10: 7 parallelizable tasks
- **Total Parallelizable Tasks**: 31 out of 65 (48%)

**Independent Test Criteria**:
- US1: POST/GET/DELETE favorites API works, optimistic UI visible <150ms
- US2: PUT/GET/PATCH preferences endpoint works, map reflects settings
- US3: App works fully offline with cached data, syncs on reconnect
- US4: Clusters form at zoom-out, preview cards appear on tap, 60fps
- US5: Fuzzy search returns relevant results P95 < 1s, offline search works
- US6: Skeleton visible within 150ms on all data screens
- US7: Session state restored correctly up to 30 minutes, auth unaffected
