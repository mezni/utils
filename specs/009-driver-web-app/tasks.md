# Tasks: Driver Web App

**Input**: Feature specification and plan from `specs/009-driver-web-app/`

**Prerequisites**: plan.md, spec.md

**Organization**: Tasks grouped by phase for independent implementation.

**Format**: `[ID] [P] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story (e.g., US1, US2)
- Include exact file paths in descriptions

**Path Conventions**:
- **App code**: `apps/driver-web/src/`
- **Packages**: `packages/{api-client,auth-client,event-taxonomy,design-tokens}/src/`

---

## Phase 0: Setup & App Shell

**Purpose**: Install missing dependencies, wire up routing, state management providers, and app layout. Unblocks all user stories.

- [ ] T001 [P] Add dependencies to `apps/driver-web/package.json`: `@tanstack/react-query`, `react-router`, `@bornemap/api-client`, `@bornemap/auth-client`, `@bornemap/event-taxonomy`, `leaflet.markercluster` (types as dev)
- [ ] T002 Create app shell in `apps/driver-web/src/main.tsx` — wrap `<App />` with `<QueryClientProvider>`, `<BrowserRouter>`
- [ ] T003 Replace existing placeholder `apps/driver-web/src/App.tsx` with app layout: `<Header />` + `<MapView />` in a flex container
- [ ] T004 Replace `apps/driver-web/src/App.css` with layout styles (full-height map, no scroll, LTR/RTL logical properties)

---

## Phase 1: Map Foundation (User Story 1) — P1

**Goal**: Interactive map with clustered markers, viewport-driven data fetching, three map states (idle, active, station selected).

- [ ] T005 [P] [US1] Install `leaflet.markercluster` and update `apps/driver-web/src/components/ui/map-container.tsx` to support clustered markers
- [ ] T006 [P] [US1] Create `apps/driver-web/src/hooks/useStationMarkers.ts` — React Query hook that fetches stations by bounding box via api-client
- [ ] T007 [P] [US1] Create `apps/driver-web/src/hooks/useViewport.ts` — viewport change handler with 500ms debounce + AbortController for in-flight request cancellation
- [ ] T008 [P] [US1] Build `apps/driver-web/src/components/MapStateOverlay.tsx` — idle skeleton, active spinner, empty-state message
- [ ] T009 [US1] Wire map states in `apps/driver-web/src/components/MapView.tsx`: idle → viewport active → station selected; compose MapContainer + StationMarkers + MapStateOverlay
- [ ] T010 [US1] Verify acceptance: markers render and cluster on zoom, viewport refetch fires after pan/zoom stop, empty-state displays when no stations

---

## Phase 2: Station Detail & Search (User Story 2) — P1

**Goal**: Detail panel on marker click; search overlay with text input and filters.

- [ ] T011 [P] [US2] Build `apps/driver-web/src/components/StationDetailPanel.tsx` — side panel (260–400px) that slides in from the left, resizing the map
- [ ] T012 [P] [US2] Build `apps/driver-web/src/components/StationInfo.tsx` — name, description, address, distance from user
- [ ] T013 [P] [US2] Build `apps/driver-web/src/components/ChargerList.tsx` — connector type chips (CCS/Type2/CHAdeMO) with power rating and availability status
- [ ] T014 [P] [US2] Build `apps/driver-web/src/components/SearchOverlay.tsx` — collapsible overlay triggered by header icon; contains search input + connector type + availability filters
- [ ] T015 [P] [US2] Build `apps/driver-web/src/components/SearchResults.tsx` — scrollable list of matching stations
- [ ] T016 [P] [US2] Create hooks: `apps/driver-web/src/hooks/useStationDetail.ts`, `apps/driver-web/src/hooks/useSearch.ts` — React Query hooks for single station fetch and debounced search
- [ ] T017 [US2] Wire search with 300ms debounce in `apps/driver-web/src/hooks/useSearch.ts`; cancel in-flight requests on new query
- [ ] T018 [US2] Verify acceptance: detail panel opens on marker click with all fields, search returns filtered results within 500ms, panel closes on outside click

---

## Phase 3: Progressive Authentication (User Story 3) — P2

**Goal**: Anonymous browsing for read-only actions; login modal appears only for gated actions (favorite, review).

- [ ] T019 [P] [US3] Implement auth-client in `packages/auth-client/src/index.ts` — `getToken()`, `login()`, `logout()` with Keycloak integration (if available) or local mock
- [ ] T020 [P] [US3] Build `apps/driver-web/src/components/AuthModal.tsx` — reuses Modal primitive from Sprint 8; presents Keycloak login or mock auth form; shows error state on failure
- [ ] T021 [P] [US3] Create `apps/driver-web/src/hooks/useAuth.ts` — auth state management (isAuthenticated, user info, login/logout actions)
- [ ] T022 [US3] Integrate auth with api-client in `apps/driver-web/src/lib/api.ts` — create ApiClient instance with `getToken` callback for JWT injection
- [ ] T023 [US3] Gate favorite/review actions in UI: show AuthModal when anonymous user attempts gated action; after success, complete the original action
- [ ] T024 [US3] Handle expired JWT: on 401 from api-client, show re-auth modal; after re-authentication, retry the failed request automatically
- [ ] T025 [US3] Verify acceptance: anonymous browsing works end-to-end; auth modal appears on gated action; action completes after login without repeating trigger

---

## Phase 4: Favorites & Reviews (User Story 4) — P2

**Goal**: Registered drivers can favorite stations and submit/edit/delete reviews. Favorites accessible via inline map filter.

- [ ] T026 [P] [US4] Build `apps/driver-web/src/components/FavoriteButton.tsx` — heart toggle icon; filled if favorited, outline if not; shows auth modal if anonymous
- [ ] T027 [P] [US4] Create hooks: `apps/driver-web/src/hooks/useFavorites.ts` — `useFavorites(userId)`, `useFavoriteToggle()` with optimistic updates
- [ ] T028 [US4] Implement inline favorites filter in `apps/driver-web/src/components/MapView.tsx` — toggle to show only favorited stations on map
- [ ] T029 [P] [US4] Build `apps/driver-web/src/components/ReviewForm.tsx` — rating (1–5 stars) + comment textarea; validates one review per user
- [ ] T030 [P] [US4] Build `apps/driver-web/src/components/ReviewList.tsx` — list of existing reviews with edit/delete controls for the owner
- [ ] T031 [P] [US4] Create hooks: `apps/driver-web/src/hooks/useReviews.ts` — `useReviews(stationId)`, `useReviewMutation()` (create/update/delete)
- [ ] T032 [US4] Enforce one review per user per station: disable submit button if user already reviewed; show inline error message on duplicate attempt
- [ ] T033 [US4] Verify acceptance: add/remove favorite updates UI immediately (optimistic); submit/edit/delete review works; "No reviews yet" when empty; duplicate review blocked

---

## Phase 5: Clickstream Events (User Story 5) — P3

**Goal**: Every meaningful interaction emits a fire-and-forget clickstream event.

- [ ] T034 [P] [US5] Create `apps/driver-web/src/hooks/useClickstream.ts` — hook that provides `emit(eventName, payload?)` function; uses api-client to POST to analytics endpoint; silently ignores failures
- [ ] T035 [US5] Instrument map events: `page.viewed` (on mount), `map.loaded` (on tiles ready), `map.viewport_changed` (on debounced pan/zoom), `station.marker_clicked` (on marker click)
- [ ] T036 [US5] Instrument search events: `search.performed` (on results returned)
- [ ] T037 [US5] Instrument favorite events: `favorite_station.added`, `favorite_station.removed` (after toggle completes)
- [ ] T038 [US5] Instrument review events: `review.submitted`, `review.updated` (after mutation succeeds)
- [ ] T039 [US5] Instrument auth events: `auth.started` (on modal open), `auth.succeeded`, `auth.failed` (on login result)
- [ ] T040 [US5] Verify acceptance: all 10+ event types fire with correct `EventName` and envelope; user experience is unaffected if event emission fails

---

## Phase 6: Polish & Cross-Cutting

**Purpose**: Edge case hardening, RTL verification, performance benchmarks, error boundaries.

- [ ] T041 [SC-007] RTL audit: verify all new components (StationDetailPanel, SearchOverlay, ReviewForm, etc.) use CSS logical properties — no hardcoded `left`/`right`
- [ ] T042 [SC-001, SC-002, SC-005] Performance verification: map mount <500ms, viewport update <1s, search <500ms, skeleton appears <200ms
- [ ] T043 Build `apps/driver-web/src/components/ErrorBoundary.tsx` — catches render errors, shows fallback UI with retry button
- [ ] T044 Edge case integration: rapid panning (debounce + cancellation), network error on auth (retry/dismiss), expired JWT (re-auth loop), empty states for all lists
- [ ] T045 Update `AGENTS.md` to reference Sprint 9 plan path
- [ ] T046 Build `apps/driver-web` and fix any compilation errors; run existing unit tests to verify no regressions

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 0**: No dependencies — setup only
- **Phase 1 (US1)**: Depends on Phase 0 — needs app shell + dependencies
- **Phase 2 (US2)**: Depends on Phase 1 — needs working map with markers
- **Phase 3 (US3)**: Depends on Phase 0 — independent of map/detail (parallel with Phases 1–2 for auth-client work; dependent for gated action wiring)
- **Phase 4 (US4)**: Depends on Phase 2 + Phase 3 — needs detail panel + auth
- **Phase 5 (US5)**: Depends on Phases 1–4 — instrumentation touches all features
- **Phase 6**: Depends on all phases — final hardening

### Within Each Phase

- Token files are independent (can be parallel within a phase)
- Components without shared state can be built in parallel
- Hooks depend on api-client being configured (Phase 0 T004)

### Parallel Opportunities

| Phase | Parallel tasks |
|-------|---------------|
| Phase 1 | T005 (map-container update) + T006 (hook) + T007 (viewport hook) + T008 (overlay component) |
| Phase 2 | T011–T015 (all 5 components) + T016 (hooks) |
| Phase 4 | T026 (FavoriteButton) + T029 (ReviewForm) + T030 (ReviewList) + T031 (hooks) |
| Phase 5 | T035–T039 (per-feature instrumentation) |

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Phase 0 — Setup app shell with QueryClient + Router
2. Phase 1 — Map with markers, viewport query, clustering
3. **STOP and VALIDATE**: Map loads, markers cluster/uncluster, viewport debounce works

### Incremental Delivery

1. Phase 0 + Phase 1 → Interactive map with station discovery
2. Add Phase 2 → Station detail panel + search
3. Add Phase 3 → Progressive auth
4. Add Phase 4 → Favorites + reviews (requires auth)
5. Add Phase 5 → Clickstream instrumentation
6. Add Phase 6 → Polish, RTL, perf, error handling

---

## Notes

- `@tanstack/react-query` is the single source of truth for all server state; no local state duplication
- Auth is handled at the UI layer (modal trigger) and data layer (JWT injection in api-client)
- Clickstream events are fire-and-forget: `emit()` returns void, errors are swallowed
- Favorites live only as an inline map filter — no `/favorites` route
- Side panel resizes the map (flex layout), does not overlay it
- All new components must use design-system tokens (no inline colors/spacing)
- RTL support via CSS logical properties only — no separate RTL stylesheets
