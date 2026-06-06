# Tasks: Driver Web App with Mock Data

**Input**: Design documents from `/specs/002-driver-web-mock/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Component tests included per story to verify rendering variants and states.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

- **App root**: `apps/driver-web/`
- **Source**: `apps/driver-web/src/`
- **Components**: `apps/driver-web/src/components/`
- **Screens**: `apps/driver-web/src/screens/`
- **Mocks**: `apps/driver-web/src/mocks/`
- **i18n**: `apps/driver-web/src/i18n/`
- **Hooks**: `apps/driver-web/src/hooks/`
- **Types**: `apps/driver-web/src/types/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Scaffold the Driver Web App with Vite + React + TypeScript, configure Tailwind, i18n, and routing.

- [ ] T001 Initialize `apps/driver-web` with Vite + React + TypeScript using `pnpm create vite` template
- [ ] T002 Create `apps/driver-web/package.json` with workspace config, scripts (dev, build, test, lint, format), and all dependencies: react-router-dom, react-i18next, i18next, i18next-browser-languagedetector, tailwindcss, postcss, autoprefixer, vitest, @vitejs/plugin-react, @testing-library/react, @testing-library/jest-dom, @testing-library/user-event, jsdom, eslint, prettier
- [ ] T003 [P] Create `apps/driver-web/vite.config.ts` with React plugin, path aliases (`@/` → `src/`), and vitest configuration with jsdom environment
- [ ] T004 [P] Create `apps/driver-web/tsconfig.json` extending the root tsconfig with path aliases and strict mode
- [ ] T005 [P] Create `apps/driver-web/tailwind.config.ts` extending `packages/ui/tailwind.config.base.js` with `apps/driver-web/src/` content paths
- [ ] T006 [P] Create `apps/driver-web/postcss.config.js` with tailwindcss and autoprefixer plugins
- [ ] T007 Create `apps/driver-web/src/index.css` with Tailwind directives (`@tailwind base/components/utilities`) and base RTL-aware styles
- [ ] T008 Create `apps/driver-web/src/main.tsx` with React.StrictMode, i18n init, and RouterProvider
- [ ] T009 Create `apps/driver-web/src/i18n/index.ts` configuring i18next with react-i18next, browser language detector, Arabic and French resources, and RTL direction detection
- [ ] T010 [P] Create `apps/driver-web/src/i18n/ar.json` with all Arabic translations (Home, Station Detail, Search, Favorites, Profile, Login/Register, component labels, error/empty states)
- [ ] T011 [P] Create `apps/driver-web/src/i18n/fr.json` with all French translations (same keys as ar.json)
- [ ] T012 Create `apps/driver-web/src/App.tsx` with React Router configuration using createBrowserRouter — all 6 routes declared with path and element: `/` (Home/Map), `/stations/:id` (Station Detail), `/search` (Search Results), `/favorites` (Favorites), `/profile` (Profile), `/login` (Login/Register)

**Checkpoint**: `pnpm dev` starts the Vite dev server. Router renders with all 6 routes returning placeholder content.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Types, mock data, layout wrapper, and hooks that ALL user stories depend on.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T013 Create `apps/driver-web/src/types/index.ts` with TypeScript interfaces: Station, Charger, Review, DriverUser, FilterState (mirroring data-model.md fields exactly — Station.id, name, address, coordinates, distance, chargerCount, availableCount, availability, rating, reviewCount; Charger.id, stationId, connectorType, powerKw, availability, pricePerKwh; Review.id, stationId, authorName, rating, text, date, language; DriverUser.id, name, email, phone, avatarUrl, favoriteStationIds, language; FilterState.chargerType, availability, searchQuery)
- [ ] T014 [P] Create `apps/driver-web/src/mocks/stations.ts` — 15 stations with Tunisian addresses and realistic coordinates (cities: Tunis, Ariana, Ben Arous, Sfax, Sousse, Nabeul, Bizerte), IDs STN-001 through STN-015, typed as `Station[]`
- [ ] T015 [P] Create `apps/driver-web/src/mocks/chargers.ts` — 2–4 chargers per station (40–60 total) with connector types Type2/CCS/CHAdeMO, power 3.7–350 kW, availability statuses, NanoID prefixes CHG-{stationNum}-{index}, typed as `Charger[]`
- [ ] T016 [P] Create `apps/driver-web/src/mocks/reviews.ts` — 3–5 reviews per station (45–75 total) with 1–5 star ratings, Arabic and French text content, NanoID prefixes REV-{stationNum}-{index}, typed as `Review[]`
- [ ] T017 [P] Create `apps/driver-web/src/mocks/users.ts` — 1 mock user (USR-001) with favorites referencing 3 station IDs, typed as `DriverUser[]`
- [ ] T018 Create `apps/driver-web/src/components/MobileTopBar.tsx` — Top bar with hamburger menu icon (button with ARIA label "Open menu"), brand name "BorneMap", notification bell with count badge, uses useTranslation for labels, accepts MobileTopBarProps interface per contracts
- [ ] T019 Create `apps/driver-web/src/hooks/useStations.ts` — Hook returning mock station data: `useStations()` returns `{ stations: Station[], getStationById(id: string): Station | undefined, getChargersForStation(stationId: string): Charger[], getReviewsForStation(stationId: string): Review[] }` — imports from mocks/
- [ ] T020 [P] Create `apps/driver-web/src/hooks/useFavorites.ts` — Hook using React context for favorites management: `useFavorites()` returns `{ favorites: string[], isFavorite(id: string): boolean, toggleFavorite(id: string): void }`, backed by a React context provider wrapping the app
- [ ] T021 [P] Create `apps/driver-web/src/hooks/useMockFilter.ts` — Hook for filter+search state: `useMockFilter()` returns `{ filter: FilterState, setChargerType(type), setAvailability(filter), setSearchQuery(query), filteredStations: Station[] }` — applies charger type, availability, and text search filters to stations array

**Checkpoint**: Types defined, mock data importable and typed, MobileTopBar renders with i18n, hooks return data without errors.

---

## Phase 3: User Story 1 - Browse Stations on Map (Priority: P1) 🎯 MVP

**Goal**: A driver can visit the home page and see charging stations on a map with search and filter controls.

**Independent Test**: Load `/` — map placeholder (#EAF0E6) shows station markers, sidebar lists StationCards with name/address/distance/charger count/availability badge, SearchBar and FilterPills are visible above the list.

### Implementation for User Story 1

- [ ] T022 [P] [US1] Create `apps/driver-web/src/components/SearchBar.tsx` — Floating card-style input with search icon, text input with placeholder from i18n, onChange handler, onSubmit on Enter, autoFocus prop, ARIA label "Search stations"
- [ ] T023 [P] [US1] Create `apps/driver-web/src/components/FilterPills.tsx` — Two horizontal rows of pill buttons: Charger Type row (All, Type2, CCS, CHAdeMO) and Availability row (All, Available only), active pill has highlighted background from design tokens, inactive is muted, ARIA pressed state on each pill
- [ ] T024 [P] [US1] Create `apps/driver-web/src/components/MapPinMarker.tsx` — Circle marker positioned absolutely within map container via percentage top/left, three visual states: default (green), selected (primary blue with glow shadow), unavailable (gray), ARIA label with station name and availability
- [ ] T025 [P] [US1] Create `apps/driver-web/src/components/ZoomControls.tsx` — Fixed-position vertical button group with + and - buttons, rounded buttons with shadow, ARIA labels "Zoom in" and "Zoom out", onClick handlers (no-op for this sprint)
- [ ] T026 [P] [US1] Create `apps/driver-web/src/components/StationCard.tsx` — Card with station name (bold), address (secondary text), distance in km, charger count (X/Y available), availability Badge component, rating stars, click handler to navigate to `/stations/:id`
- [ ] T027 [US1] Create `apps/driver-web/src/screens/HomeMapScreen.tsx` — Split layout: left side full-bleed map placeholder (bg #EAF0E6) with positioned MapPinMarker divs from mock stations, right sidebar scrollable StationCard list, top bar with SearchBar + FilterPills above station list, ZoomControls overlaid on map, BottomStationCard pinned at bottom of map area showing selected station summary, MobileTopBar at top, both SearchBar and FilterPills wired to useMockFilter hook

**Checkpoint**: Navigating to `/` shows the home screen with map placeholder, station markers, working SearchBar and FilterPills, StationCard list in sidebar with correct mock data.

---

## Phase 4: User Story 2 - View Station Details & Reviews (Priority: P1)

**Goal**: A driver can click a station card or marker to see full station details with chargers and reviews.

**Independent Test**: Click a StationCard on the home page or navigate to `/stations/STN-001` — station info renders, charger list shows connector type/power/status, reviews show star ratings and content.

### Implementation for User Story 2

- [ ] T028 [P] [US2] Create `apps/driver-web/src/components/ChargerRow.tsx` — Row layout showing connector type icon/name, power in kW (e.g., "50 kW"), StatusBadge with availability, and price per kWh. Uses data from Charger interface. ARIA label with connector type and status.
- [ ] T029 [P] [US2] Create `apps/driver-web/src/components/ReviewCard.tsx` — Card with star rating display (filled/empty stars), author name, relative date (e.g., "il y a 3 jours"), review text, respects RTL for Arabic content by setting `dir` based on review.language, ARIA label with rating value
- [ ] T030 [P] [US2] Create `apps/driver-web/src/components/BottomStationCard.tsx` — Compact station summary card with name, address, distance, availability badge, charger availability X/Y, rating, spec rows array, "Get directions" button (visual-only), onClick handler to navigate to detail
- [ ] T031 [US2] Create `apps/driver-web/src/screens/StationDetailScreen.tsx` — Station info header (name, address, map pin), ChargerRow list section, rating summary row (average + count), ReviewCard list section, uses `useParams` for station ID, loads data from useStations hook, MobileTopBar at top, handles invalid station ID with ErrorState component from packages/ui

**Checkpoint**: Navigating to `/stations/STN-001` shows station detail with chargers and reviews. Back navigation from browser works.

---

## Phase 5: User Story 3 - Search, Filter & Favorites (Priority: P2)

**Goal**: A driver can search stations by text, filter by charger type/availability, and save favorites.

**Independent Test**: Navigate to `/search` with query params — filtered results display. `/favorites` shows saved stations or EmptyState. FilterPills on home page filter the station list.

### Implementation for User Story 3

- [ ] T032 [P] [US3] Create `apps/driver-web/src/screens/SearchResultsScreen.tsx` — SearchBar at top with pre-filled query from URL params or state, FilterPills below, paginated StationCard list, EmptyState when no results, results populated from useMockFilter hook
- [ ] T033 [P] [US3] Create `apps/driver-web/src/screens/FavoritesScreen.tsx` — StationCard list filtered by useFavorites hook favorites IDs, EmptyState when no favorites saved, uses MobileTopBar
- [ ] T034 [US3] Wire FilterPills on HomeMapScreen to re-filter station list in real time as pills change — FilterPills onChange calls useMockFilter setter, StationCard list updates automatically
- [ ] T035 [US3] Extend StationCard with favorite toggle icon (heart outline/filled) — wire to useFavorites hook, isFavorite reflects on cards with filled/outline heart icon across all screens
- [ ] T035b [P] [US3] Test StationCard favorite toggle renders heart icon in correct state, toggles on click, calls useFavorites hook

**Checkpoint**: `/search` shows filtered results. `/favorites` shows saved stations. Home map FilterPills re-filter the list instantly. Heart icon toggles on card click.

---

## Phase 6: User Story 4 - Profile & Login/Register (Priority: P3)

**Goal**: A driver can view profile form and login/register screen with static layout.

**Independent Test**: Navigate to `/profile` — form with name, email, phone inputs renders. `/login` — centered card with email, password inputs and social login buttons renders.

### Implementation for User Story 4

- [ ] T036 [P] [US4] Create `apps/driver-web/src/screens/ProfileScreen.tsx` — Form layout with Input components (name, email, phone) from packages/ui, Button component for "Save Changes" (non-functional), avatar placeholder, MobileTopBar, all labels from i18n
- [ ] T037 [P] [US4] Create `apps/driver-web/src/screens/LoginRegisterScreen.tsx` — Centered card with tab toggle (Login | Register), Input for email and password, Button for submit (non-functional), social login buttons row with Google/Apple/Facebook icons (visual-only), link to register if on login tab and vice versa, MobileTopBar

**Checkpoint**: `/profile` and `/login` render with all form fields visible. No submission logic — static only.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Build verification, RTL verification, accessibility, and final quality checks.

- [ ] T038 [P] Test MobileTopBar renders with hamburger, brand name, notification bell, toggles sidebar, handles RTL correctly
- [ ] T039 [P] Test SearchBar renders with placeholder, handles keystroke onChange, submits on Enter, supports autoFocus
- [ ] T040 [P] Test FilterPills renders two rows, toggles active/inactive states on click, calls onChange with correct value
- [ ] T041 [P] Test MapPinMarker renders default/selected/unavailable states with correct colors and ARIA labels
- [ ] T042 [P] Test ZoomControls renders +/- buttons and fires onClick handlers
- [ ] T043 [P] Test StationCard renders name, address, distance, charger count, availability badge, rating stars; onClick fires with station ID
- [ ] T044 [P] Test ChargerRow renders connector type, power kW, StatusBadge, price per kWh
- [ ] T045 [P] Test ReviewCard renders star rating, author, date, text; respects RTL dir based on language prop
- [ ] T046 [P] Test BottomStationCard renders station summary, spec rows, onClick fires with station ID
- [ ] T047 Verify `pnpm build` for `apps/driver-web` completes with zero warnings — `pnpm --filter @borne-map/driver-web build`
- [ ] T048 Manual RTL verification: set language to Arabic, verify every screen has correct `dir="rtl"`, sidebar on right, layout flipped, no broken elements
- [ ] T049 Manual navigation verification: click through all 6 screens, verify browser back button, verify direct URL entry for each route
- [ ] T050 [P] Verify color contrast: all text meets ≥4.5:1 ratio (≥3:1 for large text) using contrast checker on all 6 screens
- [ ] T051 [P] Run accessibility audit (aXe DevTools or lighthouse) on all 6 screens — fix all WCAG 2.1 AA violations
- [ ] T052 [P] Verify keyboard navigation: Tab through all interactive elements on every screen, verify focus indicators are visible, Enter/Space activates buttons and links
- [ ] T053 Final network tab verification: confirm zero backend/API calls during full app walkthrough

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — can start immediately
- **Phase 2 (Foundational)**: Depends on Setup — BLOCKS all user stories
- **Phase 3 (US1 - Browse Map, P1)**: Depends on Foundational — MVP deliverable
- **Phase 4 (US2 - Station Detail, P1)**: Depends on Foundational — can run parallel with US1
- **Phase 5 (US3 - Search/Favorites, P2)**: Depends on Foundational + US1 (reuses SearchBar, FilterPills, StationCard)
- **Phase 6 (US4 - Profile/Login, P3)**: Depends on Foundational — independent of other stories
- **Phase 7 (Polish)**: Depends on all 4 user stories

### User Story Dependencies

- **US1 (P1)**: No user story dependencies — starts after Phase 2
- **US2 (P1)**: No user story dependencies — starts after Phase 2, independent of US1
- **US3 (P2)**: Uses US1 components (SearchBar, FilterPills, StationCard) — sequential after US1
- **US4 (P3)**: No user story dependencies — starts after Phase 2, independent of US1/US2

### Parallel Opportunities

- T003/T004/T005/T006 (setup config files) — parallel
- T014/T015/T016/T017 (mock data files) — parallel
- T022/T023/T024/T025/T026 (US1 components) — parallel within phase
- T028/T029/T030 (US2 components) — parallel within phase
- T032/T033 (US3 screens) — parallel
- T036/T037 (US4 screens) — parallel
- T038–T046 (component tests) — all parallel
- US2 can start in parallel with US1 (no component overlap)
- US4 can start in parallel with US1 (no component overlap)

---

## Parallel Example: User Story 1

```bash
# Launch all components for User Story 1 together:
Task: "Create SearchBar in apps/driver-web/src/components/SearchBar.tsx"
Task: "Create FilterPills in apps/driver-web/src/components/FilterPills.tsx"
Task: "Create MapPinMarker in apps/driver-web/src/components/MapPinMarker.tsx"
Task: "Create ZoomControls in apps/driver-web/src/components/ZoomControls.tsx"
Task: "Create StationCard in apps/driver-web/src/components/StationCard.tsx"

# After all components pass review, create the screen:
Task: "Create HomeMapScreen in apps/driver-web/src/screens/HomeMapScreen.tsx"
```

---

## Implementation Strategy

### MVP First (User Story 1 + 2 — Both P1)

1. Complete Phase 1: Setup — scaffold + config files
2. Complete Phase 2: Foundational — types, mock data, layout, hooks
3. Complete Phase 3: User Story 1 — Home/Map browse
4. Complete Phase 4: User Story 2 — Station Detail (can run parallel with US3)
5. **STOP and VALIDATE**: Both P1 stories independently testable
6. Deploy/demo if ready

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add US1 + US2 → Test independently → Demo (core browsing complete!)
3. Add US3 (Search/Favorites) → Test independently → Demo
4. Add US4 (Profile/Login) → Test independently → Demo
5. Polish → Build verification → RTL check → Final demo

### Parallel Team Strategy

With multiple developers:
1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: US1 (Home/Map) + US3 (Search/Favorites — reuses US1 components)
   - Developer B: US2 (Station Detail) + US4 (Profile/Login — independent)
3. Developer B can also handle component tests in Phase 7
4. Stories complete and integrate independently
