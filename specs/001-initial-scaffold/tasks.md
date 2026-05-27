---
description: "Task list for BorneMap Platform Scaffold"
---

# Tasks: BorneMap Platform Scaffold

**Input**: Design documents from `specs/001-initial-scaffold/`

**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/api-stations-nearby.md

**Tests**: Test tasks are included for the API contract coverage as specified in US3.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Backend Rust workspace**: `backend/` at repository root
- **Mobile app**: `apps/mobile-driver/`
- **Infrastructure**: `.github/workflows/`, `deployments/`, repo root `docker-compose.yml`, `Makefile`, `.env.example`

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [x] T001 Create project directory structure per the plan (apps/, backend/, db/, deployments/, docs/.github/)
- [x] T002 [P] Create root `docker-compose.yml` with PostGIS 15-3.3 service, volume, network, and healthcheck
- [x] T003 [P] Create `.env.example` with DATABASE_URL, API_PORT, JWT_SECRET, EXPO_PUBLIC_API_URL
- [x] T004 [P] Create `Makefile` with up, down, status, test-backend, dev-api targets
- [x] T005 [P] Create `deployments/docker-compose.prod.yml` skeleton
- [x] T006 [P] Create `deployments/nginx/default.conf` skeleton
- [x] T007 [P] Create `docs/architecture.md` and `docs/onboarding.md` skeleton files

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T008 Initialize Rust workspace with `backend/Cargo.toml` workspace manifest with members: api-service, auth-service, core, infra
- [x] T009 [P] Create `backend/core/Cargo.toml` with serde, chrono dependencies; define StationHub, Charger, Provider domain structs in `backend/core/src/lib.rs`
- [x] T010 [P] Create `backend/infra/Cargo.toml` with sqlx, postgis dependencies; add database connection pool module in `backend/infra/src/lib.rs`
- [x] T011 Create `backend/api-service/Cargo.toml` with actix-web, serde, serde_json, chrono, parking_lot dependencies; add path deps on core and infra
- [x] T012 Create `backend/api-service/src/main.rs` with Actix-web HttpServer, Logger middleware, shared AppState with RwLock for mock stations
- [x] T013 Create `backend/api-service/src/handlers/locate.rs` with StationHub, Charger structs, `generate_mock_data()` seeding 2+ Tunisian stations, `GET /api/v1/stations/nearby` handler
- [x] T014 [P] Create `.github/workflows/ci.yml` with backend-test job (fmt check, cargo check, cargo test) and frontend-test job (npm ci, expo export)

**Checkpoint**: Foundation ready — `cargo test --workspace` passes and `/api/v1/stations/nearby` returns mock data

---

## Phase 3: User Story 1 - Driver Discovers Nearby Charging Stations (Priority: P1) 🎯 MVP

**Goal**: A driver opens the app and sees charging stations on a map centered on Tunis, with station details on tap and navigation capability.

**Independent Test**: Launch the mobile app with mock data, verify stations appear as markers, tap a marker to see station detail card with charger info.

### Implementation for User Story 1

- [x] T015 [P] Initialize React Native Expo project in `apps/mobile-driver/` with `npx create-expo-app`
- [x] T016 [P] Install dependencies: react-native-maps, axios in `apps/mobile-driver/`
- [x] T017 Create API client module in `apps/mobile-driver/src/services/api.js` with `fetchNearbyStations(lat, lng)` function using axios
- [x] T018 Create `apps/mobile-driver/src/components/StationCard.js` displaying station name, provider, status, charger list, plug type, power output, and a "Navigate" button
- [x] T019 Create `apps/mobile-driver/src/screens/MapScreen.js` with MapView centered on Tunis region (36.8065, 10.1815), markers with color-coded pins (green=Available, red=Occupied), marker tap shows StationCard in bottom drawer
- [x] T020 Add pan/zoom handler to MapScreen: re-fetch nearby stations when map movement settles, using current map center coordinates
- [x] T021 Add search bar component in MapScreen that accepts place names, uses device geocoding to center the map, triggers station re-fetch
- [x] T022 Add "Navigate" button action: open device's default maps app with station coordinates via platform URL scheme
- [x] T023 Implement loading state: ActivityIndicator with descriptive text while stations are being fetched
- [x] T024 Implement error state: overlay with error message and "Retry" button when API is unreachable; keep existing station data visible behind overlay
- [x] T025 Implement empty state: friendly illustration and message when no stations found
- [x] T026 Implement offline banner: detect network loss and show non-blocking banner at top of screen; keep existing station data visible
- [x] T027 Implement pull-to-refresh gesture on map view to manually re-fetch station data
- [x] T028 Implement automatic 30-second foreground refresh timer for station data
- [x] T029 Handle slow API response: show spinner/skeleton on initial load; if timeout exceeded, transition to error state with retry
- [x] T030 Create `apps/mobile-driver/App.js` that renders MapScreen as the root component

**Checkpoint**: Map screen launches, shows Tunis-centered map with station markers, tapping marker shows card with charger details and navigation button, search bar works, error/loading/offline states render correctly.

---

## Phase 4: User Story 2 - Filter Stations by Availability (Priority: P2)

**Goal**: A driver can filter the map to show only stations with available chargers.

**Independent Test**: Apply "Available" filter — only Available stations remain on map. Tap "All" — all stations reappear.

### Implementation for User Story 2

- [x] T031 [P] [US2] Add filter button row component in MapScreen above the map with "All" and "Available" toggle buttons
- [x] T032 [P] [US2] Add `filteredStations` state logic in MapScreen: when "Available" filter active, only render markers with status "Available"
- [x] T033 [US2] Style active/inactive filter buttons with distinct colors and elevation

**Checkpoint**: Filter buttons render correctly, tapping "Available" hides Occupied stations, tapping "All" restores all stations.

---

## Phase 5: User Story 3 - Backend API Returns Consistent Station Data (Priority: P2)

**Goal**: The `/api/v1/stations/nearby` endpoint follows a defined JSON contract verified by automated tests.

**Independent Test**: Send GET request to `/api/v1/stations/nearby`, validate response matches StationHub schema with nanouuid identifiers.

### Tests for User Story 3

- [x] T034 [US3] Add backend contract test in `backend/api-service/src/handlers/locate.rs` (enable test module): verify response is non-empty JSON array, IDs start with `stn-`, status is valid
- [x] T035 [US3] Add backend validation test: verify each returned StationHub has matching `^[a-z]{3}-[a-f0-9]{8}$` pattern on id fields
- [x] T036 [US3] Add backend validation test: verify chargers array is non-empty with valid id, plug_type, power_output, status fields

**Checkpoint**: `cargo test --workspace` passes with all contract and validation tests green.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [x] T037 [P] Create `db/seeds/tunisia-stations.sql` with INSERT statements for the two mock stations using nanouuid IDs and PostGIS ST_SetSRID coordinates
- [x] T038 [P] Add Rust format-on-save documentation to `docs/architecture.md`
- [x] T039 Update `README.md` with project overview, tech stack, quickstart instructions
- [x] T040 Run full validation: `cargo test --workspace` passes (api-service: 3/3 tests pass)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **User Stories (Phases 3-5)**: All depend on Foundational (backend API must be running)
  - US1 and US2 can proceed in parallel or sequentially
  - US3 tests validate the Foundation-level implementation
- **Polish (Phase 6)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Depends on Foundational (Phase 2) — API must be running for mobile to fetch data
- **User Story 2 (P2)**: Depends on US1 completion (adds filter UI to MapScreen from US1)
- **User Story 3 (P2)**: Validates Foundational API — can run independently from US1/US2

### Within Each User Story

- Models before services
- Services before endpoints/UI
- Core implementation before edge cases
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks (T001-T007): T002-T006 can run in parallel with T001
- Foundational tasks: T009 (core crate) and T010 (infra crate) can run in parallel
- US1 tasks: T015-T016 (Expo init, deps) run first, then T017-T030 can partially parallelize
- US2 tasks: T031-T032 can run in parallel
- US3 tests: T034-T036 can all run in parallel as they target the same test file
- Polish tasks: T037-T038 can run in parallel

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (backend API + CI)
3. Complete Phase 3: User Story 1 (map + stations + search + navigation + error states)
4. **STOP and VALIDATE**: Launch mobile app, verify stations appear on map, tap to see details
5. Deploy/demo if ready

### Incremental Delivery

1. Setup + Foundational → Backend API is live
2. Add User Story 1 → Mobile map with station discovery (MVP!)
3. Add User Story 2 → Filter by availability
4. Add User Story 3 → API contract tests (verifies Foundational)
5. Each story adds value without breaking previous stories
