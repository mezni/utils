# Tasks: Integration & Testing

**Input**: Design documents from `/specs/005-integration-testing/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests ARE the product in this phase — every task is a test configuration, test script, or test execution.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Infrastructure**: `infra/docker-compose.yml`, `infra/traefik/`
- **Backend services**: `source/services/driver-service/`, `source/services/admin-service/`
- **Mobile app**: `source/front/mobile-driver/`
- **Web app**: `source/front/web-driver/`
- **Test data**: `infra/migrations/004_seed_stations.sql`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure for integration testing

- [ ] T001 Create Traefik configuration directory and static routing file in `infra/traefik/dynamic.yml`
- [ ] T002 [P] Add Traefik service to Docker Compose in `infra/docker-compose.yml`
- [ ] T003 [P] Configure Traefik routing rules for driver-service (`PathPrefix /api/v1/stations` → `:8080`) in `infra/traefik/dynamic.yml`
- [ ] T004 [P] Configure Traefik routing rules for admin-service (`PathPrefix /api/v1/admin, /api/v1/events` → `:8081`) in `infra/traefik/dynamic.yml`
- [ ] T005 [P] Configure Traefik error handling middleware (503 on upstream down) in `infra/traefik/dynamic.yml`
- [ ] T006 [P] Configure Traefik rate limiting middleware (100 req/s per IP) in `infra/traefik/dynamic.yml`
- [ ] T007 [P] Install Maestro CLI for mobile E2E tests (document in `source/front/mobile-driver/README.md`)
- [ ] T008 [P] Install k6 for load testing (document in `specs/005-integration-testing/quickstart.md`)
- [ ] T009 [P] Install Pact CLI or pact-rust for contract testing in `source/services/driver-service/Cargo.toml` and `source/services/admin-service/Cargo.toml`
- [ ] T009b [P] Document Android emulator / iOS simulator setup for mobile E2E tests in `source/front/mobile-driver/README.md`
- [ ] T009c [P] Add fallback note: mobile E2E tests require emulator; web-only E2E (Playwright) can run without it in `source/front/mobile-driver/README.md`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core wiring that MUST be complete before ANY user story E2E tests can run

**Critical**: No user story work can begin until this phase is complete

- [ ] T010 Update mobile app API base URL to Traefik gateway (`http://localhost:8080`) in `source/front/mobile-driver/config/env.ts`
- [ ] T011 Update web app API base URL to Traefik gateway (`http://localhost:8080`) in `source/front/web-driver/src/config/env.ts`
- [ ] T012 [P] Add `API_BASE_URL` environment variable documentation in `source/front/mobile-driver/.env.example`
- [ ] T013 [P] Add `API_BASE_URL` environment variable documentation in `source/front/web-driver/.env.example`
- [ ] T014 Verify Docker Compose starts all services (PostGIS, driver-service, admin-service, Traefik) with `docker-compose up -d` and test connectivity

**Checkpoint**: Foundation ready — all services running behind Traefik, apps configured to route through gateway

---

## Phase 3: User Story 1 - API Gateway Routing (Priority: P1) 🎯 MVP

**Goal**: Verify Traefik correctly routes requests to the appropriate backend service and handles error conditions

**Independent Test**: Send requests to `http://localhost:8080/api/v1/stations` and verify response from driver-service; send to `http://localhost:8080/api/v1/admin/stations` and verify response from admin-service; send unknown route and verify 404; stop driver-service and verify 503

- [ ] T015 [P] [US1] Write script to test Traefik routes to driver-service `GET /api/v1/stations` in `specs/005-integration-testing/tests/traefik-routing.sh`
- [ ] T016 [P] [US1] Write script to test Traefik routes to admin-service `GET /api/v1/admin/stations` in `specs/005-integration-testing/tests/traefik-routing.sh`
- [ ] T017 [P] [US1] Write script to test unknown route returns 404 in `specs/005-integration-testing/tests/traefik-routing.sh`
- [ ] T018 [P] [US1] Write script to test upstream-down returns 503 in `specs/005-integration-testing/tests/traefik-routing.sh`
- [ ] T019 [US1] Run all Traefik routing tests and verify 100% pass

**Checkpoint**: API gateway routing verified — Traefik correctly routes to both services, handles errors

---

## Phase 4: User Story 2 - App-to-Backend Wiring (Priority: P1) 🎯 MVP

**Goal**: Verify mobile and web apps successfully communicate with backend services through Traefik gateway

**Independent Test**: Launch mobile app configured with Traefik URL, verify station list loads from backend; launch web app with same config, verify same data appears

- [ ] T020 [P] [US2] Configure mobile app dev environment to use Traefik gateway URL in `source/front/mobile-driver/.env`
- [ ] T021 [P] [US2] Configure web app dev environment to use Traefik gateway URL in `source/front/web-driver/.env`
- [ ] T022 [US2] Start all services with Traefik, launch mobile app, verify station markers load on map from backend data
- [ ] T023 [US2] Launch web app, verify same station data loads through Traefik
- [ ] T024 [US2] Create a station via admin API, verify it appears in both mobile and web app within 30 seconds

**Checkpoint**: Mobile and web apps communicate through Traefik — data flows end to end

---

## Phase 5: User Story 3 - End-to-End Discovery Flow (Priority: P1) 🎯 MVP

**Goal**: Verify the complete station discovery flow works end to end: geolocation → nearby stations → map markers → station detail → chargers

**Independent Test**: Run automated E2E test that simulates user location near Tunis Central (36.8065, 10.1815), verifies markers appear, taps a marker, and checks station detail screen loads with charger data

### Tests for User Story 3

- [ ] T025 [P] [US3] Create Maestro E2E test flow for station discovery in `source/front/mobile-driver/e2e/discovery-flow.yaml`
- [ ] T026 [P] [US3] Write test step: simulate geolocation at test station coordinates
- [ ] T027 [P] [US3] Write test step: wait for map markers to appear within 5s timeout
- [ ] T028 [P] [US3] Write test step: tap first station marker
- [ ] T029 [P] [US3] Write test step: verify station detail screen shows name, address, charger info
- [ ] T030 [P] [US3] Write test step: pull to refresh on station list
- [ ] T031 [P] [US3] Write test step: verify recovery actions (retry button + back nav) on simulated network failure
- [ ] T032 [P] [US3] Create Playwright E2E test for web app discovery flow in `source/front/web-driver/e2e/discovery-flow.spec.ts`
- [ ] T033 [P] [US3] Write web E2E test: load map, verify markers appear
- [ ] T034 [P] [US3] Write web E2E test: click marker, verify detail screen
- [ ] T035 [P] [US3] Write dark mode E2E test: toggle theme, verify all screens render correctly in `source/front/mobile-driver/e2e/dark-mode.yaml`
- [ ] T036 [P] [US3] Write auth rejection test: send request without auth header, verify 401/403 graceful rejection (no crash) in `specs/005-integration-testing/tests/auth-rejection.sh`
- [ ] T037 [US3] Run full discovery E2E test suite, verify all scenarios pass on 5 consecutive runs

**Checkpoint**: Complete discovery flow validated end to end — MVP core functionality verified

---

## Phase 6: User Story 4 - Event Logging End-to-End (Priority: P2)

**Goal**: Verify user interactions (station views, searches, navigation) are captured as events in the analytics database

**Independent Test**: Perform specific user actions (view station, search, navigate), then query the analytics database to verify corresponding events were captured with correct fields

- [ ] T038 [US4] Trigger a station detail view action, verify `station_detail_view` event appears in `analytics_db.raw_events` with correct `station_id`
- [ ] T039 [US4] Trigger a text search ("Tunis"), verify `search` event appears with `search_query` and `result_count`
- [ ] T040 [US4] Trigger a nearby search at test coordinates, verify `nearby_search` event with correct `lat`/`lng`
- [ ] T041 [US4] Trigger a navigation action to a station, verify `navigate_to_station` event with correct `station_id`
- [ ] T042 [US4] Send a batch of 50+ events, verify all events persisted without data loss in `analytics_db.raw_events`
- [ ] T043 [US4] Send malformed event data, verify 400 error with validation message (no partial persistence)
- [ ] T044 [US4] Document event logging E2E test procedure in `specs/005-integration-testing/quickstart.md`

**Checkpoint**: Event logging verified — all interaction types captured with correct data in analytics database

---

## Phase 7: User Story 5 - Contract & Performance Validation (Priority: P2)

**Goal**: Verify API responses conform to documented contracts and meet performance thresholds

**Independent Test**: Run Pact contract tests against all driver-service and admin-service endpoints; run k6 load test with 50 concurrent requests targeting nearby search

### Contract Tests

- [ ] T045 [P] [US5] Write Pact contract test for `GET /api/v1/stations` in `source/services/driver-service/tests/contract_tests.rs`
- [ ] T046 [P] [US5] Write Pact contract test for `GET /api/v1/stations/{id}` in `source/services/driver-service/tests/contract_tests.rs`
- [ ] T047 [P] [US5] Write Pact contract test for `GET /api/v1/stations/nearby` in `source/services/driver-service/tests/contract_tests.rs`
- [ ] T048 [P] [US5] Write Pact contract test for `GET /api/v1/health` in `source/services/driver-service/tests/contract_tests.rs`
- [ ] T049 [P] [US5] Write Pact contract tests for admin-service station CRUD endpoints in `source/services/admin-service/tests/contract_tests.rs`
- [ ] T050 [P] [US5] Write Pact contract tests for admin-service event endpoints (single + batch) in `source/services/admin-service/tests/contract_tests.rs`
- [ ] T051 [US5] Run all contract tests, verify 100% pass for both success and error response schemas

### Performance Tests

- [ ] T052 [P] [US5] Write k6 load test script for nearby search endpoint with 50 concurrent VUs in `specs/005-integration-testing/tests/load-test.js`
- [ ] T053 [P] [US5] Write k6 load test script for station list endpoint with 50 concurrent VUs in `specs/005-integration-testing/tests/load-test.js`
- [ ] T054 [US5] Run load tests, verify p95 nearby search latency < 100ms
- [ ] T055 [US5] Document contract and performance test procedures in `specs/005-integration-testing/quickstart.md`
- [ ] T055b [US5] Verify FR-016 across all endpoints: send requests without auth header to driver-service (`/api/v1/stations`, `/api/v1/stations/{id}`, `/api/v1/stations/nearby`, `/health`) and admin-service (CRUD, events) — all return 401/403 gracefully in `specs/005-integration-testing/tests/auth-rejection.sh`

**Checkpoint**: Contract compliance and performance baselines established — all endpoints meet documented contracts and latency targets

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Automation, reporting, and documentation that affects all user stories

- [ ] T056 Create GitHub Actions workflow for integration tests in `.github/workflows/integration-tests.yml`
- [ ] T057 [P] Configure contract test job in CI workflow
- [ ] T058 [P] Configure E2E test job (web) in CI workflow
- [ ] T059 [P] Configure load test job in CI workflow
- [ ] T060 Create test report aggregation script that produces single pass/fail summary with timing and failure details in `specs/005-integration-testing/tests/report.sh`
- [ ] T061 Run quickstart.md validation — verify all documented commands work
- [ ] T062 Final review: verify all 16 functional requirements have corresponding tests

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — Traefik must be running before routing tests
- **User Story 2 (Phase 4)**: Depends on US1 completion — Traefik routing must be verified before app wiring
- **User Story 3 (Phase 5)**: Depends on US2 completion — apps must be wired to Traefik before E2E discovery tests
- **User Story 4 (Phase 6)**: Can start after US2 (apps wired) but independent of US3 — events flow through admin-service
- **User Story 5 (Phase 7)**: Can start after Foundational — contract and perf tests run directly against services, independent of frontend wiring
- **Polish (Phase 8)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational — No dependencies on other stories
- **User Story 2 (P1)**: Depends on US1 (Traefik routing must work before apps can route through it)
- **User Story 3 (P1)**: Depends on US2 (apps must be wired before E2E flow can be tested)
- **User Story 4 (P2)**: Depends on Foundational — can run in parallel with US3 (if staffed) since events and discovery are independent flows
- **User Story 5 (P2)**: Depends on Foundational — can run in parallel with all other stories (contract/perf tests are service-side only)

### Within Each User Story

- Tests are written and verified to fail before marking complete
- Configuration before execution
- Simple curl/manual verification before automated test scripts
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel (T002-T009)
- All US1 routing test scripts can run in parallel (T015-T018)
- US4 (events) and US5 (contract/perf) can run in parallel with US3 (discovery E2E) after US2 completes
- Within US5: all contract tests (T045-T050) and load test scripts (T052-T053) can run in parallel
- Polish CI tasks (T057-T059) can run in parallel

---

## Parallel Example: User Story 1

```bash
# Launch all routing test scripts together:
Task: "Write script to test Traefik routes to driver-service"
Task: "Write script to test Traefik routes to admin-service"
Task: "Write script to test unknown route returns 404"
Task: "Write script to test upstream-down returns 503"
```

## Parallel Example: User Story 5

```bash
# Launch all contract tests together:
Task: "Write Pact contract test for GET /api/v1/stations"
Task: "Write Pact contract test for GET /api/v1/stations/{id}"
Task: "Write Pact contract test for GET /api/v1/stations/nearby"
Task: "Write Pact contract test for GET /api/v1/health"
Task: "Write Pact contract test for admin CRUD endpoints"
Task: "Write Pact contract test for admin event endpoints"

# Launch load test scripts together:
Task: "Write k6 load test for nearby search"
Task: "Write k6 load test for station list"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup — Traefik config, Docker Compose
2. Complete Phase 2: Foundational — App wiring, tool install
3. Complete Phase 3: User Story 1 — API Gateway Routing tests
4. **STOP and VALIDATE**: Traefik routes correctly
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Infrastructure ready
2. Add User Story 1 → Traefik routing verified → MVP (gateway operational)
3. Add User Story 2 → Apps wired through gateway → Integration baseline
4. Add User Story 3 → Full discovery flow E2E → Core UX validated (complete MVP)
5. Add User Story 4 → Event logging verified → Observability validated
6. Add User Story 5 → Contracts + performance → Production readiness confidence
7. Polish → CI automation + reporting

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (Traefik routing) + User Story 2 (app wiring) → sequential dependency
   - Developer B: User Story 5 (contract/perf tests) — independent, service-side only
3. After US1+US2 complete:
   - Developer A: User Story 3 (discovery E2E) + User Story 4 (event logging) — can share E2E infrastructure
   - Developer B: Polish (CI pipeline, reporting) — depends on all stories
4. Stories integrate and verify independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Verify tests fail before marking complete
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- All test scripts must be executable: `bash script.sh` or `k6 run script.js`
- Traefik routing tests use curl against localhost:8080
- Contract tests use Pact framework within Rust test harness
- E2E tests use Maestro CLI (mobile) and Playwright (web)
- Load tests use k6 with Grafana dashboard output
