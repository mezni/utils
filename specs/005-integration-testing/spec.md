# Feature Specification: Integration & Testing

**Feature Branch**: `005-integration-testing`

**Created**: 2026-06-12

**Status**: Draft

**Input**: User description: "read from mvp 1 phase 5"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - API Gateway Routing (Priority: P1)

As a developer, I want all API traffic to route through the Traefik gateway so mobile and web apps have a single entry point and the backend services are not directly exposed.

**Why this priority**: The gateway is the infrastructure foundation — without it, the apps cannot communicate with backend services in a production-like manner. Every other test depends on proper routing.

**Independent Test**: Can be fully tested by sending requests to the Traefik endpoint and verifying they are correctly routed to driver-service (`/api/v1/stations`) and admin-service (`/api/v1/admin/stations`). Delivers infrastructure confidence that all services are reachable through a single entry point.

**Acceptance Scenarios**:

1. **Given** Traefik is running with the configured routing rules, **When** a request is sent to `http://localhost:8080/api/v1/stations`, **Then** the request is routed to the driver-service and returns a valid response
2. **Given** Traefik is running, **When** a request is sent to `http://localhost:8080/api/v1/admin/stations`, **Then** the request is routed to the admin-service
3. **Given** Traefik receives a request for an unknown route, **When** no matching service is configured, **Then** Traefik returns a 404 or 502 with appropriate error response
4. **Given** the driver-service is down, **When** a request is sent through Traefik, **Then** the gateway returns a 503 with a meaningful error

---

### User Story 2 - App-to-Backend Wiring (Priority: P1)

As a developer, I want the mobile and web apps to successfully communicate with backend services through the API gateway so the full discovery flow works end to end.

**Why this priority**: Without proper wiring between frontend apps and backend services, users cannot discover stations or view details — the entire user-facing value chain depends on this integration.

**Independent Test**: Can be fully tested by launching the mobile app against the Traefik gateway and verifying that station list, map markers, and detail screens load real data from the backend. Delivers E2E integration confidence.

**Acceptance Scenarios**:

1. **Given** the mobile app is configured with the Traefik gateway URL, **When** the map screen loads, **Then** station markers appear populated with data from the driver-service API
2. **Given** the mobile app receives station data from the backend, **When** a user taps a marker, **Then** the station detail screen shows charger information from the database
3. **Given** the web app is configured with the same gateway URL, **When** it loads stations, **Then** it displays the same data as the mobile app
4. **Given** apps are wired to Traefik, **When** a station is created via the admin API, **Then** it becomes discoverable through the driver app within 30 seconds

---

### User Story 3 - End-to-End Discovery Flow (Priority: P1)

As a quality engineer, I want to verify that the complete station discovery flow — from geolocation to map markers to station detail — works correctly end to end so users have a seamless experience.

**Why this priority**: The complete discovery flow is the core value proposition of MVP-1. If this flow breaks at any point, the product is unusable. Validating the full chain end to end is the highest priority integration test.

**Independent Test**: Can be fully tested by running an automated E2E test that simulates a user location, waits for nearby stations to load on the map, taps a station marker, and verifies the station detail screen displays correctly with charger information from the database. Delivers confidence that the entire value chain works.

**Acceptance Scenarios**:

1. **Given** the driver has granted geolocation permissions, **When** the map screen loads, **Then** nearby stations (within the configured radius) appear as interactive markers within 5 seconds (including geolocation + API query + render — the p95 API query target is 100ms per SC-005)
2. **Given** stations are rendered on the map, **When** the user taps a station marker, **Then** the station detail screen loads with name, address, opening hours, and charger information within 2 seconds
3. **Given** the user is on the station detail screen, **When** they view charger information, **Then** each charger displays type (CCS/CHAdeMO/AC), connector count, and operational status matching the database
4. **Given** the user navigates back to the station list, **When** they pull to refresh, **Then** the list updates with the latest data from the backend

---

### User Story 4 - Event Logging End-to-End (Priority: P2)

As a developer, I want to verify that user interactions (station views, searches, detail views) are captured as events and written to the analytics database so future analytics have reliable data.

**Why this priority**: Event instrumentation is the foundation for future analytics. If events aren't captured correctly during MVP-1, there will be no baseline data when analytics features are built in later phases. Still, this is P2 since it doesn't block the core discovery UX.

**Independent Test**: Can be fully tested by performing specific user actions (viewing a station, searching, navigating) and then querying the analytics database to verify the corresponding events were captured with correct data. Delivers observability confidence.

**Acceptance Scenarios**:

1. **Given** a user views a station detail, **When** the detail screen renders, **Then** a "station_detail_view" event is logged to the analytics database with the station ID and timestamp
2. **Given** a user performs a text search, **When** results are returned, **Then** a "search" event is logged with the search query, result count, and timestamp
3. **Given** a user enables geolocation-based nearby search, **When** results load, **Then** a "nearby_search" event is logged with the user's coordinates and result count
4. **Given** a user triggers a navigation action to a station, **When** the external navigation app opens, **Then** a "navigate_to_station" event is logged with the station ID
5. **Given** a batch of 50+ events is generated rapidly, **When** events are sent to the batch endpoint, **Then** all events are persisted to the analytics database without data loss

---

### User Story 5 - Contract & Performance Validation (Priority: P2)

As a quality engineer, I want to verify that all API responses conform to their documented contracts and meet performance thresholds so the system is reliable and fast.

**Why this priority**: API contract violations cause silent failures in frontend apps, and poor performance degrades user experience. While these don't block E2E functionality, they are critical for production readiness.

**Independent Test**: Can be fully tested by running contract tests against each API endpoint and performance benchmarks for nearby search queries (<100ms p95). Delivers API reliability and performance confidence.

**Acceptance Scenarios**:

1. **Given** the driver-service is running, **When** contract tests execute against `GET /api/v1/stations`, **Then** the response schema matches the documented contract (pagination, station fields, types)
2. **Given** the driver-service is running, **When** contract tests execute against `GET /api/v1/stations/nearby?lat=...&lng=...&radius=...`, **Then** the response matches the documented schema and returns stations sorted by distance
3. **Given** a nearby search query, **When** the query executes, **Then** the response time is under 100ms (p95) for searches with 1000+ stations in range
4. **Given** the admin-service is running, **When** contract tests execute against all CRUD endpoints, **Then** request and response schemas match the documented contracts
5. **Given** the admin-service event endpoints receive events, **When** contract tests validate the payload, **Then** the event schema matches the documented contract

---

### Edge Cases

- What happens when Traefik routing configuration is incorrect? Requests should fail fast with clear 502/503 errors, not silently route to the wrong service
- What happens when the database connection pool is exhausted during load testing? Queries should queue or fail gracefully with retry logic rather than crashing the service
- What happens when events are sent to the batch endpoint with malformed data? The batch endpoint should reject the entire batch with a validation error, not partially persist events
- How does the system handle the case where Traefik is running but both backend services are down? The gateway should return 503 with a meaningful error message
- What happens when contract tests encounter fields not in the documented schema? Extra fields should produce a warning but not fail the test; missing required fields should fail

## Clarifications

### Session 2026-06-12

- Q: Should security/auth testing be included in Phase 5? → A: Test only graceful rejection — verify unauthenticated requests return proper 401/403 without crashing. Full JWT flow testing deferred to MVP-3.
- Q: What recovery actions should error states display? → A: Retry button + back navigation — users can retry the failed action or navigate back.
- Q: What concurrency target should load tests target? → A: 50 concurrent requests — moderate load to catch routing and connection pool bottlenecks without over-engineering.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST route all `/api/v1/stations*` requests through Traefik to the driver-service
- **FR-002**: System MUST route all `/api/v1/admin*` requests through Traefik to the admin-service
- **FR-003**: Traefik MUST return 503 when the upstream service is unavailable
- **FR-004**: Mobile app MUST connect to backend services through the Traefik gateway (not directly to services)
- **FR-005**: Web app MUST use the same Traefik endpoint the mobile app uses
- **FR-006**: E2E test suite MUST cover the complete discovery flow: geolocation trigger → nearby stations load → map markers render → station detail displays with charger data
- **FR-007**: E2E test suite MUST cover dark mode: toggle works in both mobile and web apps, all screens display correctly in dark mode
- **FR-008**: E2E test suite MUST cover error handling: simulate network failure and verify recovery actions (retry button + back navigation) appear
- **FR-009**: Event logging MUST capture "station_detail_view", "search", "nearby_search", and "navigate_to_station" events
- **FR-010**: Events MUST be persisted to the analytics database (analytics_db.raw_events table) in append-only fashion
- **FR-011**: Event batch endpoint MUST accept up to 100 events per request
- **FR-012**: Contract tests MUST cover all driver-service endpoints: `GET /api/v1/stations`, `GET /api/v1/stations/{id}`, `GET /api/v1/stations/nearby`, `GET /api/v1/health`
- **FR-013**: Contract tests MUST cover admin-service endpoints: station CRUD, event ingestion
- **FR-014**: Contract tests MUST validate both success (2xx) and error (4xx, 5xx) response schemas
- **FR-015**: Performance benchmarks MUST measure nearby search query latency with 1000+ stations in range under 50 concurrent requests
- **FR-016**: Tests MUST verify that unauthenticated requests to all endpoints return proper 401/403 responses without crashing the application

### Key Entities *(include if feature involves data)*

- **Traefik Configuration**: Routing rules that map URL prefixes to backend services. Defines the gateway behavior for `/api/v1/*` paths.
- **Event Record**: An append-only entry in the analytics database capturing a user interaction (station view, search, navigation). Contains event type, timestamp, actor context, and event-specific payload.
- **Contract Test**: An automated test that validates API request/response schemas against a documented specification. Covers field types, required fields, enumerations, and error shapes.
- **E2E Test**: An automated integration test that exercises the full application stack (frontend → gateway → backend → database) to validate a complete user flow.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of API requests route correctly through Traefik from both mobile and web apps (no direct backend URL access in production configuration)
- **SC-002**: The complete discovery flow (geolocation → nearby stations → map markers → station detail → chargers) passes E2E tests on 5 consecutive runs without failure
- **SC-003**: All user interaction events are captured in the analytics database with 100% accuracy (verified by comparing triggered actions to stored event records)
- **SC-004**: 100% of documented API endpoints have passing contract tests covering both success and error responses
- **SC-005**: Nearby search queries complete in under 100ms (p95) with 1000+ stations in the search radius
- **SC-006**: Dark mode works correctly on all screens in both mobile and web apps (verified by E2E test)
- **SC-007**: All error states display recovery actions (verified by E2E test with simulated network failures)
- **SC-008**: Test suite produces a single human-readable report with pass/fail summary, timing, and failure details after each run

## Assumptions

- **Docker Environment**: All services (driver-service, admin-service, PostGIS, Traefik) run locally via Docker Compose during integration testing — the configuration from Phases 1-2 provides this
- **Test Data**: Pre-seeded Tunisia test stations with known coordinates exist in the database — seed scripts from Phase 1 (`004_seed_stations.sql`) provide this
- **Gateway Address**: Traefik runs on `localhost:8080` for local development, matching the existing Docker Compose configuration
- **Network Environment**: Mobile E2E testing uses an Android emulator or iOS simulator on the same machine as the Docker services — Expo's development server handles device-to-host networking
- **Test Tooling**: The test suite uses the project's existing test framework (Jest for unit, Detox or Maestro for E2E) — no new test infrastructure is introduced
- **CI Pipeline**: Tests run in the existing GitHub Actions CI pipeline configured in Phase 3 — parallel execution for unit, contract, and E2E tests
- **Event Verification**: Events are verified by directly querying the analytics database after triggering actions — no separate event viewer UI is built in this phase
- **Performance Baseline**: Initial performance benchmarks are taken with the development database; production-scale tuning is deferred to Phase 6 stabilization sprint
