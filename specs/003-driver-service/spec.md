# Feature Specification: Driver Service

**Feature Branch**: `003-driver-service`

**Created**: 2026-06-11

**Status**: Draft

**Input**: User description: "Sprint 1.2 — Driver Service (Core Backend)"

## Clarifications

### Session 2026-06-11

- Q: Should list and nearby endpoints limit results? → A: Yes — apply a default limit of 100 results without pagination; pagination deferred to MVP-5
- Q: Should a health check endpoint be added? → A: Yes — add `GET /api/v1/health` returning HTTP 200 with DB connectivity status
- Q: What error code convention should be used? → A: HTTP status codes + JSON body with string `code` (e.g. "not_found") and `message` field

## User Scenarios & Testing

### User Story 1 - List All Stations (Priority: P1)

A mobile app user opens the discovery map and the app needs to display available charging stations. The app fetches a lightweight list of all stations to show as map markers.

**Why this priority**: Foundation for all map-based discovery — without station data, no other feature works. Every other user story depends on the API service being live and returning data.

**Independent Test**: Can be fully tested by calling the list endpoint and verifying it returns well-formed station data (id, name, coordinates) with no nested joins or heavy payloads.

**Acceptance Scenarios**:

1. **Given** the platform_db has stations seeded, **When** a GET request is made to the stations list endpoint, **Then** stations are returned with id, name, and coordinates, up to a maximum of 100 results
2. **Given** the platform_db has no stations, **When** a GET request is made to the stations list endpoint, **Then** an empty array is returned (not null or error)
3. **Given** the service is starting up, **When** the platform_db is temporarily unreachable, **Then** the service reports a healthy startup failure and does not serve incomplete data

---

### User Story 2 - Find Nearby Stations (Priority: P1)

A mobile app user pans the map or opens the app for the first time. The app needs to discover charging stations within the visible map area or near the user's location, ordered by distance.

**Why this priority**: Core discovery UX — users need to find stations near them, not see all stations globally. This is the primary driver-facing API.

**Independent Test**: Can be fully tested by calling the nearby endpoint with lat/lng/radius parameters and verifying the response contains stations sorted by proximity, with results limited to the specified radius.

**Acceptance Scenarios**:

1. **Given** stations exist within the search radius, **When** a GET request is made to the nearby endpoint with valid lat, lng, and radius_m, **Then** stations are returned ordered by distance ascending
2. **Given** no stations exist within the search radius, **When** a GET request is made to the nearby endpoint, **Then** an empty array is returned
3. **Given** invalid coordinates (lat outside -90/90, lng outside -180/180), **When** a GET request is made to the nearby endpoint, **Then** a clear validation error is returned
4. **Given** a zero or negative radius, **When** a GET request is made to the nearby endpoint, **Then** a clear validation error is returned

---

### User Story 3 - View Station Details (Priority: P2)

A mobile app user taps a station marker on the map and sees a detail view. The detail view shows the station name, address, available chargers (with connector type and power), and the operating partner.

**Why this priority**: This is the detail interaction after discovery, enabling users to decide which station to visit. P2 because the map markers with basic info (from US1/US2) already provide value without details.

**Independent Test**: Can be fully tested by calling the detail endpoint with a known station ID and verifying the response includes the station, charger list (with connector type and power), and partner info. Also test with a non-existent ID.

**Acceptance Scenarios**:

1. **Given** a station exists, **When** a GET request is made to the detail endpoint with its ID, **Then** the response includes the station, its chargers (connector type, power, status), and its partner
2. **Given** a non-existent station ID, **When** a GET request is made to the detail endpoint, **Then** a 404-style response is returned with a clear message
3. **Given** a station with no chargers, **When** a GET request is made to the detail endpoint, **Then** the response includes an empty chargers array (not null)

---

### User Story 4 - Consistent API Response Format (Priority: P2)

Frontend developers consuming the API need predictable JSON response structures across all endpoints — consistent error format, uniform pagination (if needed), and predictable field casing.

**Why this priority**: Enables frontend development in parallel without per-endpoint response handling. P2 because the API works without it, but frontend integration is significantly faster with it.

**Independent Test**: Can be tested by making requests to all three endpoints and verifying the response JSON follows a consistent schema — same envelope format, same error structure, same date formatting.

**Acceptance Scenarios**:

1. **Given** any API endpoint, **When** a successful request is made, **Then** the response follows a consistent JSON envelope with the data payload in a predictable location
2. **Given** any API endpoint, **When** a request results in an error, **Then** the error response follows a consistent format with a machine-readable code and human-readable message
3. **Given** any API endpoint, **When** a request includes invalid input, **Then** validation errors are reported with field-level detail

---

### Edge Cases

- What happens when the database schema changes (new columns)? The API must not break — responses should only include explicitly selected fields
- How does the system handle concurrent requests? Multiple requests should be processed concurrently without sequential blocking
- What happens when the database connection is lost mid-request? The request should fail with a clear error, and subsequent requests should attempt reconnection
- How does the service handle startup when the database is not yet ready? The service should retry connection with backoff and not crash-loop
- What happens to very large result sets? The nearby endpoint returns a maximum of 100 results, deferring pagination to MVP-5

## Requirements

### Functional Requirements

- **FR-001**: The service MUST expose a REST API on port 8080
- **FR-002**: All API endpoints MUST be prefixed with `/api/v1/`
- **FR-003**: The service MUST expose a `GET /api/v1/stations` endpoint returning stations with id, name, address, latitude, and longitude, with a default maximum of 100 results
- **FR-004**: The service MUST expose a `GET /api/v1/stations/nearby` endpoint accepting lat (f64), lng (f64), and radius_m (f64) query parameters, returning stations within the radius ordered by distance, with a default maximum of 100 results
- **FR-005**: The nearby endpoint MUST use PostGIS spatial queries (`ST_DWithin`) to filter stations by geographic distance
- **FR-006**: The service MUST expose a `GET /api/v1/stations/{id}` endpoint returning station details including associated chargers and partner info
- **FR-007**: All endpoints MUST return consistent JSON response envelopes (success envelope: data payload; error envelope: string `code` key (e.g. "not_found", "validation_error") + `message` field)
- **FR-008**: Validation errors MUST return field-level detail indicating which input parameter was invalid and why
- **FR-009**: The service MUST expose a `GET /api/v1/health` endpoint returning HTTP 200 with database connectivity status when the service is ready
- **FR-010**: The service MUST support configurable database connection via environment variables (host, port, user, password, db_name)
- **FR-011**: The service MUST log all incoming requests with method, path, status code, and duration

### Key Entities

- **Station**: A charging station location with id, name, address, latitude, longitude. Core entity returned by list and nearby endpoints.
- **Charger**: An individual charging unit at a station, with connector type, power rating, and status. Returned as part of station detail.
- **Partner**: The organization operating a station. Returned as part of station detail for brand/ownership context.
- **ApiResponse**: The standard response envelope wrapping all API responses — consistent structure for success and error cases.

## Success Criteria

### Measurable Outcomes

- **SC-001**: A user can discover nearby stations in under 3 seconds from opening the app (network round trip + server processing + PostGIS query)
- **SC-002**: The nearby query returns results within 200ms server-side processing time for datasets of up to 1000 stations
- **SC-003**: The API service starts up and becomes ready within 10 seconds under normal conditions (database available)
- **SC-004**: All three endpoints (`/stations`, `/stations/nearby`, `/stations/{id}`) return consistent JSON envelope structures verifiable by automated assertion
- **SC-005**: The service handles 100 concurrent requests without errors or response degradation beyond 2x baseline latency

## Assumptions

- The `borne-data` shared library crate (Sprint 1.1) will be used for database access, connection pooling, and spatial queries
- Platform_db is already running with PostGIS enabled and the inventory schema populated (Sprint 0)
- No authentication is required for MVP-1 (added in MVP-3 with Keycloak)
- The service will not expose admin/management endpoints (those belong to Sprint 1.3/2.0 admin-service)
- Response payloads will be JSON over HTTP
- Coordinates use the WGS84 standard (EPSG:4326)
- The service will be developed using the backend stack defined in the project constitution
