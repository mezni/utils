# Feature Specification: MVP-1 Sprint 1 — Backend Core API (driver-service)

**Feature Branch**: `002-backend-core-api`

**Created**: 2026-06-13

**Status**: Draft

**Input**: Sprint 1 from MVP-1 Discovery Core - Expose geospatial API through Rust-based driver-service with three endpoints: list stations, get station by ID, and nearby geospatial search.

---

## User Scenarios & Testing

### User Story 1 - Backend Developer Implements Station List Endpoint (Priority: P1)

A backend developer needs to implement a `GET /api/v1/stations` endpoint that returns all EV charging stations from the database, ensuring the API is accessible and returns real PostGIS data.

**Why this priority**: P1 because this is the simplest endpoint and validates the full stack (handler → service → repository → DB) is wired correctly. It's the foundation for all other endpoints.

**Independent Test**: Can be fully tested by starting the service and hitting `GET /api/v1/stations` — must return a JSON array of stations with correct fields (id, name, status, latitude, longitude, distance).

**Acceptance Scenarios**:

1. **Given** the driver-service is running and connected to platform_db, **When** a client sends `GET /api/v1/stations`, **Then** the API returns a JSON array of all stations in the database
2. **Given** the database has 4 seeded stations, **When** the list endpoint is called, **Then** the response contains exactly 4 station objects
3. **Given** a station record exists, **When** the list endpoint returns it, **Then** each station object includes id, name, status, latitude, longitude, and distance fields
4. **Given** the database connection is unavailable, **When** the list endpoint is called, **Then** the API returns a 503 error with a descriptive message

---

### User Story 2 - Backend Developer Implements Station Detail Endpoint (Priority: P1)

A backend developer needs to implement a `GET /api/v1/stations/{id}` endpoint that returns a single station by its unique identifier, enabling the frontend to show station details on marker click.

**Why this priority**: P1 because the station detail view is a core user interaction — users tap a marker and expect to see station information.

**Independent Test**: Can be fully tested by calling `GET /api/v1/stations/STA-00001` — must return the matching station object with all fields, or a 404 if not found.

**Acceptance Scenarios**:

1. **Given** a station with ID `STA-00001` exists, **When** a client sends `GET /api/v1/stations/STA-00001`, **Then** the API returns the full station object including id, name, status, latitude, longitude, and distance
2. **Given** a non-existent station ID, **When** a client sends `GET /api/v1/stations/NONEXISTENT`, **Then** the API returns a 404 error with a descriptive message
3. **Given** an invalid ID format, **When** a client sends `GET /api/v1/stations/`, **Then** the API returns a 400 error with a descriptive message

---

### User Story 3 - Backend Developer Implements Nearby Search Endpoint (Priority: P1)

A backend developer needs to implement a `GET /api/v1/stations/nearby?lat={lat}&lng={lng}&radius={meters}` endpoint that performs PostGIS geospatial queries to find stations within a given radius of a point, ordered by proximity. This is the core geospatial feature of MVP-1.

**Why this priority**: P1 because this is the core value proposition of MVP-1 — users discovering nearby EV charging stations on a map. Without this, the product has no geospatial capability.

**Independent Test**: Can be fully tested by calling `GET /api/v1/stations/nearby?lat=36.8&lng=10.2&radius=50000` — must return stations within 50km of Tunis center ordered by distance ascending, with latency under 200ms.

**Acceptance Scenarios**:

1. **Given** stations exist in the database, **When** a client sends nearby query with valid coordinates, **Then** the API returns stations within the specified radius ordered by distance (closest first)
2. **Given** stations exist within the search radius, **When** the nearby endpoint is called, **Then** each returned station includes a `distance` field in meters from the query point
3. **Given** no stations exist within the search radius, **When** the nearby endpoint is called, **Then** the API returns an empty array with a 200 status code
4. **Given** a nearby query is performed, **When** the query completes, **Then** the response time is under 200ms on a local development machine
5. **Given** the query is missing required parameters, **When** a client calls the endpoint without `lat`, **Then** the API returns a 400 error with a descriptive message
6. **Given** invalid parameter values, **When** a client sends `lat` outside valid range (-90 to 90), **Then** the API returns a 400 error with a descriptive message

---

### User Story 4 - Backend Developer Wires Database Connection (Priority: P1)

A backend developer needs to configure the database connection pool to platform_db, enabling the service to execute SQL queries against PostGIS and return real station data.

**Why this priority**: P1 because this blocks all three endpoint stories — without database connectivity, no endpoint can return real data. It must be completed first.

**Independent Test**: Can be fully tested by starting the service and verifying it connects to platform_db, then calling any endpoint and confirming data is returned from the database (not mocked/hardcoded).

**Acceptance Scenarios**:

1. **Given** platform_db is running, **When** the driver-service starts, **Then** it successfully connects to the database and logs a connection confirmation
2. **Given** the service is connected, **When** a SQL query is executed against `inventory.station`, **Then** it returns real data from the PostGIS database
3. **Given** platform_db is unavailable, **When** the service starts, **Then** it retries the connection and logs an appropriate error without crashing
4. **Given** the service is running, **When** a client sends `GET /api/v1/health`, **Then** it returns HTTP 200 with `{"status": "ok"}` and the body indicates database connectivity (e.g., `"database": "connected"`)

---

### Edge Cases

- What happens if the database is unreachable at startup? → Service retries connection with configurable retry count and exponential backoff (default: 3 retries, 1s base). If all retries are exhausted, the service exits with a non-zero error code and lets Docker restart policy handle recovery.
- What if a station has no name in the database? → API returns an empty string for the name field
- What if extremely large radius (e.g., 1000km) is requested? → Query still executes efficiently thanks to GIST index
- What if the database contains stations with NULL coordinates? → Service filters out stations with invalid or NULL coordinates
- What if multiple clients query simultaneously? → Connection pool handles concurrent requests up to configurable pool size

---

## Requirements

### Functional Requirements

- **FR-001**: System MUST expose `GET /api/v1/stations` endpoint returning all stations from `inventory.station` as a JSON array
- **FR-002**: System MUST expose `GET /api/v1/stations/{id}` endpoint returning a single station by ID, returning 404 if not found
- **FR-003**: System MUST expose `GET /api/v1/stations/nearby?lat={lat}&lng={lng}&radius={meters}` endpoint performing PostGIS proximity search
- **FR-004**: Nearby search MUST use `ST_DWithin` for radius filtering and return stations ordered by distance ascending
- **FR-005**: Nearby search MUST filter results to only include stations with `status = 'active'`
- **FR-006**: All endpoints MUST return stations with fields: id, name, status, latitude, longitude, and distance (distance=0 for non-nearby queries)
- **FR-007**: System MUST validate query parameters: lat (-90 to 90), lng (-180 to 180), radius (> 0, default 5000m if not specified)
- **FR-008**: System MUST connect to platform_db using a connection pool with configurable pool size (default 10-20 connections) and retry logic. The service MUST use an async Rust runtime (tokio/axum) to efficiently handle 5-50 concurrent requests.
- **FR-009**: System MUST return appropriate HTTP status codes (200 success, 400 bad request, 404 not found, 503 service unavailable)
- **FR-010**: All endpoints MUST complete in under 200ms on local development hardware
- **FR-011**: System MUST log startup connection status and query errors at appropriate log levels
- **FR-012**: System MUST expose `GET /api/v1/health` endpoint returning HTTP 200 with body `{"status": "ok"}` and current database connectivity status. Used for Docker health checks and deployment orchestration.

### Key Entities

- **Station**: Represents a real EV charging station in platform_db.inventory.station with id, name, status, latitude, longitude, and GEOGRAPHY location. Read-only in Sprint 1 (data sourced from OSM import in Sprint 0).
- **API Response**: JSON response object containing station fields (id, name, status, latitude, longitude, distance) returned as a single object (detail) or array of objects (list/nearby).

---

## Success Criteria

### Measurable Outcomes

- **SC-001**: All three API endpoints (`/stations`, `/stations/{id}`, `/stations/nearby`) return correct data from the database and can be tested independently
- **SC-002**: Nearby search queries complete in under 200ms on local development hardware
- **SC-003**: API returns proper HTTP error codes (400, 404, 503) with descriptive messages for invalid inputs and failure conditions
- **SC-004**: The service connects to the real platform_db database (not mocked data) and returns PostGIS query results accurately
- **SC-005**: All endpoints are accessible at the `/api/v1/` versioned path prefix (including `/api/v1/health`)
- **SC-006**: Docker health check (`GET /api/v1/health`) returns 200 and confirms database connectivity when the service is operational

---

## Assumptions

- **Database is Running**: Sprint 0 infrastructure (platform_db with PostGIS) is already deployed and accessible
- **Station Data Exists**: Sprint 0 OSM import has seeded station data into `inventory.station`
- **No Authentication**: No authentication or authorization is required for these endpoints (deferred to MVP-3)
- **Local Development**: Development and testing occurs on local machines with Docker
- **Rust Toolchain**: Developer machine has Rust toolchain installed (rustc, cargo) for building the driver-service
- **Single Service**: No service discovery, load balancing, or container orchestration needed for MVP-1
- **Async Runtime**: The service uses tokio (async Rust) as the async runtime with axum as the HTTP framework. No synchronous web framework will be considered.
- **Docker Health Check**: Sprint 0's docker-compose.yml includes a health check stanza that will use `GET /api/v1/health`; no additional orchestration tooling required
- **Read-Only API**: Sprint 1 is read-only; no create, update, or delete operations
- **Network**: Service can reach platform_db on localhost:5432 via Docker networking
