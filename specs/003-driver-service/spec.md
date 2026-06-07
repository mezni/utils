# Feature Specification: Driver Service

**Feature Branch**: `003-driver-service`

**Created**: 2026-06-07

**Status**: Draft

**Input**: Sprint 1.3 per roadmap — Driver Service that serves real data from the PostgreSQL database with inventory and GIS schemas.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Health Check Endpoint (Priority: P1)

A developer (or another service) calls the driver service health endpoint to verify the service is running and connected to the database. The response confirms the service status and database connection health.

**Why this priority**: Health endpoints are essential for service discovery, monitoring, and debugging in production environments. They provide immediate feedback about service health.

**Independent Test**: Call `GET /api/v1/health` and verify it returns the expected JSON response with status: ok, service: driver-service, db: ok.

**Acceptance Scenarios**:

1. **Given** a running driver service instance, **When** a developer calls `GET /api/v1/health`, **Then** the response is `{"status":"ok","service":"driver-service","db":"ok"}`
2. **Given** the database is down, **When** the developer calls `GET /api/v1/health`, **Then** the response indicates service status as ok but db status as not ok
3. **Given** the driver service is stopped, **When** the developer calls `GET /api/v1/health`, **Then** the service returns a 503 Service Unavailable status

---

### User Story 2 — Stations Nearby Endpoint (Priority: P1)

A developer calls the driver service stations nearby endpoint to find charging stations within a geographic radius. The response returns stations from the database with their coordinates, and calculates the distance to the requested location.

**Why this priority**: The "stations nearby" feature is the core user-facing functionality for drivers discovering charging stations nearby them.

**Independent Test**: Call `GET /api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=5` and verify it returns station objects with coordinates and distance information.

**Acceptance Scenarios**:

1. **Given** the database has stations, **When** the developer calls `GET /api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=5`, **Then** the response contains station objects with id, name, coordinates, and distance
2. **Given** the requested location has no stations within the radius, **When** the developer calls the endpoint, **Then** the response returns an empty array
3. **Given** valid parameters but no database connection, **When** the developer calls the endpoint, **Then** the service returns a 500 Internal Server Error
4. **Given** invalid latitude/longitude parameters, **When** the developer calls the endpoint, **Then** the service returns a 400 Bad Request status

---

### User Story 3 — Integration Testing (Priority: P2)

A developer runs integration tests to verify the driver service API endpoints work correctly with the database and return the expected data.

**Why this priority**: Integration tests ensure the API behaves correctly with real database data and edge cases.

**Independent Test**: Run the integration tests suite and verify both endpoints pass with seeded data and fail appropriately with invalid parameters.

**Acceptance Scenarios**:

1. **Given** the database is seeded with 3 partners, 15 stations, and 24 chargers, **When** the developer runs integration tests, **Then** all tests pass
2. **Given** a location far from any stations (e.g., Antarctica), **When** the developer calls the nearby endpoint with a 100km radius, **Then** the response is an empty array
3. **Given** invalid parameters (e.g., radius = -1), **When** the developer calls the nearby endpoint, **Then** the service returns a 400 Bad Request

---

### Edge Cases

- **Database unavailability**: Health check returns db status as not ok, nearby endpoint returns 500 Internal Server Error
- **Invalid parameters**: Invalid latitude/longitude, negative radius, or radius > 100 returns 400 Bad Request
- **Empty database**: Nearby endpoint returns empty array (no stations to return)
- **Invalid authentication**: If authentication is added later, unauthenticated requests should be rejected (401 Unauthorized)
- **Concurrent requests**: Multiple simultaneous requests are handled without race conditions

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: A Rust service MUST exist at `services/driver-service` using Actix-web framework
- **FR-002**: The service MUST accept `POSTGRES_URL` environment variable for database connection
- **FR-003**: The service MUST include a `GET /api/v1/health` endpoint that returns `{"status":"ok","service":"driver-service","db":"ok"}`
- **FR-004**: The service MUST include a `GET /api/v1/stations/nearby?lat={lat}&lng={lng}&radius_km={radius}` endpoint
- **FR-005**: The nearby endpoint MUST accept numeric parameters for latitude, longitude, and radius
- **FR-006**: The nearby endpoint MUST query `inventory.station` table using spatial query `ST_DWithin(gis.station_locations.geom, point, radius*1000)` and return station objects with id, name, coordinates, and distance
- **FR-007**: The nearby endpoint MUST return stations sorted by distance from the requested location
- **FR-008**: The service MUST connect to the database using the `ev-db` shared crate from Sprint 1.1
- **FR-009**: The service MUST include integration tests for the health endpoint and nearby endpoint
- **FR-010**: The service MUST be containerized in a Dockerfile with multi-stage Rust build

### Key Entities

- **DriverService**: The Rust service application with Actix-web server, database connection, and API endpoints

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `GET /api/v1/health` returns `{"status":"ok","service":"driver-service","db":"ok"}` with HTTP 200
- **SC-002**: `GET /api/v1/stations/nearby` with valid coordinates and radius returns stations from seeded database
- **SC-003**: `GET /api/v1/stations/nearby` with no stations within radius returns empty array
- **SC-004**: `GET /api/v1/stations/nearby` with invalid parameters returns HTTP 400 Bad Request
- **SC-005**: Service starts and connects to PostgreSQL via Docker Compose with database URL
- **SC-006**: CI pipeline passes for driver-service with tests
- **SC-007**: Integration tests for health endpoint pass
- **SC-008**: Integration tests for nearby endpoint pass

## Assumptions

- PostgreSQL 16 with PostGIS 3.4 is available (from Sprint 1.1 Docker Compose)
- The driver service will run in the same Docker Compose network as the database
- The service will be deployed as a Docker container in production
- Future authentication will be implemented in Sprint 2.x

## Clarifications

### Session 2026-06-07

- Q: What should the response format be for the nearby endpoint? → A: JSON array of station objects with id, name, latitude, longitude, and distance (in km).
- Q: Should the nearby endpoint include pagination for large result sets? → A: Not for this sprint; pagination can be added in future sprints as the dataset grows.
- Q: What should happen if the database connection fails? → A: Health endpoint should indicate db status as not ok, nearby endpoint should return 500 Internal Server Error.
- Q: Should the nearby endpoint validate latitude/longitude range (e.g., -90 to 90 for lat, -180 to 180 for lng)? → A: Yes, reject requests with out-of-range values with 400 Bad Request.
