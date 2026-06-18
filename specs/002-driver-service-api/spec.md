# Feature Specification: Driver Service & Spatial API

**Feature Branch**: `002-driver-service-api`

**Created**: 2026-06-17

**Status**: Draft

**Input**: User description: "sprint 1.2 — Rust Edge Engine Services: Actix-web driver-service, SQLx pooling, `/api/v1/nearby` endpoint, Traefik routing"

## Clarifications

### Session 2026-06-17

- Q: Observability — What minimal observability signals should the driver-service emit during the validation phase? → A: Structured JSON logging to stdout (method, path, status, duration, trace ID); no metrics endpoint.
- Q: Maximum radius — Should the API enforce an upper bound on the radius parameter? → A: Enforce a 200 km maximum; return 400 if exceeded.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Nearby station lookup (Priority: P1)

A driver opens the map and sees charging stations near their current location. The system returns stations sorted by distance with operator name, within a configurable radius.

**Why this priority**: This is the core spatial query that all front-end clients (mobile and web) depend on. Without it, the map is empty.

**Independent Test**: A curl request to `/api/v1/nearby?lat=36.8&lng=10.18&radius=10000` returns a JSON array of stations within 10 km of Tunis city center, ordered by proximity.

**Acceptance Scenarios**:

1. **Given** seed station data exists in `inventory.station`, **When** a GET request is sent to `/api/v1/nearby` with valid lat/lng/radius parameters, **Then** a JSON array of nearby stations is returned with station ID, name, coordinates, distance in meters, and operator name.
2. **Given** no stations exist within the requested radius, **When** a GET request is sent, **Then** an empty JSON array is returned (not a 404 error).
3. **Given** a station is soft-deleted (`deleted_at` is set), **When** a nearby query is performed, **Then** that station is excluded from results.
4. **Given** a station has `is_private = TRUE`, **When** a nearby query is performed, **Then** the station is included with `is_private: true`.
5. **Given** invalid parameters (lat outside ±90, lng outside ±180, radius ≤ 0), **When** a GET request is sent, **Then** a 400 Bad Request is returned with a descriptive error message.

---

### User Story 2 - Service health and readiness (Priority: P2)

The Traefik gateway and deployment infrastructure need a health endpoint to determine whether the driver service is alive and ready to accept traffic.

**Why this priority**: Health probes are required for reliable deployment orchestration. Without them, the service cannot be safely deployed or restarted.

**Independent Test**: A curl request to `/health` returns HTTP 200 with a JSON body showing database connectivity status.

**Acceptance Scenarios**:

1. **Given** the database is reachable and the connection pool is healthy, **When** a GET request is sent to `/health`, **Then** HTTP 200 is returned with `{"status": "ok"}`.
2. **Given** the database connection pool is exhausted or unreachable, **When** a GET request is sent to `/health`, **Then** HTTP 503 is returned with `{"status": "degraded"}`.

---

### User Story 3 - Gateway routing (Priority: P3)

The Traefik reverse proxy routes requests to the correct backend service based on URL path prefixes.

**Why this priority**: Without Traefik routing, external clients have no unified entry point and must connect to individual service ports directly.

**Independent Test**: A curl request to `http://traefik-host/api/v1/nearby?...` is forwarded to the driver-service running on port 3001 and returns a valid response.

**Acceptance Scenarios**:

1. **Given** Traefik is configured with a router rule for path prefix `/api/v1/`, **When** a request arrives at the gateway, **Then** it is forwarded to the driver-service on port 3001.
2. **Given** the driver-service is not running, **When** a request arrives at the gateway, **Then** Traefik returns a 502 Bad Gateway or connection refused, not a misleading 404.

### Edge Cases

- What happens when the database connection pool is exhausted during a surge of nearby requests? The service should queue or reject gracefully with 503, not crash.
- What happens when latitude/longitude values are at extreme valid boundaries (±90, ±180)? The geographic function handles these correctly.
- What happens when radius exceeds 200 km? The API returns a 400 error — the maximum allowed radius is enforced to prevent abuse.
- What happens when the Traefik router rule conflicts with another service's prefix? Rules must be ordered with the most specific prefix first.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST expose an HTTP GET endpoint at `/api/v1/nearby` that accepts `lat`, `lng`, and `radius` query parameters.
- **FR-002**: System MUST validate that `lat` is between -90 and 90, `lng` is between -180 and 180, and `radius` is between 1 and 200000 (200 km), returning 400 with `{"error": "<descriptive message>"}` for invalid input.
- **FR-003**: System MUST call the existing `gis.get_nearby_stations(lng, lat, radius_meters)` PostGIS function to retrieve nearby stations.
- **FR-004**: System MUST return results as a JSON object with a `stations` array containing objects with: `station_id`, `station_name`, `latitude`, `longitude`, `distance_meters`, `is_private`, `partner_name`.
- **FR-005**: System MUST return `{"stations": []}` (not null, not 404) when no stations match the query.
- **FR-006**: System MUST exclude soft-deleted stations (where `deleted_at IS NOT NULL`) from results.
- **FR-007**: System MUST emit structured JSON logs to stdout for every HTTP request, including method, path, response status, duration, and a unique request trace ID.
- **FR-008**: System MUST expose an HTTP GET endpoint at `/health` that returns 200 with `{"status": "ok"}` when the database connection pool is healthy, and 503 with `{"status": "degraded"}` when it is not.
- **FR-009**: System MUST use a database connection pool with configurable min/max connections, health-checked on startup and periodically.
- **FR-010**: System MUST be configured via environment variables for database URL, bind address, connection pool size, and CORS origins.
- **FR-011**: Traefik MUST be configured with a router rule for path prefix `/api/v1/` forwarding to the driver-service on port 3001.
- **FR-012**: Traefik MUST be configured with a health check pointing at the driver-service's `/health` endpoint.

### Key Entities *(include if feature involves data)*

- **Station**: A physical charging location with coordinates, name, operator, privacy flag, and soft-delete support. Read from `inventory.station` via the existing `gis.get_nearby_stations()` PostGIS function.
- **Database Connection Pool**: A managed pool of database connections maintained by the driver-service, with health checking and configurable sizing.
- **Traefik Router**: A routing rule in the Traefik configuration that maps URL path prefixes to backend services.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A driver can retrieve nearby stations sorted by distance in under 500 ms (cold start) and under 100 ms (warm, with connection pool established) for a typical query with 10 km radius in an area with 50+ stations.
- **SC-002**: The `/health` endpoint responds in under 50 ms regardless of database load.
- **SC-003**: Invalid input parameters (lat/lng/radius out of bounds) are rejected with a 400 status and descriptive error message in under 50 ms.
- **SC-004**: The service starts, establishes a database connection pool, and begins serving requests within 5 seconds of process launch.
- **SC-005**: Traefik routes `/api/v1/*` requests to the driver-service without observable latency overhead (under 5 ms added).

## Assumptions

- The `gis.get_nearby_stations` PostGIS function already exists and is tested (Sprint 1.1 deliverable).
- The database is already running with seed data and the correct schema (Sprint 1.1 deliverable).
- The driver-service will run in the same Docker Compose network as the database.
- Traefik configuration in Sprint 1.2 covers routing rules only — TLS termination is deferred to MVP-6.
- Connection pool size defaults: min 1, max 10 connections, adjustable via environment variable.
- CORS is configured to allow all origins during validation phase, tightened in MVP-6.
- The Actix-web framework is selected per the constitution Tech Stack constraint.
