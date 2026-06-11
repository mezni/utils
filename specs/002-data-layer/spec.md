# Feature Specification: Data Layer

**Feature Branch**: `002-data-layer`

**Created**: 2026-06-10

**Status**: Draft

**Input**: User description: "sprint 1.1"

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Developer can run spatial queries against platform_db via a shared data library (Priority: P1)

A developer building a nearby-stations endpoint needs to call `ST_DWithin` from application code. The data layer provides a reusable library with connection management, data models, and spatial query functions so each service doesn't duplicate database logic.

**Why this priority**: Every backend service depends on the shared data layer to read from platform_db. Without it, no API endpoint can function.

**Independent Test**: Run an integration test that establishes a database connection, executes a spatial query (find stations within 5 km of a given point), and returns structured station data — all without starting an HTTP server.

**Acceptance Scenarios**:

1. **Given** a running platform_db with seed data, **When** a developer calls `find_stations_nearby(lat, lng, radius_m)`, **Then** the function returns all stations within the specified radius, ordered by distance.
2. **Given** a station ID that exists in seed data, **When** a developer calls `get_station_by_id(id)`, **Then** the function returns the station with its chargers and partner details.
3. **Given** a database connection failure, **When** any query is attempted, **Then** the library returns a descriptive error without crashing the calling process.

---

### User Story 2 — Developer can run database migrations from the shared library (Priority: P1)

New schema changes need to be applied consistently across all environments. The data layer includes a migration system that tracks applied changes and applies pending ones on startup.

**Why this priority**: Schema changes are inevitable during development. A migration system prevents drift between local, CI, and production databases.

**Independent Test**: Run the migration tool against a fresh platform_db. Verify that all migration files are applied in order and that the `_migrations` tracking table records each one. Run again and confirm no migrations are re-applied.

**Acceptance Scenarios**:

1. **Given** a fresh platform_db with no migrations applied, **When** the migration command runs, **Then** all pending migration files execute in filename order and a tracking table records each one.
2. **Given** all migrations have already been applied, **When** the migration command runs again, **Then** no migrations are re-executed and the command exits successfully.
3. **Given** a migration file with a syntax error, **When** the migration command attempts to apply it, **Then** the migration fails, the error is reported, and already-applied migrations are not rolled back.

---

### User Story 3 — Developer can verify data layer correctness via integration tests (Priority: P2)

The data layer needs automated tests that run against a real (or containerized) platform_db to catch regressions in queries, type mappings, and connection handling.

**Why this priority**: Without integration tests, schema changes or query logic changes risk breaking downstream services silently.

**Independent Test**: Run the integration test suite with a single command. The suite spins up a test database container, runs migrations, loads seed data, executes all query and connection tests, and tears down the container — all without manual setup.

**Acceptance Scenarios**:

1. **Given** the integration test suite, **When** all tests pass, **Then** every query in the shared library has been exercised against a real PostGIS instance.
2. **Given** a failing query test, **When** a developer inspects the test output, **Then** the error message identifies the exact query, input parameters, and database error.
3. **Given** the test suite, **When** it completes, **Then** the test database container is automatically removed.

---

### Edge Cases

- What happens when the database server is unreachable (network partition, server down)?
- How does the spatial query handle coordinates at the antimeridian or poles (lon: ±180, lat: ±90)?
- What happens when a migration file contains destructive changes (DROP TABLE, ALTER COLUMN)?
- How does the connection pool behave when all connections are in use?
- What happens when the database schema is ahead of the application code (e.g., rolled-back deployment)?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The data layer MUST provide a connection pool that manages connections to platform_db with configurable min/max pool size.
- **FR-002**: The data layer MUST support executing parameterized SQL queries against platform_db.
- **FR-003**: The data layer MUST provide data model types that map to the `inventory.partner`, `inventory.station`, and `inventory.charger` tables.
- **FR-004**: The data layer MUST implement a spatial search function that accepts latitude, longitude, and radius in meters and returns stations within that radius using PostGIS `ST_DWithin` with results ordered by distance.
- **FR-005**: The data layer MUST implement a station detail function that returns a station with its associated chargers and partner, accepting a station ID as input.
- **FR-006**: The data layer MUST implement a migration system that applies SQL migration files in order and tracks which have been applied.
- **FR-007**: The data layer MUST expose error types that distinguish between connection errors, query errors, and not-found conditions.
- **FR-008**: The data layer MUST provide an integration test suite that runs against a real PostGIS database and exercises all query functions.
- **FR-009**: The data layer MUST support connection configuration via environment variables (host, port, user, password, database name).
- **FR-010**: The data layer MUST log connection events, query execution times (slow queries > 500ms), and connection pool status at configurable log levels.
- **FR-011**: The data layer MUST implement connection retry with exponential backoff on initial startup (max 3 retries, 1s/2s/4s intervals).

### Key Entities *(include if feature involves data)*

- **Partner**: A station owner (business or personal). Attributes: name, type, verification status. Used by Driver Service to display station ownership.
- **Station**: A physical EV charging location. Attributes: name, address, latitude, longitude. Related to Partner (many-to-one). Used for map display and nearby search.
- **Charger**: An individual charging unit at a station. Attributes: connector type, power rating (kW), status. Related to Station (many-to-one). Used for station detail view.
- **SpatialQuery**: A query type representing a geographic search. Attributes: center point (lat/lng), radius (meters), result ordering. Used to encapsulate PostGIS parameters.
- **Migration**: A versioned SQL change file with a unique identifier and description. Used to evolve the database schema consistently.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer can add a new station query function to the shared library in under 30 minutes, including writing the SQL, model mapping, and a passing integration test.
- **SC-002**: A spatial nearby query (ST_DWithin) returns results within 200ms on the seed dataset (30 chargers, 10 stations).
- **SC-003**: The integration test suite completes in under 60 seconds against a fresh containerized platform_db.
- **SC-004**: All connection errors (unreachable host, auth failure, pool exhaustion) return typed errors that the caller can handle or propagate — no panics or crashes in normal failure scenarios.
- **SC-005**: A developer can clone the repo and run the full integration test suite with a single command, without manually configuring database credentials.

## Clarifications

### Session 2026-06-10

- Q: Does Sprint 1.1 data layer need to support analytics_db for the Clickstream Service? → A: No. Sprint 1.1 covers platform_db only. The Clickstream Service (Sprint 1.3) will build its own data layer for analytics_db.

## Assumptions

- The project uses a Rust-based backend ecosystem; the shared data layer is a Rust library crate.
- The database schema from Sprint 0 (`inventory` schema with partner, station, charger tables) is already deployed in platform_db.
- The PostGIS extension is already enabled in platform_db (Sprint 0).
- Seed data from Sprint 0 (3 partners, 10 stations, 30 chargers) is present in platform_db for integration tests.
- Docker Compose from Sprint 0 is the standard way to run platform_db locally and in CI.
- The data layer is consumed by the Driver Service (Sprint 1.2). The Clickstream Service (Sprint 1.3) will build its own data layer for analytics_db.
- Integration tests run against a dedicated test database (not production).
- Connection pool configuration defaults: min 2, max 10 connections.
- Slow query threshold: 500ms for initial setup, tunable per environment.
- Migration files are plain SQL in a `migrations/` directory, named with a timestamp prefix.
