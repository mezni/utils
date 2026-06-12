# Feature Specification: Infrastructure & Database Setup

**Feature Branch**: `001-infra-database-setup`

**Created**: 2026-06-11

**Status**: Draft

**Input**: User description: "read from mvp 1 phase 1"

## Clarifications

### Session 2026-06-11

- Q: Database architecture — separate instances or same instance? → A: Two separate PostgreSQL 16 containers — one for platform_db (with PostGIS), one for analytics_db (plain PostgreSQL)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Reproducible Local Development Environment (Priority: P1)

As a platform developer, I want to start the entire BorneMap platform locally with a single
command so I can begin implementing features immediately without manual configuration.

**Why this priority**: All subsequent development depends on a working local environment.
Without this, no backend or frontend work can proceed.

**Independent Test**: A new developer can clone the repository, run the start command,
and access the running database within 5 minutes without any manual setup steps beyond
installing prerequisites.

**Acceptance Scenarios**:

1. **Given** a fresh clone of the repository, **When** the developer runs the platform
   start command, **Then** all services initialize and confirm readiness within 5 minutes
2. **Given** the platform is running, **When** the developer connects a database client,
   **Then** they can verify connectivity to all configured databases
3. **Given** the platform is running, **When** the developer inspects running services,
   **Then** all required processes are healthy and logging startup information

---

### User Story 2 - Database Schema Initialization (Priority: P1)

As a platform operator, I want the station inventory and analytics databases initialized
with correct schemas and indexes so the platform can store and retrieve station data
efficiently.

**Why this priority**: Station discovery (MVP-1 core feature) requires a properly
structured database with spatial indexes to deliver sub-second search results.

**Independent Test**: The database contains all required tables, constraints, and indexes
as defined in the schema documentation, verifiable by running schema inspection queries.

**Acceptance Scenarios**:

1. **Given** both database instances are running, **When** a developer connects to
   platform_db and inspects the catalog, **Then** all required schemas (inventory,
   gis) exist with correct table definitions
2. **Given** the inventory schema exists, **When** a developer queries table structures,
   **Then** station, charger, and partner tables have all required columns and constraints
3. **Given** the analytics_db instance is initialized, **When** a developer inspects the
   raw_events table, **Then** it enforces append-only rules (no updates, no deletes)
4. **Given** spatial indexes are created on platform_db, **When** a developer runs an
   explain plan on a radius query, **Then** the query plan uses the spatial index

---

### User Story 3 - Test Data with Real Coordinates (Priority: P2)

As a QA engineer, I want realistic Tunisia station data pre-loaded so I can visually
verify the map and nearby search on real coordinates without manual data entry.

**Why this priority**: The map and geospatial search features cannot be tested or
demonstrated without actual geographic coordinates to query against.

**Independent Test**: After setup, a radius search around Tunis returns known test
stations with correct coordinates, distances, and charger details.

**Acceptance Scenarios**:

1. **Given** seed data is loaded, **When** a developer queries all stations,
   **Then** at least 3 stations across multiple partners appear with complete details
2. **Given** seed data is loaded, **When** a developer queries chargers for a station,
   **Then** multiple charger types (CCS2, CHAdeMO, Type2) are represented
3. **Given** seed data is loaded, **When** a developer runs a nearby search from Tunis
   city center, **Then** results return stations ordered by distance with correct values

---

### Edge Cases

- What happens when the developer's machine does not have required prerequisites
  installed (e.g., container runtime, database client)?
- How does the system handle port conflicts when services attempt to bind to
  already-occupied ports?
- What happens if database initialization scripts are re-run on an already-initialized
  database (idempotency)?
- How does the system respond when the container registry is unavailable for pulling
  database images?
- What happens when one database instance starts successfully but the other fails
  (partial initialization)?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide a single command to start all local services
- **FR-002**: System MUST persist database data across restarts (no data loss on stop)
- **FR-003**: platform_db MUST be configured with spatial data support for geographic queries
- **FR-004**: Platform database MUST include schemas for station inventory
  (partners, stations, chargers) with entity-prefixed identifiers
- **FR-005**: Station table MUST support geographic coordinates (latitude, longitude)
  with auto-generated spatial geometry
- **FR-006**: Analytics database MUST be append-only — existing records cannot be
  modified or deleted after insertion
- **FR-007**: Database initialization scripts MUST be idempotent (safe to run multiple
  times without errors or duplicate data)
- **FR-008**: System MUST provide environment variable documentation listing all
  configuration parameters with defaults and descriptions
- **FR-009**: System MUST pre-load seed data with realistic Tunisia coordinates
  including multiple partners, stations, and charger types
- **FR-010**: All tables MUST support soft-delete for infrastructure entities
  (stations, chargers, partners) via a deletion timestamp field
- **FR-011**: System MUST document all required environment variables in a single
  reference file
- **FR-012**: System MUST verify connectivity from running services to the database
  on startup
- **FR-013**: System MUST provision two separate database instances — platform_db
  (with spatial extensions) and analytics_db (plain PostgreSQL)

### Key Entities

- **Partner**: A charging network operator or company (e.g., TotalEnergies TN) that
  owns and manages charging stations. Has a unique name and contact information.
  Stored in platform_db.
- **Station**: A physical charging location with geographic coordinates, address,
  operating hours, and status. Belongs to exactly one partner. Stored in platform_db
  with spatial geometry for geospatial queries.
- **Charger**: An individual charging connector within a station. Has a connector type
  (CCS2, CHAdeMO, Type2), power rating, price per kWh, and operational status.
  Stored in platform_db.
- **Raw Event**: An immutable record of a user interaction (station viewed, search
  performed, map panned). Stores event type, user/session identifiers, and a flexible
  JSON payload. Cannot be updated or deleted after creation. Stored in analytics_db.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer can go from repository clone to running database with seed
  data in under 5 minutes
- **SC-002**: Platform database contains all required tables (partner, station, charger)
  with correct constraints and spatial indexes
- **SC-003**: Analytics database enforces append-only rules — attempted updates and
  deletes have no effect
- **SC-004**: Seed data includes at least 2 partners, 3 stations, and 5 chargers
  across different types
- **SC-005**: Nearby search query (10km radius) on platform_db returns results in under 100ms for
  up to 1000 stations
- **SC-006**: Database survives a restart with all data intact (no data loss)
- **SC-007**: Environment variable documentation is complete — all deployment
  parameters are documented with descriptions and default values

## Assumptions

- Developers have a container runtime installed (Docker or equivalent)
- The target operating system can run containerized services
- Ports 5432 (platform_db) and 5433 (analytics_db) are available for database instances
- Tunisia geographic coordinates use the WGS 84 standard (SRID 4326)
- Seed data is for development and testing only — production data will be different
- The database user and password are configured for local development only
