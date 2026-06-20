# Feature Specification: Admin Service Setup (Sprint 1.1)

**Feature Branch**: `001-admin-service-setup`

**Created**: 2026-06-19

**Status**: Draft

**Input**: User description: "SpecKit Implementation Manifest — Sprint 1.1 (BorneMap) v2"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Admin Creates a Partner Account (Priority: P1)

An administrator creates a new partner/operator in the BorneMap system to begin managing charging infrastructure. The system validates all inputs, generates a unique partner ID (OPR-*), and persists the record to the inventory database.

**Why this priority**: Partner creation is the foundational operation upon which all station and charger management depends. Without partners, there can be no stations or chargers.

**Independent Test**: An admin can submit a partner creation request and receive a confirmed partner ID (OPR-* format) with all associated details, operating independently of station or charger functionality.

**Acceptance Scenarios**:

1. **Given** a valid partner creation request with name, network type, contact info, **When** the admin submits it, **Then** the system creates the partner, returns a unique OPR-* identifier, and confirms the record persists
2. **Given** a partner creation request with missing required fields, **When** the admin submits it, **Then** the system rejects it with validation errors identifying the missing fields
3. **Given** a partner creation request, **When** submitted via the OpenAPI contract, **Then** the system validates input and creates the entity (authorization enforcement deferred until Auth Service sprint)

---

### User Story 2 - Admin Creates a Station (Priority: P2)

An administrator adds a new charging station to the system, linked to an existing partner, with precise geographic coordinates (GEOGRAPHY Point, 4326).

**Why this priority**: Stations are the core spatial entities for driver discovery. They must be connected to an existing partner.

**Independent Test**: An admin can create a station with a location and partner reference, verifying the station is stored with correct spatial data and visible in subsequent queries.

**Acceptance Scenarios**:

1. **Given** a valid station creation request with name, partner ID, geographic coordinates, **When** the admin submits it, **Then** the system creates the station with a unique STA-* identifier and validates the location format
2. **Given** a station creation request with invalid coordinates (lat out of range), **When** the admin submits it, **Then** the system rejects it with a spatial validation error
3. **Given** a station creation request referencing a non-existent partner ID, **When** the admin submits it, **Then** the system rejects it with a foreign key constraint error

---

### User Story 3 - Admin Manages Chargers at a Station (Priority: P3)

An administrator adds, updates, and removes chargers at an existing station. Each charger has a connector type, current type, power rating, and availability status.

**Why this priority**: Chargers complete the infrastructure hierarchy (partner → station → charger) and enable detailed infrastructure management.

**Independent Test**: An admin can create, view, and delete chargers linked to a station, verifying cascading deletion behavior when the station is removed.

**Acceptance Scenarios**:

1. **Given** a valid charger creation request with station ID, connector type, power rating, **When** the admin submits it, **Then** the system creates the charger with a unique CHG-* identifier
2. **Given** a charger linked to a station, **When** the station is deleted, **Then** the charger is also deleted (ON DELETE CASCADE)
3. **Given** a charger with duplicate connector type at the same station, **When** the admin attempts creation, **Then** the system rejects it with a uniqueness constraint error

---

### User Story 4 - System Health Check (Priority: P4)

Operators verify the admin service is running and responsive via the health check endpoint.

**Why this priority**: Health monitoring ensures operational visibility for infrastructure management.

**Independent Test**: A monitoring system can poll the health endpoint and receive a 200 status confirming service availability.

**Acceptance Scenarios**:

1. **Given** the admin service is running, **When** a health check request is sent, **Then** the system returns HTTP 200 with status information

---

### Edge Cases

- What happens when a partner with referenced stations is deleted? → ON DELETE SET NULL on stations.partner_id preserves station records
- How does the system handle duplicate partner slugs/names? → Uniqueness constraints at database level
- What happens when geographic coordinates are outside expected ranges? → Insert-time validation rejects invalid coordinates
- How does the system respond to malformed entity IDs? → CHECK constraint validation rejects invalid nanoid format
- What happens when the database connection fails? → Service returns 503 Service Unavailable

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST expose partner CRUD endpoints (create, read, update, delete) as defined in the OpenAPI contract
- **FR-002**: System MUST expose station CRUD endpoints (create, read, update, delete) with spatial location data as defined in the OpenAPI contract
- **FR-003**: System MUST expose charger CRUD endpoints (create, read, update, delete) as defined in the OpenAPI contract
- **FR-004**: System MUST generate unique entity IDs in the format OPR-*, STA-*, and CHG-* using nanoid(12)
- **FR-005**: System MUST validate and enforce the canonical nanoid(12) format at the database level via CHECK constraints
- **FR-006**: System MUST enforce spatial data validation for station locations (GEOGRAPHY Point, 4326 with GIST index)
- **FR-007**: System MUST enforce relational constraints: stations.partner_id → partners.id (ON DELETE SET NULL), chargers.station_id → stations.id (ON DELETE CASCADE)
- **FR-008**: System MUST provide a health check endpoint returning service status
- **FR-009**: System MUST expose an OpenAPI-defined API with endpoints for /health, /partners, /stations, /chargers
- **FR-010**: System MUST validate all inputs before persistence with descriptive error responses
- **FR-011**: System MUST reject requests with invalid or malformed entity IDs
- **FR-012**: System MUST enforce ENUM-style constraints via lookup tables (access_types, data_sources, connector_types, current_types, connector_statuses)
- **FR-013**: System MUST enforce that no raw SQL or dynamic SQL is used (SQLx compile-time queries only)
- **FR-014**: System MUST run a deterministic CI pipeline (speckit-lint) that validates architecture, schema isolation, nanoid format, OpenAPI compliance, SQLx safety, frontend boundaries, and migration integrity

### Key Entities *(include if feature involves data)*

- **Partner**: An operator or company managing charging stations. Attributes include name, network type, contact information, verification status. Linked to stations.
- **Station**: A physical charging location with geographic coordinates (GEOGRAPHY Point, 4326). Belongs to a partner. Contains one or more chargers.
- **Charger**: An individual charging unit at a station. Has connector type, current type, power rating, availability status. Cascading deletion with station.
- **Access Type**: Lookup ENUM describing station access (e.g., public, restricted, private).
- **Data Source**: Lookup ENUM describing how station data was sourced.
- **Connector Type**: Lookup ENUM describing charger connector standards (e.g., Type 2, CCS, CHAdeMO).
- **Current Type**: Lookup ENUM describing electrical current type (e.g., AC, DC).
- **Connector Status**: Lookup ENUM describing operational status (e.g., available, occupied, offline).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Admin can complete partner creation in under 3 seconds from request submission to confirmation
- **SC-002**: Station creation with spatial coordinates is validated at insert time with geographic accuracy
- **SC-003**: All entity IDs conform to the deterministic nanoid(12) format with zero collision risk
- **SC-004**: The OpenAPI specification is the single source of truth with all endpoints documented and no undocumented endpoints exist
- **SC-005**: The speckit-lint CI pipeline runs all 7 validation modules (service_topology, schema_isolation, naming, openapi_first, sqlx_safety, frontend_boundary, migration_validation) and produces deterministic pass/fail results
- **SC-006**: Zero raw SQL or dynamic SQL construction detected in the codebase via lint validation
- **SC-007**: All database integrity rules (foreign keys, CHECK constraints, unique constraints, spatial validation) are enforceable at the database level

## Assumptions

- The admin-service is the only backend service in this sprint (auth-service and driver-service are out of scope)
- Authentication and authorization are handled by a separate auth-service (not implemented in this sprint); all authorization behavior including 403 responses is deferred until the Auth Service sprint
- The dashboard frontend is a UI shell only with no persistence or domain logic
- The OpenAPI-generated client is the exclusive API access mechanism for the frontend
- PostgreSQL 16 with PostGIS extension is provisioned and available
- The speckit-lint CI pipeline is implemented as a Rust CLI and runs deterministically
- All database access uses SQLx compile-time queries only
- Entity IDs are generated server-side using a shared nanoid(12) utility
- The CI pipeline enforces all hard fail conditions as defined in the SpecKit manifest
