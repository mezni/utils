# Feature Specification: Admin Service Core Operations

**Feature Branch**: `003-admin-flow`

**Created**: 2026-06-19

**Status**: Draft

**Input**: User description: "mvp-1-admin-flow.md Sprint 2"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Partner Management (Priority: P1)

A partner administrator logs in to the system and creates a new partner entity with their organization details, including name, network type, support contact information, and verification status. The partner is assigned a unique identifier following the `OPR-` prefix convention.

**Why this priority**: Partners are the foundational entities in the system - without partners, no stations or chargers can be created. This is the entry point for partner operators to integrate with the platform.

**Independent Test**: Can be fully tested by creating a partner and verifying it appears in the database with all fields populated, unique identifier generated, and audit log entry created.

**Acceptance Scenarios**:

1. **Given** an authenticated admin/partner user, **When** they submit a partner creation request with valid name, network type, and contact information, **Then** the partner is created in the database with unique `OPR-` identifier and audit log entry is recorded
2. **Given** an authenticated user, **When** they submit a partner creation request with missing required fields, **Then** the system returns a 400 Bad Request with validation error details
3. **Given** a partner with specific network type, **When** they attempt to create another partner with the same name, **Then** the system returns a 409 Conflict error
4. **Given** an authenticated partner operator, **When** they attempt to create a partner, **Then** the system returns a 403 Forbidden error (partner scope restriction)
5. **Given** an authenticated partner operator, **When** they attempt to create a partner with invalid network type, **Then** the system returns a 400 Bad Request with validation error

---

### User Story 2 - Station Management (Priority: P1)

A partner administrator logs in and creates stations for their partners. Stations are assigned unique `STA-` identifiers and must be geolocated. Each station can have multiple chargers associated with it. Stations are linked to a specific partner and may include OpenStreetMap references.

**Why this priority**: Stations are the core locations where users will charge their vehicles. Without stations, the charging network has no physical presence. This is the next logical step after partners exist.

**Independent Test**: Can be fully tested by creating a station with valid location and partner, verifying it appears in the database with all fields, and confirming Redis cache bust triggers successfully.

**Acceptance Scenarios**:

1. **Given** an authenticated admin/partner user, **When** they submit a station creation request with partner ID, location, and valid fields, **Then** the station is created with unique `STA-` identifier and audit log entry is recorded
2. **Given** a station with partner_id, **When** another partner attempts to update this station, **Then** the system returns a 403 Forbidden error (scope restriction)
3. **Given** an authenticated partner operator, **When** they submit a station creation request with invalid location (non-geographic coordinates), **Then** the system returns a 400 Bad Request with validation error
4. **Given** a station with missing required fields, **When** a station creation is attempted, **Then** the system returns a 400 Bad Request with validation error details
5. **Given** an authenticated partner, **When** they create a station and the database transaction commits successfully, **Then** the materialized view is refreshed and Redis cache is invalidated synchronously

---

### User Story 3 - Charger Management (Priority: P1)

A partner administrator logs in and creates or updates chargers for stations. Each charger has specific technical specifications (connector type, status, current type, power, voltage, amperage) and inventory information (available vs total count). Chargers are assigned unique `CHG-` identifiers and must be associated with an existing station.

**Why this priority**: Chargers are the actual charging points. Without chargers, stations are just empty locations. This is essential for the charging network functionality.

**Independent Test**: Can be fully tested by creating a charger with valid station_id and technical specifications, verifying it appears in the database with unique `CHG-` identifier, and confirming all post-commit steps (Redis bust, audit log) execute successfully.

**Acceptance Scenarios**:

1. **Given** an authenticated admin/partner user with a station, **When** they submit a charger creation request with valid station_id and technical specifications, **Then** the charger is created with unique `CHG-` identifier and audit log entry is recorded
2. **Given** a charger configuration, **When** available count exceeds total count, **Then** the system returns a 400 Bad Request with validation error
3. **Given** a charger with invalid connector type, **When** attempting to create/update, **Then** the system returns a 400 Bad Request with validation error
4. **Given** an authenticated partner, **When** they attempt to update a charger not belonging to their partner, **Then** the system returns a 403 Forbidden error (scope restriction)
5. **Given** an authenticated partner, **When** they create a charger and the database transaction commits, **Then** the system logs audit entry with BEFORE and AFTER snapshots

---

### User Story 4 - Idempotent Operations (Priority: P2)

A partner administrator submits the same POST request multiple times (e.g., due to network retry). The system detects the duplicate request using an idempotency key and returns the original response without re-executing the mutation.

**Why this priority**: Prevents duplicate partner, station, or charger creation from network retries. Critical for data integrity but not as foundational as the CRUD operations themselves.

**Independent Test**: Can be fully tested by making two identical POST requests with the same idempotency key within 24 hours and verifying the second request returns the original response with `Idempotency-Replayed: true` header.

**Acceptance Scenarios**:

1. **Given** a unique idempotency key, **When** a POST request is made with this key for the first time, **Then** the mutation executes and the idempotency key is stored with 24h TTL
2. **Given** a unique idempotency key, **When** another POST request is made with the same key within 24 hours, **Then** the system returns the original response with `Idempotency-Replayed: true` header without re-executing the mutation
3. **Given** a POST request without an idempotency key, **When** submitted, **Then** the system returns a 409 Conflict error (duplicate request detected)
4. **Given** an idempotency key, **When** the key exists in Redis but the stored response is from a different resource type, **Then** the system rejects the request (security best practice)

---

### User Story 5 - Transactional Consistency (Priority: P2)

A partner administrator performs a mutation that updates multiple entities (e.g., updates a partner's contact information). The system ensures all database operations within a single request complete successfully before returning the response, maintaining ACID properties.

**Why this priority**: Ensures data integrity when multiple entities are modified. Critical for preventing partial updates that could leave the system in an inconsistent state.

**Independent Test**: Can be fully tested by making a mutation that involves multiple database operations and verifying either all operations commit or none do, with appropriate error handling.

**Acceptance Scenarios**:

1. **Given** a mutation affecting multiple entities, **When** all database operations succeed, **Then** the transaction commits and the system returns a successful response with all post-commit steps (Redis bust, audit log) executed
2. **Given** a mutation with one failed database operation, **When** the transaction fails, **Then** the system returns an appropriate error response (500 Internal Server Error or 409 Constraint Violation) and rolls back all operations
3. **Given** a transaction with post-commit steps, **When** MV refresh or Redis bust fails, **Then** the system logs a warning and proceeds with the successful database commit (failure policy: do not roll back)

---

### User Story 6 - Audit Trail (Priority: P2)

Every mutation performed by authenticated users is logged to an audit log with comprehensive information about what changed, when it changed, and who made the change. The audit log includes before and after snapshots of the affected entities.

**Why this priority**: Provides accountability and debugging capabilities. Critical for security and compliance but not blocking for basic CRUD operations.

**Independent Test**: Can be fully tested by making a mutation and querying the audit log to verify it contains the correct actor, action, target information, and before/after snapshots.

**Acceptance Scenarios**:

1. **Given** an authenticated user making a mutation, **When** the mutation completes successfully, **Then** an audit log entry is created with actor_id, action, target_type, target_id, before_snapshot, after_snapshot, and payload
2. **Given** a CREATE operation, **When** the audit log is queried, **Then** the before_snapshot field is NULL and after_snapshot contains the full entity data
3. **Given** an UPDATE operation, **When** the audit log is queried, **Then** both before_snapshot and after_snapshot contain the full entity data with differences
4. **Given** a failed audit log insertion, **When** the primary mutation succeeds, **Then** the system logs an error but does not roll back the transaction (audit is observability, not transactional)

---

### Edge Cases

- What happens when a partner is updated but the update is rejected due to validation errors? The transaction rolls back and no audit entry is created.
- How does the system handle missing or malformed Idempotency-Key header on POST requests? The system returns 400 Bad Request with validation error.
- What happens when Redis is unreachable during cache bust? The system logs a warning and continues with the successful response, setting `X-Cache-Bust-Failed: true` header.
- What happens when the materialized view refresh exceeds the 2-5s timeout? The system logs a warning and continues with the successful response (stale data will be corrected on next write).
- How does the system handle a partner attempting to access another partner's resources? The system returns 403 Forbidden and logs the security violation.
- What happens when a mutation is attempted without the correct role (e.g., partner attempting admin-only operations)? The system returns 403 Forbidden.
- How does the system handle concurrent mutations on the same entity? The database-level uniqueness constraints prevent duplicates.
- What happens when all required database tables are unavailable? The system returns 500 Internal Server Error.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST allow authenticated admin and partner users to create, read, update, and delete partners with the `OPR-` prefix identifier format
- **FR-002**: System MUST allow authenticated admin and partner users to create, read, update, and delete stations with the `STA-` prefix identifier format
- **FR-003**: System MUST allow authenticated admin and partner users to create, read, update, and delete chargers with the `CHG-` prefix identifier format
- **FR-004**: System MUST validate that partners belong to the authenticated user (scope restriction: partner cannot mutate another partner's resources)
- **FR-005**: System MUST validate that stations belong to the authenticated user's partner (scope restriction: partner cannot mutate another partner's stations)
- **FR-006**: System MUST validate that chargers belong to the authenticated user's partner (scope restriction: partner cannot mutate another partner's chargers)
- **FR-007**: System MUST use unique entity identifiers with the `OPR-`, `STA-`, `CHG-` prefix format and regex validation
- **FR-008**: System MUST enforce referential integrity (e.g., station must reference a valid partner, charger must reference a valid station)
- **FR-009**: System MUST perform all database mutations within explicit transactions
- **FR-010**: System MUST refresh materialized views synchronously after successful database commits
- **FR-011**: System MUST invalidate Redis cache synchronously after successful database commits
- **FR-012**: System MUST log all mutations to the audit log with BEFORE and AFTER snapshots
- **FR-013**: System MUST support Idempotency-Key header on all POST endpoints for duplicate prevention
- **FR-014**: System MUST return the original response for duplicate idempotency keys within 24 hours
- **FR-015**: System MUST return 409 Conflict when a POST request is made without an Idempotency-Key header
- **FR-016**: System MUST reject Idempotency-Key headers that do not match expected UUID v4 format
- **FR-017**: System MUST extract actor information from Traefik headers (X-User-Id, X-User-Roles) and never from client body
- **FR-018**: System MUST validate JWT audience claim matches the calling client (admin routes require `aud` == `admin-dashboard`)
- **FR-019**: System MUST log validation errors with detailed information without exposing sensitive data
- **FR-020**: System MUST handle database constraint violations with 409 Conflict responses
- **FR-021**: System MUST handle entity not found with 404 Not Found responses
- **FR-022**: System MUST handle soft-deleted entities with 410 Gone responses
- **FR-023**: System MUST handle Redis failure gracefully by logging warnings and continuing (do not roll back transaction)
- **FR-024**: System MUST handle materialized view refresh timeout gracefully by logging warnings and continuing (do not roll back transaction)
- **FR-025**: System MUST handle audit log failures gracefully by logging errors and continuing (do not roll back transaction)
- **FR-026**: System MUST compute diff snapshots in the service layer, not the repository layer (repository must be audit-unaware)
- **FR-027**: System MUST use compile-time SQL queries (sqlx macros) and never raw SQL strings
- **FR-028**: System MUST return 500 Internal Server Error for unexpected internal/database errors
- **FR-029**: System MUST support geographic location data with geospatial indexing for stations
- **FR-030**: System MUST support time-stamped tracking (created_at, updated_at, deleted_at) for all entities

### Key Entities *(include if feature involves data)*

- **Partner/Operator**: Represents a partner organization in the charging network. Key attributes include unique `OPR-` identifier, name, network type (INDIVIDUAL/COMPANY), contact information (support phone/email), verification status, and audit metadata (created_by, updated_by, timestamps). Relationships: One-to-many with stations.
- **Station**: Represents a physical charging location. Key attributes include unique `STA-` identifier, partner_id reference, geolocation data (GEOGRAPHY(Point, 4326)), name, address, optional OSM reference, and audit metadata. Relationships: One-to-many with chargers, many-to-one with partner.
- **Charger**: Represents an individual charging point within a station. Key attributes include unique `CHG-` identifier, station_id reference, connector type, current type, technical specifications (power, voltage, amperage), status, inventory counts (available/total), and audit metadata. Relationships: Many-to-one with station.
- **Audit Log**: Represents a complete record of every mutation performed in the system. Key attributes include UUID primary key, actor_id (from Traefik header), action type, target type and ID, before_snapshot (JSONB), after_snapshot (JSONB), payload, and timestamp. Relationships: None (denormalized reference to entities).
- **Materialized Views**: Pre-computed aggregations for performance. Key attributes include station summaries, geospatial summaries, and review statistics. Relationships: Derived from base inventory tables.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Admin/partner users can create partners in under 1 minute with all required fields
- **SC-002**: Admin/partner users can create stations with geolocation data in under 1 minute
- **SC-002b**: Admin/partner users can create chargers with technical specifications in under 1 minute
- **SC-003**: All CRUD operations return appropriate HTTP status codes (200/201/400/403/404/409/410/500) matching the error contracts
- **SC-004**: Idempotency-key based duplicate detection prevents creation of duplicate entities within 24 hours with 100% accuracy
- **SC-005**: All mutations trigger synchronous cache bust after successful database commits with <500ms overhead
- **SC-006**: Materialized view refresh completes within 2-5 seconds for all writes, or times out gracefully after 5 seconds
- **SC-007**: Audit log captures every mutation with 100% accuracy (all entities, actors, actions, timestamps)
- **SC-008**: System supports 10 concurrent partner administrators without degradation
- **SC-009**: Scope restriction enforcement prevents cross-partner mutations with 100% accuracy
- **SC-010**: All entities use unique identifiers with the correct prefix format (OPR- / STA- / CHG-) and regex validation

## Assumptions

- The system has a deployed and healthy PostgreSQL database with the `inventory`, `users`, and `analytics_db` schemas
- The system has a deployed and healthy Redis instance for cache and idempotency key storage
- Keycloak is running and accessible by Auth Service for authentication and authorization
- Traefik is running as a reverse proxy with JWT validation middleware configured
- The admin/partner user types have the correct roles assigned in Keycloak (role:admin, role:partner)
- The system uses NanoID for entity identifier generation with the OPR- / STA- / CHG- prefix convention
- Redis cache keys use the namespace pattern `stations:tile:{z}:{x}:{y}` and `stations:near:{lat}:{lng}:{radius}`
- Materialized views are named `mv_stations_geo`, `mv_stations_summary`, and `mv_stations_reviews`
- Audit log is stored in `analytics_db.audit_log` table, not in the main `platform_db`
- Idempotency keys are UUID v4 format and have 24-hour TTL in Redis
- All mutations are performed within explicit database transactions
- Post-commit steps (MV refresh, Redis bust, audit log) execute in the service orchestration layer, not repository layer
- The system enforces database-level referential integrity and scope restrictions
- Geographic data is stored using PostGIS with SRID 4326
- All entities track creation, update, and optional deletion timestamps
- The system uses sqlx compile-time macros for all database queries
- Authentication is handled by Auth Service and JWT is validated at Traefik layer
- Audience validation occurs at the gateway layer, not within individual services
