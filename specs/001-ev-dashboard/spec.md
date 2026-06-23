# Feature Specification: EV Dashboard Platform Kernel

**Feature Branch**: `001-ev-dashboard`

**Created**: 2026-06-23

**Status**: Draft

**Input**: User description: "from docs/epics/E001-dashboard-core"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - View Dashboard Overview (Priority: P1)

As a system administrator, I want to see a dashboard overview showing total counts of all partners, stations, and chargers in the system. This provides a quick summary of system health and scale at a glance.

**Why this priority**: This is the primary entry point and gives immediate visibility into system state. It can be tested by accessing the dashboard and verifying that the counts match database records.

**Independent Test**: Can be fully tested by accessing the dashboard endpoint and verifying that KPI metrics (Total Partners, Total Stations, Total Chargers) accurately reflect database counts of active records (deleted_at IS NULL). The feature delivers immediate value as the landing page.

**Acceptance Scenarios**:

1. **Given** the system has 10 active partners, 50 active stations, and 200 active chargers, **When** an administrator visits the dashboard, **Then** the system displays 10 Partners, 50 Stations, and 200 Chargers

2. **Given** the system has 0 active partners but some partners are soft-deleted, **When** an administrator visits the dashboard, **Then** the system displays 0 Partners (only active records)

3. **Given** partners and stations are deleted, **When** an administrator refreshes the dashboard, **Then** all counts update to reflect current database state (only active records counted)

---

### User Story 2 - Manage Partners (Priority: P2)

As a system administrator, I want to create, view, and manage partner organizations (EV network operators) in the system. This enables tracking of which organizations are operating charging infrastructure.

**Why this priority**: Partners are the top-level entity that groups stations. Understanding partner information is essential for reporting and analytics, but doesn't block viewing system totals.

**Independent Test**: Can be fully tested by creating a partner via the API, verifying it appears in the database with the correct deterministic external ID format, and ensuring the partner count updates accordingly.

**Acceptance Scenarios**:

1. **Given** the system has no active partners, **When** an administrator creates a new partner named "Example EV Network", **Then** the system generates a deterministic PRT-<12-char> ID, stores the partner with the provided name and status, and returns the created partner data

2. **Given** a partner exists with ID PRT-abc123456789, **When** an administrator retrieves the partner details, **Then** the system returns the partner data including its name, status, and ID without exposing any internal database fields

3. **Given** multiple active partners exist, **When** an administrator retrieves all partners, **Then** the system returns a list of all active partners sorted by creation date, each with only their ID, name, status, and is_valid flag

4. **Given** a partner exists and is active, **When** an administrator attempts to create a duplicate partner name, **Then** the system returns a validation error indicating the name must be unique

5. **Given** a partner exists and is active, **When** an administrator requests a hard delete, **Then** the system deletes the partner AND automatically removes all associated stations (CASCADE delete)

6. **Given** a partner exists and is active, **When** an administrator requests a soft delete, **Then** the system sets deleted_at timestamp, stations are NOT automatically deleted, partners count decreases but stations remain

7. **Given** a partner is soft-deleted, **When** an administrator retrieves all partners, **Then** the system does NOT return the soft-deleted partner (deleted_at IS NOT NULL filters it out)

8. **Given** a partner is soft-deleted, **When** an administrator requests to undelete, **Then** the system removes the deleted_at timestamp and makes the partner active again

---

### User Story 3 - Manage Stations (Priority: P3)

As a system administrator, I want to create, view, and manage charging stations, each linked to a specific partner organization. This enables tracking of physical charging locations.

**Why this priority**: Stations are the next level of organization after partners. While important, managing partners must be complete first as stations reference partners.

**Independent Test**: Can be fully tested by creating a station, specifying an existing active partner ID, verifying the station is created with the correct deterministic STA ID format, and ensuring the partner's station count updates accordingly.

**Acceptance Scenarios**:

1. **Given** an active partner with ID PRT-abc123456789 exists, **When** an administrator creates a station named "Downtown Station" for this partner, **Then** the system generates a deterministic STA-<12-char> ID, stores the station with the partner's ID as a foreign key, and returns the created station data

2. **Given** a station exists with ID STA-xyz987654321 and is active, **When** an administrator retrieves the station details, **Then** the system returns the station data including its name, location, status, partner reference, and ID without exposing any internal database fields

3. **Given** multiple stations exist for different partners, **When** an administrator retrieves all stations, **Then** the system returns a list of all active stations sorted by creation date, each with only their ID, name, location, status, and partner reference

4. **Given** a partner exists and is active, **When** an administrator attempts to create a station with a non-existent partner ID, **Then** the system returns a validation error indicating the parent partner does not exist

5. **Given** a partner exists and is active, **When** an administrator requests a hard delete, **Then** the system deletes the partner AND automatically removes all associated stations (CASCADE delete)

6. **Given** a station exists and is active, **When** an administrator requests a soft delete, **Then** the system sets deleted_at timestamp, chargers are NOT automatically deleted, stations count decreases but chargers remain

7. **Given** a station is soft-deleted, **When** an administrator retrieves all stations, **Then** the system does NOT return the soft-deleted station (deleted_at IS NOT NULL filters it out)

8. **Given** a station is soft-deleted, **When** an administrator requests to undelete, **Then** the system removes the deleted_at timestamp and makes the station active again

---

### User Story 4 - Manage Chargers (Priority: P4)

As a system administrator, I want to create, view, and manage charging units within stations. This enables tracking of individual charging hardware and their operational status.

**Why this priority**: Chargers are the lowest level of granularity. While valuable for operational monitoring, the core infrastructure (Partners, Stations, Dashboard) can function without chargers.

**Independent Test**: Can be fully tested by creating a charger, specifying an existing active station ID, verifying the charger is created with the correct deterministic CHR ID format, and checking station has correct charger count.

**Acceptance Scenarios**:

1. **Given** an active station with ID STA-xyz987654321 exists, **When** an administrator creates a charger with status "active" and 50 kW power rating, **Then** the system generates a deterministic CHR-<12-char> ID, stores the charger, and returns the created charger data

2. **Given** a charger exists with ID CHR-fee987654321 and is active, **When** an administrator retrieves the charger details, **Then** the system returns the charger data including its station reference, status, power rating, and ID without exposing any internal database fields

3. **Given** a station exists with multiple active chargers, **When** an administrator retrieves the station, **Then** the system includes all active chargers in the response

4. **Given** a station exists and is active, **When** an administrator attempts to create a charger with a non-existent station ID, **Then** the system returns a validation error indicating the parent station does not exist

5. **Given** a station exists and is active, **When** an administrator updates the charger status to "inactive", **Then** the system updates the status and returns the modified charger

6. **Given** a station exists and is active, **When** an administrator requests a hard delete, **Then** the system deletes the station AND automatically removes all associated chargers (CASCADE delete)

7. **Given** a charger exists and is active, **When** an administrator requests a soft delete, **Then** the system sets deleted_at timestamp (no cascade, no other effect)

8. **Given** a charger is soft-deleted, **When** an administrator retrieves all chargers, **Then** the system does NOT return the soft-deleted charger (deleted_at IS NOT NULL filters it out)

9. **Given** a charger is soft-deleted, **When** an administrator requests to undelete, **Then** the system removes the deleted_at timestamp and makes the charger active again

---

### Edge Cases

- What happens when an administrator attempts to create a station with a non-existent active partner ID?
  - The system returns a validation error indicating the parent partner does not exist

- How does the system handle retrieving a partner or station that was soft-deleted?
  - The system returns a NOT_FOUND error for soft-deleted entities (deleted_at IS NOT NULL)

- What happens when the dashboard KPIs are calculated from a partially populated database?
  - The KPIs accurately reflect the current database state, counting only active records (deleted_at IS NULL)

- What happens when an administrator attempts to create a charger with a non-existent active station ID?
  - The system returns a validation error indicating the parent station does not exist

- What happens when an administrator hard-deletes a partner with many stations and chargers?
  - The system uses database CASCADE to automatically delete all associated stations and chargers in a single transaction

- What happens when an administrator soft-deletes a partner?
  - The system sets deleted_at timestamp; stations and chargers remain active (no cascade)

- What happens when IDs must be deterministic?
  - System generates IDs from string seeds using hash-based deterministic nanoid (consistent across instances)

## Requirements *(mandatory)*

### Functional Requirements

**Partner Management**:
- **FR-001**: System MUST provide a dashboard endpoint that returns total counts of partners, stations, and chargers (only active records)
- **FR-002**: System MUST allow administrators to create partners with a name, status, and is_valid flag and automatically generate a deterministic external ID in PRT-<12-char> format
- **FR-003**: System MUST store partners in the database with their external ID, name, status, is_valid, and audit fields
- **FR-004**: System MUST allow administrators to retrieve individual partners by their external ID
- **FR-005**: System MUST allow administrators to retrieve all active partners with pagination support
- **FR-006**: System MUST ensure partner names are unique across the system
- **FR-007**: System MUST allow administrators to perform hard delete on partners (CASCADE to stations)
- **FR-008**: System MUST allow administrators to perform soft delete on partners (no cascade)
- **FR-009**: System MUST allow administrators to undelete soft-deleted partners

**Station Management**:
- **FR-010**: System MUST allow administrators to create stations linked to specific active partners via the partner's external ID
- **FR-011**: System MUST automatically generate a deterministic external ID in STA-<12-char> format for each station
- **FR-012**: System MUST store stations with their external ID, name, location, status, partner reference, and audit fields
- **FR-013**: System MUST allow administrators to retrieve individual stations by their external ID
- **FR-014**: System MUST allow administrators to retrieve all active stations with pagination support
- **FR-015**: System MUST validate that a station's referenced partner ID exists and is active before creating the station
- **FR-016**: System MUST allow administrators to perform hard delete on stations (CASCADE to chargers)
- **FR-017**: System MUST allow administrators to perform soft delete on stations (no cascade)
- **FR-018**: System MUST allow administrators to undelete soft-deleted stations

**Charger Management**:
- **FR-019**: System MUST allow administrators to create chargers within stations with status and power rating
- **FR-020**: System MUST automatically generate a deterministic external ID in CHR-<12-char> format for each charger
- **FR-021**: System MUST store chargers with their external ID, station reference, status, power rating, and audit fields
- **FR-022**: System MUST allow administrators to retrieve individual chargers by their external ID
- **FR-023**: System MUST allow administrators to retrieve all active chargers with pagination support
- **FR-024**: System MUST allow administrators to update charger status
- **FR-025**: System MUST allow administrators to perform hard delete on chargers (no cascade)
- **FR-026**: System MUST allow administrators to perform soft delete on chargers (no cascade)
- **FR-027**: System MUST allow administrators to undelete soft-deleted chargers

**Delete Strategy (Explicit)**:
- **FR-028**: System MUST support both hard delete (CASCADE) and soft delete (no cascade)
- **FR-029**: System MUST enforce hard delete cascade at database level (ON DELETE CASCADE)
- **FR-030**: System MUST enforce soft delete behavior at application level (WHERE deleted_at IS NULL filter)
- **FR-031**: System MUST NOT automatically cascade soft delete operations (stations/chargers remain active)

**Identity System (Explicit)**:
- **FR-032**: System MUST use deterministic external IDs with format: Entity-{12 alphanumeric characters} where Entity is PRT, STA, or CHR
- **FR-033**: System MUST use Base62 character set (a-z, A-Z, 0-9) for ID generation
- **FR-034**: System MUST use hash-based deterministic nanoid generation (NOT random nanoid)
- **FR-035**: System MUST generate IDs in infrastructure layer (not domain layer)
- **FR-036**: System MUST ensure IDs are consistent across instances (deterministic generation)

**API Contract**:
- **FR-037**: System MUST use standardized API response format:
  - Success: `{success: true, data: {...}, error: null}`
  - Error: `{success: false, data: null, error: {code: "ERROR_CODE", message: "...", details: {...}}}`
- **FR-038**: System MUST version all APIs under `/api/v1`
- **FR-039**: System MUST NOT expose internal database identifiers (no UUIDs, no surrogate keys)
- **FR-040**: System MUST NOT expose raw framework responses (no Actix responses, no HTTP status codes as API contract)
- **FR-041**: System MUST include request ID in all responses for tracing

**Clean Architecture (Explicit)**:
- **FR-042**: System MUST enforce strict layering: presentation → application → domain → infrastructure
- **FR-043**: System MUST ensure domain layer contains NO database or HTTP logic
- **FR-044**: System MUST ensure infrastructure layer handles ALL database operations
- **FR-045**: System MUST ensure application layer orchestrates use-cases but does not access database directly
- **FR-046**: System MUST ensure presentation layer contains ONLY HTTP handlers and response mapping

**Repository Interfaces (Explicit)**:
- **FR-047**: System MUST define repository traits: PartnerRepository, StationRepository, ChargerRepository
- **FR-048**: System MUST enforce repository contracts across all layers
- **FR-049**: System MUST use dependency injection for repository implementations

**Service Layer (Explicit)**:
- **FR-050**: System MUST define application layer services: PartnerService, StationService, ChargerService, DashboardService
- **FR-051**: System MUST ensure service layer contains business logic (use-cases)
- **FR-052**: System MUST ensure service layer orchestrates repositories but does not access database directly

**Database (Explicit)**:
- **FR-053**: System MUST use PostgreSQL with the 'ev' schema namespace
- **FR-054**: System MUST use external IDs as PRIMARY KEY (no surrogate keys)
- **FR-055**: System MUST create indexes on foreign keys (partner_id, station_id)
- **FR-056**: System MUST implement cascading deletes on hard deletes (ON DELETE CASCADE)
- **FR-057**: System MUST use forward-only migrations with timestamp ordering
- **FR-058**: System MUST filter all queries by `deleted_at IS NULL` for active records

**API Endpoints (Explicit)**:
- **FR-059**: System MUST provide DELETE endpoints for hard delete operations
- **FR-060**: System MUST provide soft_delete endpoints (or update endpoint for status management)
- **FR-061**: System MUST provide undelete endpoints

**Pagination**:
- **FR-062**: System MUST provide pagination support for list endpoints (page, limit parameters)
- **FR-063**: System MUST limit default page size to 50 items
- **FR-064**: System MUST enforce maximum page size of 100 items

**Charger Model (Explicit)**:
- **FR-065**: System MUST define charger status enum: active, inactive, maintenance, faulted
- **FR-066**: System MUST define power rating unit as kilowatts (kW)
- **FR-067**: System MUST validate power rating range (1-1000 kW)

**Audit Fields**:
- **FR-068**: System MUST include created_by and updated_by fields (FK to admins.id)
- **FR-069**: System MUST include created_at and updated_at audit timestamps
- **FR-070**: System MUST automatically set created_at on creation, updated_at on updates
- **FR-071**: System MUST enforce that admins table exists (assumed in separate system module)

**Status Field**:
- **FR-072**: System MUST include status column on all entities (ACTIVE, INACTIVE, MAINTENANCE, DISABLED)
- **FR-073**: System MUST validate status enum on all entities
- **FR-074**: System MUST use consistent status values across all entities

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Administrators can view dashboard KPIs within 200ms for under 1000 records
- **SC-002**: Administrators can create a partner in under 30 seconds including API round-trip
- **SC-003**: System can handle at least 1000 partners and 10,000 stations without performance degradation
- **SC-004**: 95% of API requests complete within 500 milliseconds for under 1000 records
- **SC-005**: All API responses follow the standardized success/error format without implementation details
- **SC-006**: Partners, stations, and chargers use ONLY external IDs in API responses
- **SC-007**: Hard delete cascade works correctly: deleting a partner automatically removes all associated stations and chargers
- **SC-008**: Soft delete works correctly: soft-deleted entities are filtered out by default queries but remain in database

## Assumptions

- The system is a web-based dashboard accessed by administrators via a browser (desktop and tablet supported)
- Administrators are internal users managing the EV infrastructure system
- All data is stored in PostgreSQL with the 'ev' schema namespace
- Partners are the top-level organizational unit in the system hierarchy
- Stations are locations that belong to exactly one partner
- Chargers are physical units located within stations
- All external IDs are generated deterministically in the infrastructure layer
- Pagination is required for listing endpoints to support scalability
- No authentication or authorization is implemented in this epic (explicitly out of scope)
- No billing or payment processing is included (explicitly out of scope)
- No IoT or telemetry integration is included (explicitly out of scope)
- No event streaming is included (explicitly out of scope)
- No microservices architecture is used (explicitly out of scope)
- No mobile applications are included (explicitly out of scope)
- Both hard delete and soft delete are supported with different behaviors
- Admins table is assumed to exist in a separate system module (no auth system)
- Status enum values are consistent across all entities (ACTIVE, INACTIVE, MAINTENANCE, DISABLED)
