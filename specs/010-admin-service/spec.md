# Feature Specification: Admin Service

**Feature Branch**: `010-admin-service`

**Created**: 2026-06-09

**Status**: Draft

**Input**: User description: "Sprint 2.4 — Admin Service. Full CRUD for partners, stations, chargers. Availability updates. Dev X-Partner-Id header for scope testing."

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Manage Partners (Priority: P1)

An admin creates, reads, updates, and deactivates partner accounts. Partners can be verified (marked as trusted), set live (visible to drivers), activated/deactivated.

**Why this priority**: Partners are the foundation — stations and chargers cannot exist without a partner. CRUD must be complete before downstream operations work.

**Independent Test**: Can be tested by creating a partner via API, verifying the response includes all fields, then updating flags (verify, set live), and finally deactivating. Partner is persisted and readable.

**Acceptance Scenarios**:

1. **Given** valid partner data (name, type), **When** creating a partner via API, **Then** partner is created with all fields, auto-generated ID, and audit timestamps
2. **Given** an existing partner, **When** updating their `is_verified` flag from false to true, **Then** the partner is marked as verified
3. **Given** an existing partner, **When** updating their `is_live` flag, **Then** stations become visible/invisible to drivers accordingly
4. **Given** an existing partner, **When** updating `is_active` to false, **Then** the partner and all their stations/chargers are excluded from driver-facing queries
5. **Given** a non-existent partner ID, **When** requesting partner details, **Then** a not-found error is returned

---

### User Story 2 — Manage Stations (Priority: P1)

An admin creates, reads, updates, and deletes stations for a partner. Stations include location and address.

**Why this priority**: Stations are the core entity drivers interact with. Partners need to add/manage their stations.

**Independent Test**: Can be tested by creating a station for an existing partner, verifying location is stored correctly, updating fields, and deleting.

**Acceptance Scenarios**:

1. **Given** a valid partner ID and station data (name, lat, lng), **When** creating a station, **Then** station is created with correct coordinates and partner association
2. **Given** an existing station, **When** updating its name or address, **Then** changes are persisted
3. **Given** an existing station, **When** deleting it, **Then** station is removed along with its chargers and availability records
4. **Given** a station ID referencing a non-existent partner, **When** creating, **Then** a validation error is returned

---

### User Story 3 — Manage Chargers (Priority: P1)

An admin creates, reads, updates, and deletes chargers at a station. Chargers have connector type, power rating, and status.

**Why this priority**: Chargers are the atomic unit of charging availability. Without charger management, stations are empty shells.

**Independent Test**: Can be tested by creating chargers at a station with various connector types and power ratings, updating status, and deleting.

**Acceptance Scenarios**:

1. **Given** a valid station ID and charger data (connector_type, power_kw), **When** creating a charger, **Then** charger is created with status defaulting to `offline`
2. **Given** an existing charger, **When** updating its status to `maintenance`, **Then** status is updated
3. **Given** an existing charger, **When** deleting it, **Then** charger is removed
4. **Given** invalid connector_type or power_kw ≤ 0, **When** creating a charger, **Then** a validation error is returned

---

### User Story 4 — Update Station Availability (Priority: P2)

An admin (or partner) sets a station's availability status. Each update creates a new record (append-only history).

**Why this priority**: Availability determines whether drivers see stations as usable. Without this, stations always appear in the default state.

**Independent Test**: Can be tested by setting availability for a station, then querying the station detail and confirming the new status appears as current availability.

**Acceptance Scenarios**:

1. **Given** a valid station ID and a status (`available`, `partial`, `unavailable`), **When** updating availability, **Then** a new availability record is created
2. **Given** multiple availability updates for the same station, **When** querying station detail, **Then** the latest status is shown

---

### Edge Cases

- What happens when deleting a partner that still has stations?
- How does the system handle creating a station with latitude/longitude outside valid ranges?
- What happens when two concurrent updates modify the same resource?
- How does the system handle invalid connector types or power values?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST support full CRUD for partners (create, read, update, delete) with id, name, type, is_verified, is_live, is_active, and audit fields
- **FR-002**: System MUST support updating partner flags independently (verify, set live, activate/deactivate)
- **FR-003**: System MUST support full CRUD for stations (create, read, update, delete) with name, address, latitude, longitude, partner_id, and audit fields
- **FR-004**: System MUST support full CRUD for chargers (create, read, update, delete) with connector_type, power_kw, status, station_id, and audit fields
- **FR-005**: System MUST support updating station availability with status (`available`, `partial`, `unavailable`) — each update appends a new record
- **FR-006**: System MUST have a health check endpoint
- **FR-007**: System MUST accept a dev `X-Partner-Id` header for scope testing (simulates partner-scoped requests)
- **FR-008**: System MUST enforce NOT NULL and CHECK constraints matching the database schema for all write operations
- **FR-009**: System MUST return appropriate HTTP status codes: 201 for creation, 200 for success, 400 for validation errors, 404 for not found, 409 for conflicts, 500 for server errors

### Key Entities

- **Partner**: Organization or individual owning stations. Has type (business/personal), flags (verified, live, active), audit fields.
- **Station**: A charging location with name, address, coordinates, owned by a partner.
- **Charger**: An individual charging unit with connector type, power rating, status, at a station.
- **Station Availability**: Append-only history of station availability status (available, partial, unavailable).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Partners can be created, read, updated, and deleted — verified by full CRUD cycle per partner
- **SC-002**: Stations can be created, read, updated, and deleted with correct spatial coordinates — verified by full CRUD cycle per station
- **SC-003**: Chargers can be created, read, updated, and deleted with valid connector types and power — verified by full CRUD cycle per charger
- **SC-004**: Availability updates are appended — verified by creating 3 updates for a station and confirming all are stored
- **SC-005**: Invalid data (out-of-range lat/lng, invalid connector types, zero power) is rejected with descriptive error messages — verified by attempting each invalid input
- **SC-006**: The dev X-Partner-Id header is accepted and scopes operations to the specified partner — verified by integration test

## Assumptions

- Admin users are not authenticated in this sprint — the X-Partner-Id header simulates partner scope for development
- Full authentication (Keycloak + JWT) comes in MVP-3
- The database schema from Sprint 2.2 is applied and seeded
- Deleting a partner cascades to their stations, chargers, and availability records (CASCADE in DB)
- All operations are synchronous — no async workflows or event-driven patterns in this sprint
- The service runs on port 8081, alongside Driver Service on 8080
