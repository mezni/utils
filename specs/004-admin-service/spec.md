# Feature Specification: Admin Service — Station & Partner Management

**Feature Branch**: `004-admin-service`

**Created**: 2026-06-11

**Status**: Draft

**Input**: User description: "sprint 1.3"

## User Scenarios & Testing

### User Story 1 — Partner Management (Priority: P1)

An admin user creates, lists, and approves EV charging partners. Partners are organizations that own or operate charging stations. They must be registered and approved before they can manage stations.

**Why this priority**: Partners are the foundational entity — stations and chargers belong to partners. Without partner management, no station administration is possible.

**Independent Test**: Can be fully tested by calling create/list/approve partner endpoints and verifying partner records with correct approval state transitions.

**Acceptance Scenarios**:

1. **Given** a new partner submission with name and type, **When** an admin submits the create partner form, **Then** the partner is created with "pending" verification status and a unique identifier is returned
2. **Given** an existing partner in "pending" status, **When** an admin approves the partner, **Then** the partner status changes to "verified" and the partner can now manage stations
3. **Given** the system has multiple partners, **When** a staff user lists partners, **Then** all partners are returned with their current verification status
4. **Given** a create partner request with a missing name, **When** submitted, **Then** a validation error is returned indicating the required field

---

### User Story 2 — Station Management (Priority: P1)

An authorized partner user creates and updates charging stations under their partner account. Each station has a location, address, and belongs to a single partner.

**Why this priority**: Station management is the core operational feature — partners need to add their stations to the platform so drivers can discover them.

**Independent Test**: Can be fully tested by creating a partner, then creating stations under that partner, verifying station records are correctly associated.

**Acceptance Scenarios**:

1. **Given** an approved partner exists, **When** a partner user creates a station with name, location coordinates, and address, **Then** the station is created and associated with that partner
2. **Given** an existing station, **When** a partner user updates the station name or address, **Then** the station information is updated
3. **Given** an existing station with associated chargers, **When** a partner user deletes the station, **Then** the station and all its chargers are removed
4. **Given** a station create request with latitude outside valid range, **When** submitted, **Then** a validation error is returned

---

### User Story 3 — Charger Management (Priority: P2)

An authorized partner user adds and removes individual chargers at a station. Each charger has a connector type, power rating, and operational status.

**Why this priority**: Chargers are the atomic unit of charging infrastructure — drivers need precise charger-level information to plan their charging sessions.

**Independent Test**: Can be fully tested by creating a station, then adding multiple chargers with different connector types and power ratings, verifying charger records are correctly associated.

**Acceptance Scenarios**:

1. **Given** an existing station, **When** a partner user adds a charger with connector type and power rating, **Then** the charger is created and associated with the station
2. **Given** an existing charger at a station, **When** a partner user updates the charger status (e.g., to "maintenance"), **Then** the charger status is updated
3. **Given** an existing charger at a station, **When** a partner user removes the charger, **Then** the charger is deleted
4. **Given** a charger add request with an unsupported connector type, **When** submitted, **Then** a validation error is returned

---

### Edge Cases

- What happens when a non-existent partner ID is provided in a station create request? → Returns 404 not found
- What happens when a station is created with coordinates that already have a station at the same location? → Should allow it (multiple chargers at same location is valid)
- How does the system handle deleting a partner that has associated stations? → Should be prevented or cascade with confirmation
- What happens when the database is unreachable during a write operation? → Returns 503 service unavailable with consistent error envelope

## Requirements

### Functional Requirements

- **FR-001**: Admin users MUST be able to create a partner record with name and type (operator, owner, roaming)
- **FR-002**: Admin users MUST be able to list all partners with their verification status
- **FR-003**: Admin users MUST be able to approve a pending partner, changing their status from "pending" to "verified"
- **FR-004**: Partner users MUST be able to create a station under their partner account with name, latitude, longitude, and optional address
- **FR-005**: Partner users MUST be able to update station name, address, and coordinates
- **FR-006**: Partner users MUST be able to delete a station they own
- **FR-007**: Partner users MUST be able to add a charger to a station with connector type and power rating
- **FR-008**: Partner users MUST be able to update charger status
- **FR-009**: Partner users MUST be able to remove a charger from a station
- **FR-010**: All write endpoints MUST validate input data and return field-level error messages for invalid inputs
- **FR-011**: All endpoints MUST use the same JSON response envelope as the Driver Service (data, error, meta)
- **FR-012**: All endpoints MUST be under the `/api/v1/` prefix
- **FR-013**: All write operations MUST be logged with actor identity and timestamp
- **FR-014**: [NEEDS CLARIFICATION: Authentication mechanism — should endpoints require JWT Bearer tokens, API keys, or are they open during MVP-2? The Driver Service currently has no auth, and there is no auth-gateway yet.]

### Key Entities

- **Partner**: An organization that owns or operates charging stations. Has name, type (operator/owner/roaming), verification status (pending/verified), and active status.
- **Station**: A physical EV charging location belonging to a partner. Has name, address, geographic coordinates, and is uniquely identified. Belongs to exactly one partner.
- **Charger**: An individual charging unit at a station. Has connector type, power rating in kW, and operational status. Belongs to exactly one station.

## Success Criteria

### Measurable Outcomes

- **SC-001**: An admin can complete the full partner lifecycle (create → list → approve) in under 2 minutes with zero errors
- **SC-002**: Station creation API responds within 500ms for 95% of requests
- **SC-003**: All write operations properly validate inputs and return clear error messages for every invalid field
- **SC-004**: 100% of write operations are logged with actor identity and timestamp
- **SC-005**: The service handles 50 concurrent write requests without data corruption or degradation below 1s response time

## Assumptions

- Authentication will use the same `ev-auth` crate scaffolded in Sprint 1.2's `source/crates/` — JWT validation logic is ready but a full auth gateway is not yet deployed; endpoints will accept a simple API key or JWT passed as a header
- The existing `borne-data` / `ev-db` crates provide the database layer — no additional database setup is needed beyond the existing `platform_db`
- Admin users are internal platform staff; partner users are external partner organization members — this distinction will be enforced by role claims in the auth token
- All endpoints are read-write (unlike the Driver Service which is read-only via `borne_data::list_all` and similar) — new SQL queries will be needed for INSERT, UPDATE, DELETE operations
- The existing `source/crates/ev-core` holds shared domain types; the admin service will add its own DTOs for request/response payloads
- No UI is included in this sprint — this is purely the REST API layer; the dashboard UI is a separate workstream
