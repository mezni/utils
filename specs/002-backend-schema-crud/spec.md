# Feature Specification: Backend Core — Schema, Identity & CRUD

**Feature Branch**: `006-backend-schema-crud`

**Created**: 2026-05-26

**Status**: Draft

**Input**: User description: "Phase 1 from docs/plan_mvp0.md — Backend Core: Schema, Identity & CRUD"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Admin Manages Identity & Partner Data (Priority: P1)

An administrator needs to create, read, update, and remove users and partner
profiles so that the platform's identity and partner data foundation is in
place. Stations, chargers, and connector types are managed in User Stories 3
and 4.

**Why this priority**: Without user and partner profile lifecycle management,
no other feature (admin portal, partner dashboard, mobile app) can function.
This is the identity foundation for the entire platform.

**Independent Test**: Can be fully tested by issuing create/read/update/remove
requests for users and partner profiles and verifying correct responses,
persisted data, and soft-removal behavior.

**Acceptance Scenarios**:

1. **Given** the system is running, **When** an admin creates a user with email,
   username, and role, **Then** the user is persisted with a `USR-` prefixed
   human-readable ID and a success confirmation is returned.
2. **Given** a user exists, **When** an admin lists all users, **Then** all
   active (non-removed) users are returned, each with their semantic ID,
   email, username, and role.
3. **Given** a user exists, **When** an admin updates selected fields, **Then**
   only the provided fields are changed and the record's last-modified timestamp
   advances.
4. **Given** a user exists, **When** an admin removes the user, **Then** the
   user is marked as removed and no longer appears in list or detail queries.
5. **Given** a partner profile is removed, **When** listing partner profiles,
   **Then** the removed profile is excluded from results.

---

### User Story 2 - Partner Self-Registration & Authenticated Access (Priority: P2)

A driver or partner operator registers an account and authenticates to receive
access credentials, then accesses partner-scoped resources filtered to their
own data.

**Why this priority**: Authentication is required before multi-tenant
partner-scoped access can work, but CRUD endpoints are usable without auth
for admin testing first.

**Independent Test**: Can be tested by registering a user, logging in to
receive credentials, then accessing a protected resource with and without
valid credentials to verify access control.

**Acceptance Scenarios**:

1. **Given** the system is running, **When** a driver submits registration with
   email, username, and password, **Then** a user with role `driver` is created
   and access credentials are returned.
2. **Given** a registered user, **When** they submit valid credentials to the
   login endpoint, **Then** access credentials containing their user ID and role
   are returned.
3. **Given** a partner-authenticated request, **When** listing stations, **Then**
   only stations owned by the authenticated partner are returned.
4. **Given** no valid credentials, **When** accessing a protected resource,
   **Then** the request is rejected as unauthorized.

---

### User Story 3 - Station & Charger Lifecycle Management (Priority: P3)

An admin or partner creates a charging station with geographic coordinates and
adds chargers to it, then updates charger status as conditions change (available,
occupied, faulted, offline).

**Why this priority**: Stations and chargers are the core physical assets — they
must exist before the mobile app can discover them, but the CRUD foundation (P1)
and auth (P2) must be in place first.

**Independent Test**: Can be tested by creating a station, adding chargers to it,
updating a charger's status, and verifying the station detail includes the
updated charger state.

**Acceptance Scenarios**:

1. **Given** a partner user exists, **When** an admin creates a station with
   name, address, city, coordinates, and owner, **Then** the station is
   persisted with a `STN-` prefixed ID and correct location data.
2. **Given** a station exists, **When** a charger is added with connector type,
   power rating, and current type, **Then** the charger is persisted under the
   station with a `CHG-` prefixed ID.
3. **Given** a charger exists, **When** its status is updated to `faulted`,
   **Then** the charger reflects the new status on subsequent retrieval.
4. **Given** a charger exists, **When** it is removed, **Then** the charger is
   permanently deleted and no longer retrievable.

---

### User Story 4 - Connector Type Configuration (Priority: P4)

An admin manages connector types (e.g., Type 2 AC, CCS 2 DC) that chargers
reference, ensuring the type catalog remains consistent and removals are
blocked when a type is in use.

**Why this priority**: Connector types are reference data that chargers depend
on, but CRUD on them is lower priority than the primary entity flows.

**Independent Test**: Can be tested by creating a connector type, attempting to
remove it while a charger references it (should fail), then removing the
charger and deleting the type successfully.

**Acceptance Scenarios**:

1. **Given** the system is running, **When** an admin creates a connector type
   with a unique name and description, **Then** it is persisted with a `CNT-`
   prefixed ID.
2. **Given** a connector type is referenced by a charger, **When** an admin
   attempts to remove it, **Then** the removal is rejected with an error
   indicating the type is in use.
3. **Given** a connector type is not referenced, **When** an admin removes it,
   **Then** it is marked as removed and excluded from list results.

---

### Edge Cases

- What happens when a user with a duplicate email or username is created?
  The system rejects the request with a conflict error.
- What happens when a station is removed that still has chargers?
  All associated chargers are automatically removed with the station.
- What happens when a partner attempts to access a station owned by another
  partner? The system denies access.
- What happens when an invalid semantic ID format is provided in a request?
  The system rejects the request with a descriptive validation error.
- What happens when a removed entity is targeted for update?
  The system treats it as non-existent and returns a not-found response.
- What happens when coordinates are outside the valid geographic range?
  The system validates latitude (-90 to 90) and longitude (-180 to 180) and
  rejects invalid coordinates.
- What happens when two admins update the same entity concurrently?
  The second update is rejected with a conflict error; the client must
  re-read the record and retry.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST support full lifecycle management (create, list, get
  by ID, update, remove) for users. List endpoints MUST support cursor-based
  pagination to handle growing datasets.
- **FR-002**: System MUST support full lifecycle management for partner profiles.
  List endpoints MUST support cursor-based pagination.
- **FR-003**: System MUST support full lifecycle management for stations, with
  listing filtered to the authenticated partner's own stations when accessed
  as a partner. Only users with partner or admin role may be assigned as
  station owners; driver-role users are rejected. List endpoints MUST support
  cursor-based pagination.
- **FR-004**: System MUST support creating and listing chargers within a
  specific station, plus updating and permanently deleting individual chargers.
  Charger list endpoints MUST support cursor-based pagination.
- **FR-005**: System MUST support full lifecycle management for connector types.
  List endpoints MUST support cursor-based pagination.
- **FR-006**: System MUST generate all entity IDs using a human-readable prefix
  followed by a 12-character lowercase alphanumeric code (e.g.,
  `USR-m1k9p2v4x7q3`, `STN-k4m2n9p1q5v8`).
- **FR-007**: System MUST use soft-removal for users, partner profiles,
  stations, and connector types — marking records as removed while preserving
  them — and MUST exclude removed records from all list and detail queries.
  System MUST enforce optimistic locking on updates: if a record has been
  modified since the client last read it, the update MUST be rejected with
  a conflict error.
- **FR-008**: System MUST permanently delete chargers when removal is requested
  (no soft-removal for chargers).
- **FR-009**: System MUST automatically remove all chargers belonging to a
  station when that station is removed, and MUST prevent removal of connector
  types that are referenced by existing chargers.
- **FR-010**: System MUST enforce multi-tenant isolation by returning only the
  stations a partner owns when the request is authenticated as that partner.
- **FR-011**: System MUST provide a registration endpoint allowing drivers to
  create accounts, and a login endpoint accepting email and password that
  returns access credentials. Passwords MUST be at least 8 characters long.
- **FR-012**: System MUST verify access credentials on protected endpoints and
  reject requests lacking valid credentials. Access credentials MUST expire
  24 hours after issuance.
- **FR-013**: System MUST initialize the database schema on first run,
  including all tables, enumerations, relationships, and spatial indexing.
- **FR-014**: System MUST load deterministic seed data (2 connector types,
  5 partner users, 5 partner profiles, 100 stations, 300 chargers) all marked
  as test records.
- **FR-015**: System MUST store station coordinates as spatial data and
  validate that longitude falls within -180 to 180 and latitude within -90 to
  90 on input.
- **FR-016**: System MUST exclude test records from query results by default,
  with an optional parameter for admin-facing queries to include them.

### Key Entities *(include if feature involves data)*

- **User**: Represents an authenticated actor (admin, partner, or driver).
  Identified by `USR-` prefixed ID. Has email, username, password hash (Argon2id),
  role, and soft-removal support. Partners own stations; drivers discover them;
  admins manage all.

- **Partner Profile**: Extended profile for partner-type users. Identified by
  `PRT-` prefixed ID. Links to a user. Has classification (business/private),
  display name, tax ID, contact phone, and soft-removal support.

- **Station**: A physical charging station location with geographic coordinates.
  Identified by `STN-` prefixed ID. Owned by a partner or admin user.
  Has name, address, city, coordinates, operational status, and soft-removal
  support. Coordinates stored as spatial data. Admin-owned stations serve
  unassigned or test infrastructure.

- **Charger**: A physical charger unit at a station. Identified by `CHG-`
  prefixed ID. Belongs to a station, references a connector type. Has power
  rating, current type (AC/DC), and status (available/occupied/faulted/offline).
  Permanently deleted only (no soft-removal).

- **Connector Type**: A reference type for charger connectors (e.g., Type 2 AC,
  CCS 2 DC). Identified by `CNT-` prefixed ID. Has unique name and description.
  Cannot be removed while referenced by chargers.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can complete any single entity operation (create, read,
  update, or remove) in under 1 second.
- **SC-002**: Every generated ID across all entity types matches the semantic
  format `[PREFIX]-[12-char-alphanumeric]` with no exceptions.
- **SC-003**: Removed entities are completely invisible in list and detail
  queries — zero leaked records across 1000 requests.
- **SC-004**: Partner-scoped data access returns only the authenticated
  partner's own data — cross-tenant access is denied for 100% of unauthorized
  attempts.
- **SC-005**: Unauthenticated requests to protected resources are denied 100%
  of the time.
- **SC-006**: Seed data loading produces identical results on every execution,
  verified by comparing record counts and ID values across two independent
  runs.
- **SC-007**: Station coordinates are stored and retrieved accurately,
  returning the same longitude and latitude values that were submitted (within
  floating-point tolerance).
- **SC-008**: Initial database setup completes in under 10 seconds including
  seed data.

## Clarifications

### Session 2026-05-26

- Q: Should the system restrict station creation to users with the `partner` role only, or allow any authenticated user to own a station? → A: Only partner-role and admin-role users can own stations; creation rejects driver-role users. Admin-owned stations serve unassigned or test infrastructure.
- Q: How should list endpoints handle large result sets? → A: Cursor-based pagination (keyset) on all list endpoints.
- Q: What should the access credential (token) expiry duration be? → A: 24 hours.
- Q: How should the system handle concurrent updates to the same entity? → A: Optimistic locking — reject update if record was modified since read (last-write-wins with conflict detection).
- Q: What password strength rules should the registration endpoint enforce? → A: Minimum 8 characters.

## Assumptions

- The database supports spatial data types and spatial indexing (available in
  the containerized database established in Phase 0).
- Token-based authentication is sufficient for MVP0; external identity
  providers are deferred to post-MVP0.
- Passwords are hashed using a standard secure algorithm before storage.
- All API consumers (admin portal, partner dashboard, mobile app) share a
  common, versioned API surface.
- The test flag defaults to `false` on all user-created records; only seed
  data carries the test indicator.
- Partner-scoped station filtering uses the owner relationship linked to the
  users entity, matching the established data model.
- Charger removal is permanent (no soft-removal), per the platform's design
  decision that charger records are disposable while station history must be
  preserved.
- Role-based access control for specific endpoints is not enforced in this
  phase — that is deferred to later phases when the admin portal and partner
  dashboard are built.
- The database connection and server configuration use reasonable defaults
  suitable for development and local testing.
