# Feature Specification: Driver Service

**Feature Branch**: `009-driver-service`

**Created**: 2026-06-09

**Status**: Draft

**Input**: User description: "Sprint 2.3 — Driver Service. REST API for station discovery: nearby, markers, search, detail, reviews stub. All queries enforce partner visibility filter."

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Browse Nearby Stations (Priority: P1)

A driver opens the app and sees charging stations near their current location, ordered by distance. Only stations from verified, active, live partners are shown.

**Why this priority**: This is the core discovery flow. Without it, drivers cannot find stations.

**Independent Test**: Can be tested by querying a known coordinate and verifying returned stations are within the specified radius, sorted by distance, and all belong to verified+live+active partners.

**Acceptance Scenarios**:

1. **Given** a coordinate (36.8008, 10.1815) and a 10 km radius, **When** nearby stations are requested, **Then** only stations within 10 km are returned
2. **Given** stations owned by different partners, **When** nearby stations are requested, **Then** stations from unverified or non-live partners are excluded
3. **Given** no stations exist within the radius, **When** nearby stations are requested, **Then** an empty list is returned

---

### User Story 2 — View Station Detail (Priority: P1)

A driver taps a station on the map and sees its name, address, and the list of chargers with their types, power ratings, and status.

**Why this priority**: Drivers need charger-level detail to decide which station to visit.

**Independent Test**: Can be tested by requesting a specific station ID and verifying all charger information is returned with correct connector types, power values, and statuses.

**Acceptance Scenarios**:

1. **Given** a valid station ID, **When** station detail is requested, **Then** the response includes station name, address, coordinates, and all chargers
2. **Given** an invalid station ID, **When** station detail is requested, **Then** a not-found error is returned
3. **Given** a station with chargers in various states, **When** station detail is requested, **Then** each charger shows its connector type, power rating in kW, and current status

---

### User Story 3 — Search Stations (Priority: P2)

A driver searches for stations by name or address, optionally filtering by connector type.

**Why this priority**: Text search improves discoverability beyond map browsing. Connector filtering helps drivers with specific vehicle requirements.

**Independent Test**: Can be tested by searching for a known station name and verifying the correct station is returned. Filter by connector type and verify only stations with matching chargers are returned.

**Acceptance Scenarios**:

1. **Given** a text query matching a station name or address, **When** search is performed, **Then** matching stations are returned
2. **Given** a text query and a connector type filter, **When** search is performed, **Then** only stations with chargers of that connector type are returned
3. **Given** a text query with no matches, **When** search is performed, **Then** an empty list is returned

---

### User Story 4 — View Map Markers (Priority: P2)

A driver pans the map and sees station markers within the visible bounding box, with basic info (name, availability status) for quick scanning.

**Why this priority**: Map-based browsing is the primary navigation pattern. Bounding box queries enable efficient map rendering.

**Independent Test**: Can be tested by providing a bounding box that contains known stations and verifying they are returned with name and availability status.

**Acceptance Scenarios**:

1. **Given** a bounding box containing known stations, **When** markers are requested, **Then** each station includes name and availability status
2. **Given** a bounding box with no stations, **When** markers are requested, **Then** an empty list is returned

---

### User Story 5 — View Station Reviews Stub (Priority: P3)

A driver views a station's review/rating section and sees a placeholder indicating reviews are coming soon.

**Why this priority**: Reviews are a future enhancement. A stub endpoint ensures the mobile/web app can display the reviews section without crashing.

**Independent Test**: Can be tested by requesting reviews for any valid station ID and verifying a 200 response with a meaningful placeholder message.

**Acceptance Scenarios**:

1. **Given** a valid station ID, **When** reviews are requested, **Then** a 200 response is returned with a "coming soon" message
2. **Given** an invalid station ID, **When** reviews are requested, **Then** a not-found error is returned

---

### Edge Cases

- What happens when the database has no stations at all?
- How does the system handle extremely large bounding boxes or radii (e.g., covering the entire country)?
- What happens when a partner's visibility flags change (e.g., goes from live to not-live) — cached results may briefly show their stations?
- How does the system handle a station with zero chargers?
- What happens when the database connection fails or times out?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST return stations within a given radius of a coordinate, ordered by distance ascending
- **FR-002**: System MUST return stations within a given bounding box with station name and availability status
- **FR-003**: System MUST support text search on station name and address, with optional connector type filter
- **FR-004**: System MUST return full station detail (including all chargers) for a given station ID
- **FR-005**: System MUST return a reviews placeholder for any valid station ID
- **FR-006**: System MUST have a health check endpoint that returns service status
- **FR-007**: System MUST enforce the partner visibility rule on all station queries: only stations whose partner has `is_active = true`, `is_verified = true`, and `is_live = true` are returned
- **FR-008**: System MUST return appropriate HTTP error codes for invalid requests (400), not-found resources (404), and server errors (500)
- **FR-009**: System MUST accept configuration via environment variables

### Key Entities

- **Station**: A charging station with location, address, and partner owner. Has one or more chargers and a current availability status.
- **Charger**: An individual charging unit at a station with connector type, power rating, and operational status.
- **Partner**: The organization or individual that owns stations. Has visibility flags (verified, live, active) that control whether stations appear in driver-facing queries.
- **Station Availability**: The current availability state of a station (available, partial, unavailable).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Drivers can find nearby stations within 10 seconds of opening the app (including map load and data fetch)
- **SC-002**: Station detail pages load fully (including all chargers) within 2 seconds
- **SC-003**: Text search returns results within 3 seconds
- **SC-004**: All station queries correctly exclude stations from partners that are unverified, non-live, or inactive — verified by integration tests covering all flag combinations
- **SC-005**: The service starts successfully from configuration alone (no hardcoded values) — verified by providing only environment variables
- **SC-006**: The health endpoint responds within 500 ms

## Assumptions

- Drivers are not authenticated in this sprint — partner visibility is enforced server-side, not per-user
- The database schema from Sprint 2.2 is already applied and seeded
- Stations, chargers, and partners already exist in the database via seeds or admin service (Sprint 2.4)
- Reviews functionality is out of scope — only a placeholder endpoint is provided
- The service runs as a standalone binary, not behind a reverse proxy (that comes in Sprint 2.5)
- All station queries use the existing spatial index (idx_station_location) for performance
