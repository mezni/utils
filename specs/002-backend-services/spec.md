# Feature Specification: Backend Services

**Feature Branch**: `002-backend-services`

**Created**: 2026-06-11

**Status**: Draft

**Input**: User description: "read from mvp 1 phase 2"

## Clarifications

### Session 2026-06-11

- Q: What partner_id assignment model for station creation? → A: Client provides partner_id as a required field (no auth context available in MVP-1 Phase 2)
- Q: What is the error response shape? → A: Consistent `{ "error": { "code", "message", "details?` } }` format across all endpoints
- Q: Are test requirements part of this spec? → A: Yes — Constitution III mandates TDD, 80%+ unit, 100% contract, integration tests
- Q: What fields are required for event ingestion? → A: event_type, session_id, and occurred_at are minimum required; payload is optional
- Q: Batch atomicity — all-or-nothing or best-effort? → A: All-or-nothing — any validation failure rejects the entire batch

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Station Discovery via Driver Service (Priority: P1)

As a mobile app user, I want to browse charging stations near me and view detailed information so I can find a suitable place to charge my vehicle.

**Why this priority**: This is the core MVP-1 functionality — without station discovery APIs, the mobile app cannot display stations to drivers.

**Independent Test**: Start the driver-service, then call `GET /api/v1/stations/nearby?lat=36.8&lng=10.18&radius=10` and verify it returns stations sorted by distance with correct coordinates and chargers.

**Acceptance Scenarios**:

1. **Given** the driver-service is running and the database contains seed stations, **When** a user requests the station list with page and per_page parameters, **Then** the API returns paginated results with total count and correct station metadata
2. **Given** the driver-service is running, **When** a user requests nearby stations with latitude, longitude, and radius, **Then** the API returns stations within that radius sorted by distance
3. **Given** the driver-service is running, **When** a user requests a specific station by ID, **Then** the API returns full station details including charger array, pricing, hours, and status
4. **Given** the driver-service is running, **When** a user requests a non-existent station, **Then** the API returns a 404 error with a consistent error response shape

---

### User Story 2 - Station Management via Admin Service (Priority: P1)

As a platform operator, I want to create, update, and remove charging stations so the station inventory stays accurate.

**Why this priority**: Without station management, the inventory cannot be maintained — new stations cannot be added and outdated stations cannot be removed.

**Independent Test**: Start the admin-service, then create a station via `POST /api/v1/stations`, fetch it via the driver-service, update it via `PUT /api/v1/stations/{id}`, and verify the update is reflected.

**Acceptance Scenarios**:

1. **Given** the admin-service is running, **When** an operator creates a station with valid data including location and chargers, **Then** the API returns the created station with a generated ID and chargers are stored
2. **Given** a station exists, **When** an operator updates its fields via PUT, **Then** only the provided fields are updated and the response reflects the changes
3. **Given** a station exists with active chargers, **When** an operator deletes the station, **Then** the station is soft-deleted (deleted_at is set) and no longer appears in discovery queries
4. **Given** the admin-service receives a request with invalid data (missing required fields, invalid coordinates), **When** the request is processed, **Then** the API returns a 400 error with field-level validation messages

---

### User Story 3 - Event Ingestion via Admin Service (Priority: P2)

As a product analyst, I want user interactions captured reliably so I can understand usage patterns and improve the product.

**Why this priority**: Event data enables data-driven decisions. Without it, the team relies on guesses about how users interact with the app.

**Independent Test**: Send events via `POST /api/v1/events` and `POST /api/v1/events/batch`, then query the analytics_db directly to verify the events were persisted with correct data.

**Acceptance Scenarios**:

1. **Given** the admin-service is running, **When** a single event is sent with valid payload, **Then** it is persisted to analytics_db.raw_events and the API returns 201 with the event ID
2. **Given** the admin-service is running, **When** a batch of up to 100 events is sent, **Then** all events are persisted atomically and the API returns 201 with count of ingested events
3. **Given** a batch exceeds 100 events, **When** the request is processed, **Then** the API returns a 400 error with a message indicating the batch size limit
4. **Given** event data has been ingested, **When** queried in analytics_db, **Then** the append-only rules prevent UPDATE and DELETE operations on any event

---

### Edge Cases

- What happens when both databases are unreachable at service startup? (graceful degradation, health check endpoint)
- How does the driver-service handle a malformed spatial query (e.g., lat/lng out of range)?
- What happens when a batch event contains mixed valid and invalid events?
- How does the system respond when analytics_db is down but platform_db is healthy?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Driver-service MUST expose three station discovery endpoints: paginated list, nearby search by radius, and station detail by ID
- **FR-002**: Nearby search endpoint MUST accept latitude, longitude, and radius parameters and return stations sorted by distance
- **FR-003**: Station detail endpoint MUST return chargers nested under the station with type, power, pricing, and status
- **FR-004**: Both services MUST return a consistent JSON error response shape for all error conditions (400, 404, 500)
- **FR-005**: Both services MUST expose a health check endpoint at `/health` returning service status and database connectivity
- **FR-006**: Admin-service MUST expose endpoints to create, update (partial), and soft-delete stations
- **FR-007**: Station create endpoint MUST accept station location and an array of chargers in a single request
- **FR-008**: Station soft-delete MUST set the `deleted_at` timestamp — stations marked as deleted MUST be excluded from discovery queries
- **FR-009**: Admin-service MUST expose an endpoint to ingest a single analytics event into analytics_db
- **FR-010**: Admin-service MUST expose an endpoint to batch-ingest up to 100 analytics events atomically into analytics_db
- **FR-011**: Batch event endpoint MUST reject requests exceeding 100 events with a clear error message
- **FR-012**: Both services MUST establish database connection pools at startup and report readiness via the health endpoint
- **FR-013**: Services MUST log all API requests with request ID, method, path, status code, and duration
- **FR-014**: The consistent JSON error response MUST include error code, human-readable message, and optional details array
- **FR-015**: Station create MUST accept partner_id as a required field to associate the station with an existing partner
- **FR-016**: Single event ingestion MUST require event_type, session_id, and occurred_at as minimum fields; payload is optional JSON
- **FR-017**: Batch event ingestion MUST be all-or-nothing — if any event in the batch fails validation, the entire batch is rejected with a 400 error listing all validation failures
- **FR-018**: Both services MUST have unit test coverage of at least 80%
- **FR-019**: Both services MUST have contract tests covering all API endpoints (100% coverage)
- **FR-020**: Integration tests MUST verify that driver-service connects only to platform_db and admin-service connects to both platform_db and analytics_db

### Key Entities *(include if feature involves data)*

- **Station**: A physical charging location with geographic coordinates, address, operating hours, status, and partner association. Lives in platform_db.inventory.
- **Charger**: An individual charging connector within a station. Has type (CCS2, CHAdeMO, Type2), power rating, price, and status. Lives in platform_db.inventory.
- **Analytics Event**: An immutable record of a user interaction (station viewed, search performed). Contains event type, session/user identifiers, and JSON payload. Lives in analytics_db.raw_events.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All station discovery endpoints return responses in under 100ms (p95) with up to 1000 stations in the database
- **SC-002**: Station create with 5 chargers completes in under 200ms
- **SC-003**: Nearby search with invalid coordinates returns a 400 error with clear validation message — never crashes the service
- **SC-004**: Batch ingestion of 100 events completes in under 500ms and all events are queryable in analytics_db
- **SC-005**: Both services start, pass health checks, and serve requests within 10 seconds of startup
- **SC-006**: Soft-deleted stations disappear from discovery queries within the next API response (no caching delay)
- **SC-007**: All unit tests pass with 80%+ line coverage across both services
- **SC-008**: All contract tests pass for every API endpoint (100% coverage)
- **SC-009**: Batch ingestion with one invalid event among 100 is fully rejected — zero events are persisted

## Assumptions

- Application-level authentication is out of scope for Phase 2 (Keycloak integration is MVP-3)
- Both services connect to existing platform_db and analytics_db databases (Phase 1 infra is running)
- API responses use JSON format with standard HTTP status codes
- Services are started locally on :8080 (driver-service) and :8081 (admin-service)
- Rate limiting and API key validation are out of scope for Phase 2
- Event payload schema validation is minimal (accept arbitrary JSON, validate only event_type presence)
- Services use the database credentials from the existing .env file
- The consistent error response shape follows: `{ "error": { "code": "ERROR_CODE", "message": "Human-readable description" } }` with optional `details` array for field-level validation errors
- Station IDs are server-generated nanoids with STA- prefix per constitution
- Event payload accepts arbitrary JSON; only event_type, session_id, and occurred_at are validated as required
- Tests run against a separate test database, not the development seed database
