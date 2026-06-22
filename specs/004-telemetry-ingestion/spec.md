# Feature Specification: Telemetry Ingestion Core

**Feature Branch**: `004-telemetry-ingestion`

**Created**: 2026-06-22

**Status**: Revised (Architecture Alignment)

**Input**: Implement telemetry ingestion core with event validation, normalization, idempotency, and integration with analytics database. Ensure single-writer analytics enforcement (driver-service only).

## Architecture Overview

```
auth-service, driver-service, inventory-service
        ↓
POST /api/v1/telemetry/events
        ↓
Traefik → driver-service
        ↓
Schema Validation → Normalization → Enrichment → Idempotency
        ↓
Direct DB Write to analytics_db
        ↓
admin-service (read-only API)
        ↓
Analytics Team
```

### Key Design Decisions

1. **No Event Bus**: Embedded ingestion in driver-service for simplicity, guaranteed persistence, and no additional service dependency
2. **Core Services Only**: auth-service, driver-service, inventory-service generate telemetry events
3. **Event Type Registry**: Fixed enum for type safety and governance
4. **Idempotency**: UUID v7 with unique database index (time-ordered, globally unique)
5. **Location Provenance**: Required `location_source` field (event_location, session_location, last_known_location, default_location)
6. **Schema Migration**: 30-day grace period for deprecated versions with automatic marking
7. **Dead Letter Store**: Dedicated table for debugging and audit (not a queue)
8. **Analytics Query Flow**: Through admin-service with read-only database role
9. **Single Writer**: driver-service ONLY can write to analytics_db

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Driver Sends Telemetry Events

**Goal**: Driver service receives and processes telemetry events from various services, validates them against schema, enriches them with metadata, and persists to analytics database with idempotency.

**Why this priority**: Core functionality for all analytics and monitoring needs.

**Independent Test**: Send valid event to `POST /api/v1/telemetry/events` from auth-service or inventory-service, verify event appears in analytics database, check that duplicate events are rejected with idempotency key.

**Acceptance Scenarios**:

1. **Given** a driver-service receives a valid telemetry event from auth-service with schema_version, user_id (UUID), and payload, **When** the event is ingested, **Then** the event is validated, enriched with location/session/role/system metadata, and persisted to analytics_events table

2. **Given** a malformed event (missing schema_version, invalid user_id, or invalid timestamp), **When** the event is ingested, **Then** the event is rejected and logged to dead_letter_events table with provenance

3. **Given** the same event is sent twice with matching idempotency_key (UUID v7), **When** the second ingestion is attempted, **Then** the duplicate is rejected (idempotency guaranteed)

4. **Given** an event with unknown schema_version, **When** the event is ingested, **Then** the event is rejected (no 30-day grace period - immediate rejection)

5. **Given** an event with deprecated schema_version (30+ days old), **When** the event is ingested, **Then** the event is rejected (deprecated versions no longer supported)

6. **Given** an event missing location metadata, **When** the event is ingested, **Then** the location_source defaults to "default_location"

### User Story 2 - Analytics Team Queries Telemetry Events

**Goal**: Analytics team can query and analyze telemetry events through admin-service read APIs without compromising single-writer enforcement.

**Why this priority**: Enables monitoring, debugging, and business intelligence while maintaining single-writer architecture.

**Independent Test**: Query events from admin-service GET /api/v1/analytics/events, verify results match expected criteria, verify no write access.

**Acceptance Scenarios**:

1. **Given** analytics team queries for events by user_id through admin-service, **When** the query is executed, **Then** results are returned filtered by user_id with pagination support

2. **Given** analytics team queries for events within a date range through admin-service, **When** the query is executed, **Then** results are filtered by timestamp range

3. **Given** analytics team queries for events by schema_version through admin-service, **When** the query is executed, **Then** results are filtered by schema_version

4. **Given** analytics team queries for events by event_type through admin-service, **When** the query is executed, **Then** results are filtered by event_type

5. **Given** analytics team attempts to write to analytics_db through admin-service, **When** the write attempt is made, **Then** the write is rejected with 403 Forbidden (admin-service has no write access)

6. **Given** analytics team queries for events, **When** the query is executed, **Then** results are paginated by page_number and page_size (default 100 events per page)

### User Story 3 - CI Gates Enforce Telemetry Rules

**Goal**: CI pipeline enforces telemetry governance rules (analytics write isolation, schema validation, idempotency, routing, payload structure).

**Why this priority**: Ensures system-wide telemetry integrity and prevents violations of single-writer architecture.

**Independent Test**: Try to write to analytics_db from a service other than driver-service, verify CI gate fails; try to send malformed event, verify schema validation fails.

**Acceptance Scenarios**:

1. **Given** any service other than driver-service attempts to write to analytics_db, **When** the CI gate runs, **Then** the gate fails and prevents commit

2. **Given** an event without schema_version is sent, **When** the event schema validation runs, **Then** the event is rejected

3. **Given** a duplicate event is ingested, **When** the idempotency gate runs, **Then** the duplicate detection fails

4. **Given** an event with unknown schema_version is sent, **When** the schema validation runs, **Then** the event is rejected

5. **Given** an event with deprecated schema_version (30+ days old) is sent, **When** the schema validation runs, **Then** the event is rejected

6. **Given** a telemetry request is sent to a service other than driver-service, **When** the telemetry routing gate runs, **Then** the routing fails

## Requirements *(mandatory)*

### Functional Requirements

- **FR-TELE-001**: Telemetry ingestion endpoint must accept POST requests at `/api/v1/telemetry/events` from any service (auth-service, driver-service, inventory-service), validate event schema (schema_version, user_id, timestamp, payload), reject malformed events with detailed error messages, and persist to analytics_events table

- **FR-TELE-002**: Telemetry event validation layer must enforce:
  - schema_version matching known versions only (reject unknown versions)
  - schema_version not deprecated (reject versions > 30 days old)
  - user_id as valid UUID format
  - timestamp as valid ISO 8601 format
  - payload as valid JSON object
  - event_type matching EventType enum values

- **FR-TELE-003**: Event normalization pipeline must:
  - Validate event structure
  - Enrich events with location metadata (with provenance via location_source field)
  - Enrich events with session metadata (duration, start time, last activity)
  - Enrich events with role context (driver, partner, admin)
  - Enrich events with system metadata (service_name, event_source)
  - Generate idempotency_key using UUID v7
  - Persist validated events to analytics_events table

- **FR-TELE-004**: Idempotency system must:
  - Generate idempotency_key using UUID v7 (time-ordered, globally unique)
  - Create unique index on idempotency_key in analytics_events table
  - Reject duplicate events with matching idempotency_key
  - Log duplicate events for audit

- **FR-TELE-005**: Event enrichment must add:
  - location metadata with provenance: event_location (from event payload), session_location (from active session), last_known_location (cached from user profile), default_location (fallback "Unknown location")
  - session metadata: session_start (timestamp when session began), session_duration (seconds since session start), last_activity (timestamp of last event in session)
  - role context: role from JWT claims (driver, partner, admin)
  - system metadata: service_name (source service), event_source (event type identifier)

- **FR-TELE-006**: Event schema registry in domain-types must define:
  - EventType enum: AUTH_LOGIN, AUTH_LOGOUT, TOKEN_REFRESH, LOCATION_UPDATE, SESSION_START, SESSION_END, DRIVER_STATUS, INVENTORY_UPDATE, PRICE_CHANGE, STOCK_ALERT, ERROR_UNHANDLED
  - TelemetryEvent struct with all required fields
  - EnrichedMetadata struct with location/session/role/system subfields
  - LocationSource enum with provenance tracking (EventLocation, SessionLocation, LastKnownLocation, DefaultLocation)
  - Separate schemas for different event types (auth, location, session, error)

- **FR-TELE-007**: Admin-service must provide read-only API for analytics queries:
  - GET /api/v1/analytics/events (query parameters: user_id, start_time, end_time, schema_version, event_type, page_number, page_size)
  - Response: paginated list of events with total_count, total_pages
  - Return 403 Forbidden if write operation attempted

- **FR-TELE-008**: Admin-service database role must be read-only:
  - GRANT SELECT ON analytics_db TO bornemap_analytics_reader
  - REVOKE ALL PRIVILEGES ON analytics_db FROM bornemap_analytics_reader
  - No INSERT, UPDATE, DELETE, CREATE, ALTER, DROP privileges

- **FR-TELE-009**: Dead-letter logging must capture malformed events with:
  - Full event payload (JSONB)
  - Error details: validation failure, schema mismatch, error_type, error_message
  - Timestamp of error capture
  - Original request_id for traceability
  - Schema_version of original event
  - Retry_attempts counter

- **FR-TELE-010**: Telemetry routing must route all `/api/v1/telemetry/*` requests to driver-service only, verify request comes from driver-service via JWT authentication, return 403 Forbidden for other services

### Key Entities *(include if feature involves data)*

- **TelemetryEvent**: Core event structure with schema_version, user_id (UUID), timestamp, event_type (enum), payload (JSON), idempotency_key (UUID v7), enriched_metadata (location/session/role/system), status (pending, processed, failed)

- **LocationMetadata**: Additional metadata with provenance including:
  - latitude (Option<f64>)
  - longitude (Option<f64>)
  - country (Option<String>)
  - city (Option<String>)
  - location_source (LocationSource enum: EventLocation, SessionLocation, LastKnownLocation, DefaultLocation)

- **SessionMetadata**: Session context including session_start (DateTime), session_duration (seconds), last_activity (DateTime)

- **RoleMetadata**: Role context from JWT claims (driver, partner, admin)

- **SystemMetadata**: System context including service_name (string), event_source (string)

- **EventEnrichment**: Aggregate of location, session, role, and system metadata attached to events

- **EventSchema**: Schema registry entries defining telemetry event structure including schema_version, required fields, field types, validation rules, supported event types

- **EventType**: Fixed enum for event type governance (AUTH_LOGIN, AUTH_LOGOUT, TOKEN_REFRESH, LOCATION_UPDATE, SESSION_START, SESSION_END, DRIVER_STATUS, INVENTORY_UPDATE, PRICE_CHANGE, STOCK_ALERT, ERROR_UNHANDLED)

- **AnalyticsEvent**: Persisted event in analytics_events table with all enriched fields, status tracking, duplicate detection index on idempotency_key, created_at and updated_at timestamps

- **DeadLetterEvent**: Captured malformed events with full payload, error details, error_type, timestamp, original_request_id, retry_attempts, schema violations

- **AnalyticsReaderRole**: Database role for admin-service with SELECT-only privileges on analytics_db

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All telemetry events sent from auth-service, driver-service, and inventory-service are successfully ingested and appear in analytics_events table within 1 second

- **SC-002**: Events with missing schema_version, invalid user_id (non-UUID), invalid timestamp, or unknown event_type are rejected with 400 Bad Request status and detailed error messages

- **SC-003**: Events with unknown schema_version or deprecated schema_version (30+ days old) are rejected immediately (no grace period)

- **SC-004**: Events with unknown location_source default to "default_location" and continue processing

- **SC-004**: Duplicate events with matching idempotency_key (UUID v7) are rejected and marked as duplicate (idempotency guaranteed)

- **SC-005**: Analytics write gate in CI pipeline fails when any service other than driver-service attempts to write to analytics_db

- **SC-006**: Schema validation gate in CI pipeline fails when events are sent without schema_version or with invalid field types

- **SC-007**: Idempotency gate in CI pipeline detects duplicate event ingestion attempts

- **SC-008**: Telemetry routing gate ensures only driver-service can access telemetry endpoints

- **SC-009**: Payload structure validation gate in CI pipeline fails when events are sent with malformed payloads

- **SC-010**: Analytics queries through admin-service return paginated results with proper filtering

- **SC-011**: Admin-service has no write access to analytics_db (403 Forbidden on write attempts)

- **SC-012**: Dead-letter events are captured with full error details and traceability information

- **SC-013**: All events include location_source provenance (EventLocation, SessionLocation, LastKnownLocation, or DefaultLocation)

- **SC-014**: 100% of telemetry events from auth-service, driver-service, and inventory-service pass validation and idempotency checks

- **SC-015**: Event type enum is used for all events (no free-form strings)

- **SC-016**: Idempotency keys are UUID v7 (time-ordered, globally unique)

## Assumptions

- Telemetry events are sent from auth-service, driver-service, and inventory-service
- Geolocation data may be missing; fallback to default location
- Session metadata is tracked by authentication system and attached to events
- Idempotency_key is generated by driver-service (not by sender services)
- Event schemas evolve with new versions, old schemas are deprecated but supported for 30 days
- Analytics database is maintained as read-only for all services except driver-service
- Dead-letter store is implemented as separate analytics_events_dead_letter table
- CI gates are executed on every commit and PR
- User profiles contain cached location data (TTL 30 minutes)
- Admin-service queries must be paginated to prevent large result sets

## Out of Scope (Explicitly Excluded)

1. Real-time telemetry streaming (events are sent directly)
2. Event replay mechanism (future enhancement)
3. Event analytics and visualization dashboards
4. Event compression for large payloads
5. Event prioritization and queue management
6. Telemetry rate limiting per user
7. Event aggregation and summarization
8. Multi-region telemetry replication
9. Event sampling for reduced storage
10. Event archive and retention policies (beyond basic table structure)

## Dependencies

- **Internal Dependencies**:
  - Sprint 1: Keycloak authentication system must be complete (JWT roles, user_id UUID)
  - Sprint 2: GIS engine must be complete (for geolocation data in user profiles)
  - Domain-types crate: Event schemas must be defined in domain-types before implementation

- **External Dependencies**:
  - PostgreSQL 16+ for analytics database
  - Redis for caching (optional, for user profile geolocation cache)

## Risks and Mitigations

### Risk R-TELE-1: Event Schema Validation Failures
**Risk**: Events with invalid schema could be accepted and cause data quality issues.
**Impact**: High - corrupt analytics data, incorrect monitoring.
**Mitigation**:
  - Enforce schema validation at ingestion time with strict rules
  - Use PostgreSQL ENUMs for fixed schema_version values
  - Reject unknown versions immediately (no grace period)
  - Log all validation failures for debugging
  - Add CI gate for schema validation

### Risk R-TELE-2: Duplicate Event Ingestion
**Risk**: Same event sent multiple times could corrupt analytics data.
**Impact**: High - duplicate metrics, incorrect analytics.
**Mitigation**:
  - Enforce idempotency at database level with unique index
  - Use UUID v7 for idempotency_key (time-ordered, globally unique)
  - Implement idempotency CI gate
  - Log duplicate events for audit

### Risk R-TELE-3: Analytics Write Violations
**Risk**: Another service accidentally writes to analytics_db, breaking single-writer policy.
**Impact**: Critical - analytics corruption, data loss.
**Mitigation**:
  - Enforce database role permissions (driver-service only)
  - Implement CI analytics write gate
  - Audit logging for all writes to analytics_db
  - Database constraint enforcement

### Risk R-TELE-4: Performance Bottlenecks
**Risk**: High event volume could overwhelm ingestion pipeline.
**Impact**: Medium - slow ingestion, database performance issues.
**Mitigation**:
  - Implement event batching at sender services
  - Add Redis caching for frequent queries
  - Add query pagination and filtering
  - Implement query optimization with indexes

### Risk R-TELE-5: Geolocation Data Loss
**Risk**: Missing geolocation data could reduce event usefulness.
**Impact**: Low - events still usable without geolocation.
**Mitigation**:
  - Make geolocation optional with default location
  - Use user's last known location if unavailable
  - Log geolocation availability for analysis
  - Cache geolocation for session duration

### Risk R-TELE-6: Location Source Confusion
**Risk**: Ambiguous location provenance could lead to incorrect analytics.
**Impact**: Medium - incorrect location-based insights.
**Mitigation**:
  - Require location_source field (must be one of four enum values)
  - Document provenance rules clearly
  - Add validation for location_source field
  - Include location_source in all analytics queries

### Risk R-TELE-7: Schema Version Conflicts
**Risk**: Version conflicts between old and new schema deployments.
**Impact**: Medium - data quality issues, analytics discrepancies.
**Mitigation**:
  - Enforce 30-day deprecation period
  - Automatically reject deprecated versions
  - Track schema versions in metadata
  - Log schema version changes for audit

## Test Strategy

### Unit Tests
- Event schema validation (missing fields, invalid types, unknown versions)
- Idempotency key generation with UUID v7
- Event enrichment logic with all location sources
- Event deduplication logic
- Event normalization pipeline
- Location source provenance validation
- Pagination logic for analytics queries

### Integration Tests
- Telemetry ingestion endpoint from auth-service
- Telemetry ingestion endpoint from inventory-service
- Event schema validation
- Idempotency enforcement
- Dead-letter logging
- Database write operations
- CI gate validation
- Admin-service read API queries

### E2E Tests
- Full event ingestion flow from auth-service to database
- Full event ingestion flow from inventory-service to database
- End-to-end validation and persistence
- Error handling for malformed events
- Idempotency across multiple senders
- Analytics queries through admin-service
- Admin-service write attempt rejection

### Performance Tests
- Event ingestion throughput
- Validation performance
- Database write performance
- Idempotency check performance
- CI gate execution time
- Pagination performance for large result sets

## Glossary

- **TelemetryEvent**: Data packet containing event information including schema_version, user_id (UUID), timestamp, event_type (enum), payload, idempotency_key (UUID v7), and enriched metadata with provenance
- **IdempotencyKey**: UUID v7 (time-ordered, globally unique) used to detect and reject duplicate events
- **SchemaRegistry**: Centralized definition of event schemas and their versions in domain-types crate
- **EventType**: Fixed enum for event type governance (AUTH_LOGIN, AUTH_LOGOUT, etc.)
- **LocationSource**: Provenance field indicating source of location data (EventLocation, SessionLocation, LastKnownLocation, DefaultLocation)
- **AnalyticsDB**: Database exclusively for analytics data, write-only for driver-service
- **EventEnrichment**: Additional metadata automatically added to events (location with provenance, session, role, system)
- **DeadLetterEvent**: Captured malformed events for debugging and audit (stored in table, not a queue)
- **AnalyticsReaderRole**: Database role for admin-service with SELECT-only privileges on analytics_db
- **LocationProvenance**: Tracking source of location data to ensure analytics accuracy

## References

- [Constitution](../../.specify/memory/constitution.md) - Single-writer analytics enforcement
- [Sprint 2 Spec](../003-gis-engine/spec.md) - GIS engine (dependency for geolocation in user profiles)
- [Keycloak Documentation](https://www.keycloak.org/documentation) - Authentication and JWT
- [PostgreSQL Documentation](https://www.postgresql.org/docs/) - Database operations
- [UUID v7 Specification](https://datatracker.ietf.org/doc/html/rfc9562) - Time-ordered UUID generation
