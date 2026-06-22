# Feature Specification: Telemetry Ingestion Core

**Feature Branch**: `004-telemetry-ingestion`

**Created**: 2026-06-22

**Status**: Draft

**Input**: Implement telemetry ingestion core with event validation, normalization, idempotency, and integration with analytics database. Ensure single-writer analytics enforcement (driver-service only).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Driver Sends Telemetry Events

**Goal**: Driver service receives and processes telemetry events from various services (auth, driver-service itself, inventory), validates them against schema, and persists to analytics database with idempotency.

**Why this priority**: Core functionality for all analytics and monitoring needs.

**Independent Test**: Send valid event to `POST /api/v1/telemetry/events`, verify event appears in analytics database, check that duplicate events are rejected with idempotency key.

**Acceptance Scenarios**:

1. **Given** a driver-service sends a valid telemetry event with schema_version, user_id (UUID), and payload, **When** the event is ingested, **Then** the event is validated, enriched with metadata, and persisted to analytics_events table

2. **Given** a malformed event (missing schema_version, invalid user_id, or invalid timestamp), **When** the event is ingested, **Then** the event is rejected and logged to dead-letter queue

3. **Given** the same event is sent twice with matching idempotency_key, **When** the second ingestion is attempted, **Then** the duplicate is rejected (idempotency guaranteed)

### User Story 2 - Analytics Team Queries Telemetry Events

**Goal**: Analytics team can query and analyze telemetry events from analytics database.

**Why this priority**: Enables monitoring, debugging, and business intelligence.

**Independent Test**: Query events from analytics database using filters, verify results match expected criteria.

**Acceptance Scenarios**:

1. **Given** analytics team queries for events by user_id, **When** the query is executed, **Then** results are returned filtered by user_id with pagination support

2. **Given** analytics team queries for events within a date range, **When** the query is executed, **Then** results are filtered by timestamp range

3. **Given** analytics team queries for events by schema_version, **When** the query is executed, **Then** results are filtered by schema_version

### User Story 3 - CI Gates Enforce Telemetry Rules

**Goal**: CI pipeline enforces telemetry governance rules (analytics write isolation, schema validation, idempotency, routing).

**Why this priority**: Ensures system-wide telemetry integrity and prevents violations.

**Independent Test**: Try to write to analytics_db from a service other than driver-service, verify CI gate fails; try to send malformed event, verify schema validation fails.

**Acceptance Scenarios**:

1. **Given** any service other than driver-service attempts to write to analytics_db, **When** the CI gate runs, **Then** the gate fails and prevents commit

2. **Given** an event without schema_version is sent, **When** the event schema validation runs, **Then** the event is rejected

3. **Given** a duplicate event is ingested, **When** the idempotency gate runs, **Then** the duplicate detection fails

## Requirements *(mandatory)*

### Functional Requirements

- **FR-TELE-001**: Telemetry ingestion endpoint must accept POST requests at `/api/v1/telemetry/events` from driver-service only, validate event schema (schema_version, timestamp, user_id, payload), reject malformed events with detailed error messages

- **FR-TELE-002**: Telemetry event validation layer must enforce schema_version matching known versions, validate user_id as UUID, validate timestamp as valid ISO 8601 format, validate payload as valid JSON object

- **FR-TELE-003**: Event normalization pipeline must validate event structure, enrich events with geolocation metadata (if available), enrich events with session metadata (duration, source), deduplicate events using idempotency_key, persist validated events to analytics_events table

- **FR-TELE-004**: Idempotency system must generate idempotency_key using hash of event_id + schema_version, create unique index on idempotency_key in analytics_events table, reject duplicate events with matching idempotency_key, log duplicate events for audit

- **FR-TELE-005**: Event enrichment must add geolocation metadata from user's current location (latitude, longitude, country, city) from user profile, add session metadata (session_start, session_duration, last_activity), add role context (driver, partner, admin), add system metadata (service_name, event_source)

- **FR-TELE-006**: Event schema registry in domain-types must define telemetry event structure (schema_version, user_id, timestamp, event_type, payload), ensure contracts are contract-first and framework-agnostic, separate schemas for different event types (auth, location, session, error)

- **FR-TELE-007**: Frontend telemetry SDK in client-core must provide event emitter with automatic retry on failure, batch events for efficiency, generate idempotency_key for events, include automatic error reporting

- **FR-TELE-008**: Telemetry routing must route all `/api/v1/telemetry/*` requests to driver-service only, verify request comes from driver-service via JWT authentication, return 403 Forbidden for other services

- **FR-TELE-009**: Dead-letter logging must capture malformed events with full event payload, capture error details (validation failure, schema mismatch), log to dedicated dead-letter table, include timestamp and metadata for debugging

- **FR-TELE-010**: Analytics database writer must be implemented in driver-service only, restrict write access to analytics_db schema to driver-service only, prevent any service from writing directly to analytics_db, ensure analytics_db is read-only for other services

### Key Entities *(include if feature involves data)*

- **TelemetryEvent**: Core event structure with schema_version, user_id (UUID), timestamp, event_type, payload (JSON), idempotency_key, enriched_metadata (geolocation, session, role, system), status (pending, processed, failed)

- **EventEnrichment**: Additional metadata attached to events including geolocation (latitude, longitude, country, city), session metadata (session_start, duration, last_activity), role context (driver, partner, admin), system metadata (service_name, event_source)

- **EventSchema**: Schema registry entries defining telemetry event structure including schema_version, required fields, field types, validation rules, supported event types

- **AnalyticsEvent**: Persisted event in analytics_events table with all enriched fields, status tracking, duplicate detection index on idempotency_key, created_at and updated_at timestamps

- **DeadLetterEvent**: Captured malformed events with full payload, error details, error_type, timestamp, original_request_id, retry_attempts, schema violations

- **TelemetryRouting**: Configuration rules for routing telemetry requests, source service whitelist, destination service (driver-service), routing status (enabled/disabled)

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All telemetry events sent from driver-service are successfully ingested and appear in analytics_events table within 1 second

- **SC-002**: Events with missing schema_version, invalid user_id (non-UUID), or invalid timestamp are rejected with 400 Bad Request status and detailed error messages

- **SC-003**: Duplicate events with matching idempotency_key are rejected and marked as duplicate (idempotency guaranteed)

- **SC-004**: Analytics write gate in CI pipeline fails when any service other than driver-service attempts to write to analytics_db

- **SC-005**: Schema validation gate in CI pipeline fails when events are sent without schema_version or with invalid field types

- **SC-006**: Idempotency gate in CI pipeline detects duplicate event ingestion attempts

- **SC-007**: Telemetry routing gate ensures only driver-service can access telemetry endpoints

- **SC-008**: Events are enriched with geolocation, session, role, and system metadata automatically

- **SC-009**: 100% of telemetry events from driver-service pass validation and idempotency checks

- **SC-010**: Frontend telemetry SDK provides automatic retry on failure and batch support

## Assumptions

- Telemetry events are batched by frontend services before sending to reduce network overhead
- Geolocation data is available from user profile, fallback to default if unavailable
- Session metadata is tracked by authentication system and attached to events
- Idempotency_key is generated by frontend SDK, can be overridden by backend
- Event schemas evolve with new versions, old schemas are deprecated but supported for 30 days
- Analytics database is maintained as read-only for all services except driver-service
- Dead-letter queue is implemented as separate analytics_events_deadletter table
- CI gates are executed on every commit and PR

## Out of Scope (Explicitly Excluded)

1. Real-time telemetry streaming (events are batched)
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
  - Sprint 1: Keycloak authentication system must be complete
  - Sprint 2: GIS engine must be complete (for geolocation enrichment)
  - Domain-types crate: Event schemas must be defined in domain-types before implementation

- **External Dependencies**:
  - PostgreSQL 16+ for analytics database
  - Redis for caching (optional, for performance optimization)
  - PostgreSQL pg_cron extension for scheduled analytics queries

## Risks and Mitigations

### Risk R-TELE-1: Event Schema Validation Failures
**Risk**: Events with invalid schema could be accepted and cause data quality issues.
**Impact**: High - corrupt analytics data, incorrect monitoring.
**Mitigation**:
  - Enforce schema validation at ingestion time with strict rules
  - Use PostgreSQL ENUMs for fixed schema_version values
  - Log all validation failures for debugging
  - Add CI gate for schema validation

### Risk R-TELE-2: Duplicate Event Ingestion
**Risk**: Same event sent multiple times could corrupt analytics data.
**Impact**: High - duplicate metrics, incorrect analytics.
**Mitigation**:
  - Enforce idempotency at database level with unique index
  - Use idempotency_key with hash of event_id + schema_version
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
  - Implement event batching at frontend SDK
  - Add Redis caching for frequent queries
  - Implement background processing for large volumes
  - Add query pagination and filtering

### Risk R-TELE-5: Geolocation Data Loss
**Risk**: Missing geolocation data could reduce event usefulness.
**Impact**: Low - events still usable without geolocation.
**Mitigation**:
  - Make geolocation optional with default values
  - Use user's last known location if unavailable
  - Log geolocation availability for analysis
  - Cache geolocation for session duration

## Test Strategy

### Unit Tests
- Event schema validation (missing fields, invalid types)
- Idempotency key generation
- Event enrichment logic
- Event deduplication logic
- Event normalization pipeline

### Integration Tests
- Telemetry ingestion endpoint
- Event schema validation
- Idempotency enforcement
- Database write operations
- CI gate validation

### E2E Tests
- Full event ingestion flow from frontend SDK
- End-to-end validation and persistence
- Error handling for malformed events

### Performance Tests
- Event ingestion throughput
- Validation performance
- Database write performance
- CI gate execution time

## Glossary

- **TelemetryEvent**: Data packet containing event information including schema_version, user_id, timestamp, event_type, payload, and enriched metadata
- **IdempotencyKey**: Hash of event_id + schema_version, used to detect and reject duplicate events
- **SchemaRegistry**: Centralized definition of event schemas and their versions
- **AnalyticsDB**: Database exclusively for analytics data, write-only for driver-service
- **EventEnrichment**: Additional metadata automatically added to events (geolocation, session, role, system)
- **DeadLetterEvent**: Captured malformed events for debugging and audit

## References

- [Constitution](../../.specify/memory/constitution.md) - Single-writer analytics enforcement
- [Sprint 2 Spec](../003-gis-engine/spec.md) - GIS engine (dependency for geolocation)
- [Keycloak Documentation](https://www.keycloak.org/documentation) - Authentication and JWT
- [PostgreSQL Documentation](https://www.postgresql.org/docs/) - Database operations
- [Schema Registry Pattern](https://microservices.io/patterns/data/schema-registry.html) - Schema versioning
