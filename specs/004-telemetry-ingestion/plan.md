# Implementation Plan: Telemetry Ingestion Core

**Branch**: `004-telemetry-ingestion` | **Date**: 2026-06-22 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/004-telemetry-ingestion/spec.md`

## Summary

Implement telemetry ingestion core with event validation, normalization, idempotency, and integration with analytics database. Ensure single-writer analytics enforcement (driver-service only) through database roles and CI gates. Features: event schema registry, event type governance (fixed enum), UUID v7 idempotency, location provenance, 30-day schema deprecation, dead-letter store, admin-service read-only API, and CI gates for telemetry rules enforcement.

## Technical Context

**Language/Version**: Rust 1.75+ (Cargo-based toolchain)

**Primary Dependencies**: 
- serde (serialization), serde_json
- sqlx (compile-time verification)
- actix-web (REST API)
- uuid (UUID v7 for idempotency)
- thiserror (error types)
- chrono (timestamp handling)
- rust_decimal (decimal precision for location data)

**Storage**: 
- PostgreSQL 16+ for analytics database
- analytics_db schema with analytics_events table and analytics_events_dead_letter table
- Event schemas in domain-types crate
- Event type enum for governance
- admin-service database role (SELECT-only)

**Testing**: cargo test (unit/integration), cargo clippy (linting), cargo fmt (formatting)

**Target Platform**: Linux server (driver-service), Docker containers (PostgreSQL 16+)

**Project Type**: Monorepo with microservices architecture

**Performance Goals**:
- Event ingestion: < 1 second from event reception to persistence
- Validation: < 100ms per event
- Idempotency check: < 10ms
- Enrichment: < 200ms per event
- Analytics query: < 500ms per paginated query
- CI gate validation: < 30 seconds

**Constraints**:
- Single-writer analytics enforcement (driver-service ONLY writes to analytics_db)
- Event schema validation mandatory (schema_version, user_id, timestamp, payload, event_type enum)
- Unknown/deprecated schema versions rejected immediately (no 30-day grace period)
- Idempotency must be enforced at database level using UUID v7
- No dynamic SQL construction (all queries via SQLx)
- Admin-service has SELECT-only database role (no write access)
- Location provenance required (EventLocation, SessionLocation, LastKnownLocation, DefaultLocation)
- Event type must use fixed enum (no free-form strings)

**Scale/Scope**: 18 must-have tasks across 5 phases (Setup, Foundational, User Stories, Cross-Cutting)

## Enforcement Kernel Specification

### CI Execution DAG

**Stage Order** (strict linear sequence with artifact passing):

```
Stage 1: format_check
  ↓ Passes
  artifact: {json_output}

Stage 2: type_check
  ↓ Passes, consumes format_check artifact
  artifact: {json_output}

Stage 3: dependency_graph_validation
  ↓ Passes, consumes type_check artifact
  artifact: {json_output}

Stage 4: identity_validation
  ↓ Passes, consumes dependency_graph_validation artifact
  artifact: {json_output}

Stage 5: schema_validation
  ↓ Passes, consumes identity_validation artifact
  artifact: {json_output}

Stage 6: sqlx_compile_check
  ↓ Passes, consumes schema_validation artifact
  artifact: {json_output}

Stage 7: analytics_write_gate
  ↓ Passes, consumes sqlx_compile_check artifact
  artifact: {json_output}

Stage 8: ci_gate_analytics_write
  ↓ Passes, consumes analytics_write_gate artifact
  artifact: {json_output}

Stage 9: ci_gate_event_schema
  ↓ Passes, consumes ci_gate_analytics_write artifact
  artifact: {json_output}

Stage 10: ci_gate_idempotency
  ↓ Passes, consumes ci_gate_event_schema artifact
  artifact: {json_output}

Stage 11: ci_gate_telemetry_routing
  ↓ Passes, consumes ci_gate_idempotency artifact
  artifact: {json_output}

Stage 12: ci_gate_payload_structure
  ↓ Passes, consumes ci_gate_telemetry_routing artifact
  artifact: {json_output}

Stage 13: integration_tests
  ↓ Passes, consumes ci_gate_payload_structure artifact
  artifact: {json_output}

Stage 14: build_success
  ↓ Passes, consumes integration_tests artifact
  artifact: {json_output}
```

**Failure Propagation Rules**:
- Hard-stop: Any stage failure immediately aborts all subsequent stages
- Deterministic exit codes: 0=success, 1=failure, 2=skipped
- No partial success allowed
- Each stage logs detailed failure reason to CI output

**Artifact Passing Model**:
- Each stage produces strict JSON artifact on success
- Next stage consumes previous artifact as input
- No side effects between stages
- All artifacts stored in `.specify/ci-artifacts/` for audit trail

### Enforcement Validator Specifications

#### 1. analytics_write_gate

**Input**: Git diff and codebase analysis

**Algorithm**:
- Scan for services attempting to write to analytics_db schema
- Check database role assignments
- Verify only driver-service has write access
- Validate SQLx queries targeting analytics_db

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "No unauthorized writes to analytics_db detected"
}
```

**Failure Signature**: Exit code 1 with list of violations

---

#### 2. ci_gate_event_schema

**Input**: API contracts, domain-types crate, code analysis

**Algorithm**:
- Validate schema_version presence in event contracts
- Verify schema_version matches known versions only (reject unknown)
- Verify schema_version not deprecated (reject > 30 days old)
- Verify user_id validation as UUID format
- Check timestamp validation (ISO 8601)
- Validate payload structure (JSON object, required fields)
- Validate event_type matches EventType enum (no free-form strings)
- Validate location_source enum (EventLocation, SessionLocation, LastKnownLocation, DefaultLocation)

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Event schema validation enforced with version governance"
}
```

**Failure Signature**: Exit code 1 with schema violations (including unknown/deprecated versions)

---

#### 3. ci_gate_idempotency

**Input**: Code analysis for idempotency_key handling

**Algorithm**:
- Check for idempotency_key field in event ingestion
- Verify unique index on idempotency_key in analytics_events table
- Verify duplicate rejection logic exists
- Check for idempotency logging

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Idempotency enforcement validated"
}
```

**Failure Signature**: Exit code 1 with idempotency gaps

---

#### 4. ci_gate_telemetry_routing

**Input**: Traefik configuration, service structure

**Algorithm**:
- Verify telemetry endpoints exist only in driver-service
- Validate routing rules (telemetry → driver-service only)
- Check authentication enforcement (JWT required)
- Verify 403 Forbidden for other services

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Telemetry routing enforced to driver-service"
}
```

**Failure Signature**: Exit code 1 with routing violations

---

#### 5. ci_gate_payload_structure

**Input**: Code analysis for payload validation

**Algorithm**:
- Check for JSON payload validation
- Verify payload is object type
- Validate nested field types
- Check error handling for malformed payloads

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Payload structure validation enforced"
}
```

**Failure Signature**: Exit code 1 with payload validation gaps

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Gate 1: Single-Writer Analytics ({status})

**Constitution Requirement**: driver-service ONLY can write to analytics_db

**Compliance Status**: ✅ PASS

**Justification**: Implementation plan enforces single-writer analytics through database role permissions (bornemap_analytics_writer only for driver-service), CI analytics write gate, and routing rules

**Verification**:
- Database schema: analytics_db write access restricted to driver-service
- CI gate: Fails if any service other than driver-service attempts write
- Routing: Traefik routes telemetry only to driver-service
- Audit: All analytics writes logged and tracked

---

## Project Structure

### Documentation (this feature)

```text
specs/004-telemetry-ingestion/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── tasks.md             # Phase 2 output
└── checklists/
    └── requirements.md  # Quality checklist
```

### Source Code (repository root)

```text
services/driver-service/
├── src/
│   ├── api/telemetry.rs  # Telemetry ingestion endpoint
│   ├── db/analytics.rs  # Analytics database writer
│   ├── domain/types/events.rs  # Event schemas and event type enum
│   ├── middleware/validation.rs  # Event validation layer (including schema version governance)
│   ├── middleware/enrichment.rs  # Event enrichment with location provenance
│   ├── middleware/idempotency.rs  # Idempotency system (UUID v7)
│   └── handlers/telemetry.rs  # Telemetry handlers
├── migrations/
│   ├── 0005_analytics_events.up.sql  # Event table with idempotency_key unique index
│   ├── 0005_analytics_events.down.sql
│   ├── 0006_analytics_events_dead_letter.up.sql  # Dead letter table
│   └── 0006_analytics_events_dead_letter.down.sql
└── Cargo.toml  # Updated with new dependencies

apps/packages/domain-types/src/
└── events.rs  # Event schemas, EventType enum, LocationSource enum

tools/
├── ci_gate_analytics_write.sh  # Analytics write gate (driver-service only)
├── ci_gate_event_schema.sh  # Event schema validation gate (including version governance)
├── ci_gate_idempotency.sh  # Idempotency gate (UUID v7)
├── ci_gate_telemetry_routing.sh  # Telemetry routing gate (driver-service only)
└── ci_gate_payload_structure.sh  # Payload structure gate
```

**Structure Decision**: 
- Use existing domain-types crate for event schema registry and EventType enum (no free-form strings)
- Implement new telemetry modules in driver-service (no cross-service imports)
- No frontend SDK (telemetry events sent from auth-service, driver-service, inventory-service)
- CI gates in tools/ (executable scripts)
- No admin-service write access to analytics_db

## Complexity Tracking

### Enforcement Kernel Complexity

The enforcement kernel introduces complexity to ensure constitutional compliance:

| Complexity Component | Why Needed | Simpler Alternative Rejected Because |
|---------------------|------------|-------------------------------------|
| Database roles (bornemap_analytics_writer) | Enforces single-writer at DB level | Direct API calls (violates constitution) |
| CI analytics write gate | Prevents code-level violations | Manual code review (error-prone) |
| Event schema validation | Ensures data quality with governance | No validation (data corruption risk) |
| Event type enum governance | Prevents free-form strings | Free-form strings (typos, inconsistency) |
| Idempotency enforcement with UUID v7 | Prevents duplicate events, ensures time-ordering | No idempotency (duplicate metrics) |
| Location provenance tracking | Ensures analytics accuracy | No provenance (ambiguous location data) |
| Schema version governance (reject unknown/deprecated) | Ensures data quality | Grace period (inconsistent data) |
| Admin-service read-only role | Prevents write access violations | No role enforcement (data corruption risk) |

---

## Phase 0: Research & Clarifications

**Purpose**: Resolve all technical unknowns and clarify implementation approach

**Duration**: 2-3 hours

**Output**: research.md document

### Research Tasks

**R-TELE-1: Event Type Governance**
- **Task**: Research event type governance patterns
- **Goal**: Determine best approach (fixed enum vs free-form strings)
- **Alternatives**: Free-form strings, free-form with validation, fixed enum

**R-TELE-2: Idempotency Key Generation with UUID v7**
- **Task**: Research UUID v7 (RFC 9562) for idempotency keys
- **Goal**: Determine UUID v7 vs UUID v4 vs SHA256(producer_id + event_id)
- **Alternatives**: UUID v4 (random), SHA256 hash, timestamp-based

**R-TELE-3: Location Provenance Strategy**
- **Task**: Research location provenance patterns and requirements
- **Goal**: Determine best location_source values and fallback behavior
- **Alternatives**: No provenance, single source, multiple sources

**R-TELE-4: Schema Version Governance (Immediate Rejection)**
- **Task**: Research schema version rejection policies
- **Goal**: Determine best approach (immediate rejection vs grace period vs mark-only)
- **Alternatives**: Grace period, mark-only, no validation

**R-TELE-5: Dead-Letter Store Implementation**
- **Task**: Research dead-letter store patterns
- **Goal**: Determine implementation approach (table, separate database, separate service)
- **Alternatives**: Same table, separate table, separate database, event bus

**R-TELE-6: Analytics Query Flow Through Admin-Service**
- **Task**: Research read-only API patterns and database roles
- **Goal**: Determine best approach for analytics queries
- **Alternatives**: Direct DB access, admin-service with role, read-only service

### Resolved Clarifications

**Question 1**: Event type governance

**Answer**: Use fixed enum with validation (AUTH_LOGIN, AUTH_LOGOUT, TOKEN_REFRESH, LOCATION_UPDATE, SESSION_START, SESSION_END, DRIVER_STATUS, INVENTORY_UPDATE, PRICE_CHANGE, STOCK_ALERT, ERROR_UNHANDLED). Benefits:
- Type safety
- Impossible to typos
- Easy to add new types through code changes
- No free-form strings

**Question 2**: Idempotency key generation

**Answer**: Use UUID v7 (RFC 9562) for idempotency_key. Benefits:
- Time-ordered (good for analytics ordering)
- Globally unique with high probability
- Simple implementation (no hashing)
- No schema_version needed (event_id already unique)
- Compatible with PostgreSQL 13+ and all modern UUID libraries

**Question 3**: Location provenance strategy

**Answer**: Use required location_source enum (EventLocation, SessionLocation, LastKnownLocation, DefaultLocation). Benefits:
- Ensures analytics accuracy
- Clear source of location data
- Can trace data lineage
- Required field prevents missing location context

**Question 4**: Schema version governance

**Answer**: Unknown and deprecated schema versions rejected immediately (no 30-day grace period). Benefits:
- Ensures data quality
- Prevents inconsistent data
- Simpler implementation
- Clear validation rules

**Question 5**: Dead-letter store implementation

**Answer**: Use dedicated analytics_events_dead_letter table in analytics_db. Benefits:
- Separate from valid events (no impact on queries)
- Easy querying and analysis
- Can be processed asynchronously
- Supports retry workflows
- Simple implementation (no message queue complexity)

**Question 6**: Analytics query flow

**Answer**: Analytics queries through admin-service with SELECT-only database role (bornemap_analytics_reader). Benefits:
- Enforces single-writer architecture
- Clear separation of concerns
- Can add query optimization
- Supports authentication/authorization
- No direct database access

---

## Phase 1: Design & Contracts

**Purpose**: Define data models, contracts, and implementation approach

**Duration**: 2-3 days

**Output**: data-model.md, contracts/, quickstart.md

### Data Model Design

**Input**: Feature spec requirements → `data-model.md`

**Entities**:
1. **TelemetryEvent** - Core event structure with schema_version, user_id (UUID), timestamp, event_type (enum), payload (JSON), idempotency_key (UUID v7), enriched_metadata
2. **EventEnrichment** - Additional metadata with location provenance
3. **LocationMetadata** - Location data with provenance (EventLocation, SessionLocation, LastKnownLocation, DefaultLocation)
4. **SessionMetadata** - Session context (session_start, session_duration, last_activity)
5. **RoleMetadata** - Role context from JWT (driver, partner, admin)
6. **SystemMetadata** - System context (service_name, event_source)
7. **EventSchema** - Schema registry definitions
8. **AnalyticsEvent** - Persisted events in database
9. **DeadLetterEvent** - Malformed events for debugging

**Relationships**:
- Event → LocationMetadata (1:1 with provenance)
- Event → SessionMetadata (1:1)
- Event → RoleMetadata (1:1)
- Event → SystemMetadata (1:1)
- Event → EventEnrichment (aggregate of all metadata)
- Event → AnalyticsEvent (after normalization)
- Event → DeadLetterEvent (if validation fails)

**Validation Rules**:
- schema_version: Required, matches known versions only (reject unknown), not deprecated (> 30 days old)
- user_id: Required, must be valid UUID
- timestamp: Required, ISO 8601 format
- payload: Required, valid JSON object
- idempotency_key: Required, UUID v7 (time-ordered, globally unique)
- event_type: Required, EventType enum (no free-form strings)
- location_source: Required, LocationSource enum (EventLocation, SessionLocation, LastKnownLocation, DefaultLocation)

### Contracts Definition

**Input**: Feature spec contracts section → `/contracts/` directory

**Contracts to Create**:

1. **events.schema** - Event schema definitions and EventType enum
   - Defines schemas for auth, location, session, error event types
   - Specifies required fields, validation rules
   - Defines EventType enum values
   - Defines LocationSource enum (EventLocation, SessionLocation, LastKnownLocation, DefaultLocation)

2. **ingestion.contract** - Ingestion API contract
   - POST /api/v1/telemetry/events (driver-service ingestion endpoint)
   - Request/response formats
   - Error codes and messages
   - Idempotency response (UUID v7)

3. **analytics-contracts.md** - Admin-service read API
   - GET /api/v1/analytics/events
   - Query parameters: user_id, start_time, end_time, schema_version, event_type, page_number, page_size
   - Response: paginated list of events with total_count, total_pages
   - Error handling: 403 Forbidden on write attempts

### Quickstart Guide

**Input**: Feature spec assumptions and common use cases → `quickstart.md`

**Sections**:
1. Prerequisites (PostgreSQL 16+, Keycloak JWT)
2. Quick start (5-minute setup)
3. Common use cases (auth-service, driver-service, inventory-service sending events)
4. Integration examples
5. Troubleshooting

### Implementation Strategy

**Phase 1 - Foundation**:
- Create database migrations for analytics_events and analytics_events_dead_letter tables
- Implement event schema registry in domain-types (EventType enum, LocationSource enum)
- Implement event validation layer with schema version governance (reject unknown/deprecated)
- Create telemetry ingestion endpoint in driver-service
- Configure Traefik telemetry routing (driver-service only)

**Phase 2 - Core Functionality**:
- Implement event normalization pipeline
- Implement UUID v7 idempotency system with unique index
- Implement event enrichment with location provenance
- Implement dead-letter logging
- Create admin-service database role with SELECT-only privileges

**Phase 3 - Analytics & Admin**:
- Implement analytics query handler in admin-service
- Implement filtering and pagination
- Create analytics database queries module
- Implement database role enforcement (bornemap_analytics_reader)

**Phase 4 - Polish**:
- Add comprehensive error handling
- Add integration tests
- Update documentation

---

## Phase 2: Implementation

**Purpose**: Implement all features according to plan

**Duration**: 3-5 days

**Tasks**: 18 must-have tasks from tasks.md

**Execution Flow**:
1. Database migrations
2. Event schemas and enums (domain-types)
3. Core ingestion endpoint in driver-service
4. Validation layer with schema version governance
5. UUID v7 idempotency system
6. Event enrichment with location provenance
7. Dead-letter logging
8. Admin-service read-only API
9. Database role enforcement
10. CI gates
11. Documentation and testing

**Checkpoint**: After Phase 2, event ingestion fully functional with idempotency, validation, and admin-service read API

---

## Phase 3: Testing & Validation

**Purpose**: Verify all functionality and document results

**Duration**: 2 days

**Output**: 
- Integration tests
- Documentation updates
- CI gate verification
- Performance benchmarks

**Test Coverage**:
- Unit tests: 100% for all new modules
- Integration tests: All endpoints and pipelines
- E2E tests: Full event flow from SDK to database
- Performance tests: Throughput and latency targets

---

## Exit Criteria

**Phase 1 Complete**:
- Database schemas exist with analytics_events and analytics_events_dead_letter tables
- Event schemas defined in domain-types (EventType enum, LocationSource enum)
- Telemetry ingestion endpoint implemented
- Event validation layer implemented with schema version governance

**Phase 2 Complete**:
- Event normalization pipeline functional
- UUID v7 idempotency system working (duplicate detection)
- Event enrichment automatic with location provenance
- Admin-service read-only API functional
- Database role enforcement (SELECT-only)
- CI gates implemented and passing

**Phase 3 Complete**:
- All integration tests passing
- Documentation complete
- Performance targets met
- All exit criteria verified

**Sprint Complete**:
- ✅ All events flow through driver-service ingestion endpoint (from auth-service, driver-service, inventory-service)
- ✅ No external writes to analytics_db (single-writer enforcement)
- ✅ Schema validation enforced in CI (unknown/deprecated versions rejected)
- ✅ Idempotency guaranteed (UUID v7 with unique index)
- ✅ Event type enum enforced (no free-form strings)
- ✅ Location provenance required (EventLocation, SessionLocation, LastKnownLocation, DefaultLocation)
- ✅ Admin-service read API functional with SELECT-only database role

---

## Success Metrics

- 100% event ingestion success rate
- < 1 second event latency
- 100% idempotency enforcement (UUID v7)
- 100% event type enum enforcement (no free-form strings)
- 100% location provenance enforcement
- CI gates passing (5 gates)
- Integration tests passing (100% coverage)
- Admin-service has no write access to analytics_db (403 Forbidden on write attempts)
