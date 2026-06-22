# Implementation Plan: Telemetry Ingestion Core

**Branch**: `004-telemetry-ingestion` | **Date**: 2026-06-22 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/004-telemetry-ingestion/spec.md`

## Summary

Implement telemetry ingestion core with event validation, normalization, idempotency, and integration with analytics database. Ensure single-writer analytics enforcement (driver-service only) through database roles and CI gates. Features: event schema registry, frontend telemetry SDK, automatic enrichment, duplicate detection, dead-letter logging, and CI gates for telemetry rules enforcement.

## Technical Context

**Language/Version**: Rust 1.75+ (Cargo-based toolchain)

**Primary Dependencies**: 
- serde (serialization), serde_json
- sqlx (compile-time verification)
- actix-web (REST API)
- uuid (UUID validation)
- thiserror (error types)
- chrono (timestamp handling)
- hashbrown (idempotency key hashing)

**Storage**: 
- PostgreSQL 16+ for analytics database
- analytics_db schema with analytics_events table and dead_letter_events table
- Event schemas in domain-types crate
- Frontend telemetry SDK in client-core package

**Testing**: cargo test (unit/integration), cargo clippy (linting), cargo fmt (formatting)

**Target Platform**: Linux server (driver-service), Docker containers (PostgreSQL 16+), React Native app (client-core SDK)

**Project Type**: Monorepo with microservices architecture

**Performance Goals**:
- Event ingestion: < 1 second from event reception to persistence
- Validation: < 100ms per event
- Idempotency check: < 10ms
- Enrichment: < 200ms per event
- CI gate validation: < 30 seconds

**Constraints**:
- Single-writer analytics enforcement (driver-service only writes to analytics_db)
- Event schema validation mandatory (schema_version, user_id, timestamp, payload)
- Idempotency must be enforced at database level
- No dynamic SQL construction (all queries via SQLx)
- Frontend SDK must provide batching and retry

**Scale/Scope**: 10 must-have tasks, 6 should-have tasks, 2 nice-to-have tasks across 3 phases (Setup, Implementation, Polish)

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
- Verify user_id validation as UUID format
- Check timestamp validation (ISO 8601)
- Validate payload structure (JSON object, required fields)

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Event schema validation enforced in contracts"
}
```

**Failure Signature**: Exit code 1 with schema violations

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
│   ├── api/telemetry.rs # Telemetry ingestion endpoint
│   ├── db/analytics.rs  # Analytics database writer
│   ├── domain/types/events.rs  # Event schemas (new)
│   ├── middleware/validation.rs  # Event validation layer
│   ├── middleware/enrichment.rs  # Event enrichment
│   ├── middleware/idempotency.rs  # Idempotency system
│   └── handlers/telemetry.rs  # Telemetry handlers
├── migrations/
│   ├── 0005_analytics_events.up.sql  # Event table
│   └── 0005_analytics_events.down.sql
└── Cargo.toml  # Updated with new dependencies

apps/packages/domain-types/src/
└── events.rs  # Event schema registry

apps/packages/client-core/
└── src/telemetry/
    ├── sdk.rs  # Frontend telemetry SDK
    └── emitter.rs  # Event emitter

tools/
├── ci_gate_analytics_write.sh  # Analytics write gate
├── ci_gate_event_schema.sh  # Event schema validation gate
├── ci_gate_idempotency.sh  # Idempotency gate
├── ci_gate_telemetry_routing.sh  # Telemetry routing gate
└── ci_gate_payload_structure.sh  # Payload structure gate
```

**Structure Decision**: 
- Use existing domain-types crate for schema registry (contract-first)
- Implement new telemetry modules in driver-service (no cross-service imports)
- Frontend SDK in client-core (no backend imports)
- CI gates in tools/ (executable scripts)

## Complexity Tracking

### Enforcement Kernel Complexity

The enforcement kernel introduces complexity to ensure constitutional compliance:

| Complexity Component | Why Needed | Simpler Alternative Rejected Because |
|---------------------|------------|-------------------------------------|
| Database roles (bornemap_analytics_writer) | Enforces single-writer at DB level | Direct API calls (violates constitution) |
| CI analytics write gate | Prevents code-level violations | Manual code review (error-prone) |
| Event schema validation | Ensures data quality | No validation (data corruption risk) |
| Idempotency enforcement | Prevents duplicate events | No idempotency (duplicate metrics) |
| Traefik routing enforcement | Enforces telemetry routing | Manual configuration (configuration drift) |

---

## Phase 0: Research & Clarifications

**Purpose**: Resolve all technical unknowns and clarify implementation approach

**Duration**: 2-3 hours

**Output**: research.md document

### Research Tasks

**R-TELE-1: Event Schema Versioning Strategy**
- **Task**: Research event schema versioning patterns
- **Goal**: Determine versioning strategy (semantic versioning, backward compatibility, deprecation policy)
- **Alternatives**: Date-based versioning, descriptive versioning

**R-TELE-2: Idempotency Key Generation**
- **Task**: Research idempotency key generation patterns
- **Goal**: Determine best approach (hash of event_id + schema_version, UUID, custom algorithm)
- **Alternatives**: UUID-based, timestamp-based, sequence-based

**R-TELE-3: Event Enrichment Sources**
- **Task**: Research event enrichment patterns and sources
- **Goal**: Identify optimal data sources for geolocation, session, role metadata
- **Alternatives**: Fetch from API, cache at event time, process as part of ingestion

**R-TELE-4: Frontend SDK Retry Logic**
- **Task**: Research event batch and retry patterns for SDK
- **Goal**: Determine optimal batch size, retry strategies, backoff policies
- **Alternatives**: Single event, fixed batch size, adaptive batching

**R-TELE-5: Dead-Letter Queue Implementation**
- **Task**: Research dead-letter queue patterns
- **Goal**: Determine implementation approach (dedicated table, separate database, topic queue)
- **Alternatives**: Separate table in same DB, S3, Kafka topic

**R-TELE-6: Telemetry Performance**
- **Task**: Research event ingestion performance patterns
- **Goal**: Identify performance targets and optimization strategies
- **Alternatives**: Stream processing, batch processing, real-time vs delayed

### Resolved Clarifications

**Question 1**: Event schema versioning strategy

**Answer**: Use semantic versioning (e.g., "1.0.0") with automatic deprecation (30 days support for deprecated versions). This allows:
- Clear version tracking
- Backward compatibility for 30 days
- Gradual migration path for new features
- Version comparison for validation

**Question 2**: Idempotency key generation

**Answer**: Use hash of event_id (UUID) + schema_version using SHA256. This provides:
- Deterministic key generation
- Uniqueness guarantees
- No collisions
- Simple implementation

**Question 3**: Event enrichment sources

**Answer**: 
- Geolocation: Fetch from user profile (cached at auth time, TTL 30 minutes)
- Session metadata: Track in authentication system, attached to event
- Role context: Extract from JWT claims
- System metadata: Include from service identity

**Question 4**: Frontend SDK retry logic

**Answer**:
- Batch size: 10 events per batch (adjustable)
- Retry: 3 attempts with exponential backoff (1s, 2s, 4s)
- Backoff jitter: Add random jitter to prevent thundering herd
- Batch timeout: 5 seconds

**Question 5**: Dead-letter queue implementation

**Answer**: Use dedicated `analytics_events_deadletter` table in analytics_db. Benefits:
- Separate from valid events (no impact on queries)
- Easy querying and analysis
- Can be processed asynchronously
- Supports retry workflows

**Question 6**: Telemetry performance

**Answer**:
- Target: 1000 events/second ingestion
- Validation: < 100ms per event
- Idempotency: < 10ms per event
- Enrichment: < 200ms per event
- Batching: Frontend SDK batches, backend processes in batches

---

## Phase 1: Design & Contracts

**Purpose**: Define data models, contracts, and implementation approach

**Duration**: 2-3 days

**Output**: data-model.md, contracts/, quickstart.md

### Data Model Design

**Input**: Feature spec requirements → `data-model.md`

**Entities**:
1. **TelemetryEvent** - Core event structure
2. **EventEnrichment** - Additional metadata
3. **EventSchema** - Schema registry definitions
4. **AnalyticsEvent** - Persisted events in database
5. **DeadLetterEvent** - Malformed events for debugging

**Relationships**:
- Event → EventEnrichment (1:1)
- Event → AnalyticsEvent (after normalization)
- Event → DeadLetterEvent (if validation fails)

**Validation Rules**:
- schema_version: Required, matches known versions
- user_id: Required, must be valid UUID
- timestamp: Required, ISO 8601 format
- payload: Required, valid JSON object
- idempotency_key: Required, SHA256 hash
- event_type: Required, enum type (auth, location, session, error)

### Contracts Definition

**Input**: Feature spec contracts section → `/contracts/` directory

**Contracts to Create**:

1. **events.schema** - Event schema definitions
   - Defines schemas for auth, location, session, error event types
   - Specifies required fields and validation rules

2. **ingestion.contract** - Ingestion API contract
   - POST /api/v1/telemetry/events
   - Request/response formats
   - Error codes and messages
   - Idempotency response

3. **telemetry-sdk.contract** - Frontend SDK interface
   - TelemetryEmitter interface
   - Batch methods
   - Retry behavior
   - Error handling

4. **analytics-contracts.md** - Analytics queries
   - GET /api/v1/analytics/events
   - Filtering and pagination
   - Time range queries
   - Schema_version filtering

### Quickstart Guide

**Input**: Feature spec assumptions and common use cases → `quickstart.md`

**Sections**:
1. Prerequisites
2. Quick start (5-minute setup)
3. Common use cases
4. Integration examples
5. Troubleshooting

### Implementation Strategy

**Phase 1 - Foundation**:
- Create database migration for analytics_events and dead_letter_events tables
- Implement event schema registry in domain-types
- Create telemetry ingestion endpoint in driver-service
- Implement event validation layer

**Phase 2 - Core Functionality**:
- Implement event normalization pipeline
- Implement idempotency system with unique index
- Implement event enrichment logic
- Implement dead-letter logging

**Phase 3 - SDK & Routing**:
- Create frontend telemetry SDK in client-core
- Configure Traefik telemetry routing
- Implement CI gates for telemetry rules

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
2. Domain types (event schemas)
3. Core ingestion endpoint
4. Validation and idempotency
5. Enrichment logic
6. Frontend SDK
7. CI gates
8. Documentation and testing

**Checkpoint**: After Phase 2, event ingestion fully functional with idempotency and validation

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
- Database schemas exist with analytics_events and dead_letter_events tables
- Event schemas defined in domain-types
- Telemetry ingestion endpoint implemented
- Event validation layer implemented

**Phase 2 Complete**:
- Event normalization pipeline functional
- Idempotency system working (duplicate detection)
- Event enrichment automatic
- Frontend SDK provides batching and retry
- CI gates implemented and passing

**Phase 3 Complete**:
- All integration tests passing
- Documentation complete
- Performance targets met
- All exit criteria verified

**Sprint Complete**:
- ✅ All events flow through driver-service ingestion endpoint
- ✅ No external writes to analytics_db
- ✅ Schema validation enforced in CI
- ✅ Idempotency guaranteed
- ✅ Analytics gate passes
- ✅ Telemetry routing enforced

---

## Success Metrics

- 100% event ingestion success rate
- < 1 second event latency
- 100% idempotency enforcement
- CI gates passing (5 gates)
- Integration tests passing (100% coverage)
- Frontend SDK adoption rate (target: 80%+)
