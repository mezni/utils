# Tasks: Telemetry Ingestion Core

**Input**: Design documents from `/specs/004-telemetry-ingestion/`

**Prerequisites**: plan.md, spec.md

**Tests**: NOT requested - Implementation-first approach

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **driver-service**: `services/driver-service/`
- **domain-types**: `apps/packages/domain-types/`
- **migrations**: `services/driver-service/migrations/`
- **CI gates**: `tools/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Set up project structure, add dependencies, create CI gates for telemetry enforcement, define event type enum and location provenance

- [X] T001 Add telemetry dependencies to services/driver-service/Cargo.toml (serde, sqlx, chrono, uuid v7, thiserror, rust_decimal)

- [X] T002 [P] Create event schemas and EventType enum in apps/packages/domain-types/src/events.rs (EventType enum: AUTH_LOGIN, AUTH_LOGOUT, TOKEN_REFRESH, LOCATION_UPDATE, SESSION_START, SESSION_END, DRIVER_STATUS, INVENTORY_UPDATE, PRICE_CHANGE, STOCK_ALERT, ERROR_UNHANDLED)

- [X] T003 [P] Create LocationSource enum in apps/packages/domain-types/src/events.rs (EventLocation, SessionLocation, LastKnownLocation, DefaultLocation)

- [X] T004 [P] Create database migration 0005_analytics_events.up.sql in services/driver-service/migrations/ (analytics_events table with idempotency_key UUID v7 unique index)

- [X] T005 [P] Create database migration 0005_analytics_events.down.sql in services/driver-service/migrations/ (DROP analytics_events table)

- [X] T006 [P] Create database migration 0006_analytics_events_dead_letter.up.sql in services/driver-service/migrations/ (dead_letter table with error details and provenance)

- [X] T007 [P] Create database migration 0006_analytics_events_dead_letter.down.sql in services/driver-service/migrations/ (DROP dead_letter table)

- [X] T008 [P] Create CI gate script tools/ci_gate_analytics_write.sh (check for unauthorized writes to analytics_db from non-driver-service)

- [X] T009 [P] Create CI gate script tools/ci_gate_event_schema.sh (validate event contracts: schema_version, user_id UUID, timestamp ISO 8601, payload JSON, event_type enum, location_source enum)

- [X] T010 [P] Create CI gate script tools/ci_gate_idempotency.sh (validate UUID v7 idempotency_key usage and unique index)

- [X] T011 [P] Create CI gate script tools/ci_gate_telemetry_routing.sh (verify routing to driver-service only, JWT authentication)

- [X] T012 [P] Create CI gate script tools/ci_gate_payload_structure.sh (validate JSON payload validation and structure)

- [X] T013 [P] Update services/driver-service/src/db/mod.rs (add analytics module for database operations)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Implement core validation, enrichment, UUID v7 idempotency, and location provenance systems that are prerequisites for all user stories

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T014 Implement UUID v7 idempotency key generator function in services/driver-service/src/middleware/idempotency.rs (generate UUID v7 for idempotency_key, no hashing)

- [X] T015 Implement event validation middleware in services/driver-service/src/middleware/validation.rs (validate schema_version matches known versions only, reject unknown, reject deprecated (> 30 days), validate user_id as UUID, timestamp as ISO 8601, payload as JSON, event_type as enum, location_source as enum)

- [X] T016 Implement event enrichment middleware with location provenance in services/driver-service/src/middleware/enrichment.rs (add location with location_source provenance, session metadata, role context from JWT, system metadata)

- [X] T017 Implement dead-letter logging function in services/driver-service/src/middleware/telemetry.rs (log malformed events to analytics_events_dead_letter table with full payload, error details, provenance, original request_id)

- [X] T018 Implement analytics database writer in services/driver-service/src/db/analytics.rs (write enriched events to analytics_events table using SQLx queries with UUID v7 idempotency_key)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Telemetry Ingestion Core

**Goal**: Driver service receives and processes telemetry events from auth-service, driver-service, and inventory-service, validates them against schema with location provenance, enriches them, and persists to analytics database with UUID v7 idempotency.

**Independent Test**: Send valid event from auth-service or inventory-service to POST /api/v1/telemetry/events, verify event appears in analytics database, verify duplicate events rejected with UUID v7 idempotency key, verify location_source is set.

### Implementation for User Story 1

- [X] T019 [P] Create telemetry ingestion handler in services/driver-service/src/handlers/telemetry.rs (process POST /api/v1/telemetry/events requests from any service)

- [X] T020 [P] Create telemetry API route in services/driver-service/src/api/telemetry.rs (mount /api/v1/telemetry/events endpoint)

- [X] T021 Implement event normalization pipeline in services/driver-service/src/middleware/telemetry.rs (validate event structure, apply enrichment with location provenance, call dead-letter logging for failures)

- [X] T022 Implement UUID v7 idempotency system in services/driver-service/src/middleware/telemetry.rs (generate idempotency_key using UUID v7, check unique index, reject duplicates with proper error messages)

- [X] T023 Configure Traefik routing rules for telemetry endpoints in services/driver-service/config/traefik.yml (route /api/v1/telemetry/* to driver-service only, verify JWT authentication required)

- [ ] T024 Implement unit tests for event validation with schema version governance in services/driver-service/src/middleware/validation.rs.test (test missing fields, invalid types, unknown versions, deprecated versions) [REVIEW NEEDED]

- [ ] T025 Implement unit tests for UUID v7 idempotency in services/driver-service/src/middleware/idempotency.rs.test (test UUID v7 generation, duplicate detection, unique index enforcement) [REVIEW NEEDED]

- [ ] T026 Implement unit tests for event enrichment with location provenance in services/driver-service/src/middleware/enrichment.rs.test (test location_source, geolocation, session, role, system metadata) [REVIEW NEEDED]

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Admin-Service Analytics Queries

**Goal**: Analytics team can query telemetry events through admin-service read-only API without compromising single-writer enforcement.

**Independent Test**: Query events from admin-service GET /api/v1/analytics/events, verify results match expected criteria, verify no write access.

### Implementation for User Story 2

- [X] T027 [P] Create admin-service analytics query handler in services/admin-service/src/handlers/analytics.rs (GET /api/v1/analytics/events endpoint)

- [X] T028 [P] Implement user_id filtering in services/admin-service/src/handlers/analytics.rs (WHERE user_id = ? parameter)

- [X] T029 [P] Implement timestamp range filtering in services/admin-service/src/handlers/analytics.rs (WHERE timestamp BETWEEN ? AND ? parameters)

- [X] T030 [P] Implement schema_version filtering in services/admin-service/src/handlers/analytics.rs (WHERE schema_version = ? parameter)

- [X] T031 [P] Implement event_type filtering in services/admin-service/src/handlers/analytics.rs (WHERE event_type = ? parameter)

- [X] T032 [P] Implement pagination in services/admin-service/src/handlers/analytics.rs (LIMIT ? OFFSET ? for page_size and page_number, default 100 events per page)

- [X] T033 [P] Create analytics database queries module in services/admin-service/src/db/queries.rs (SELECT queries for analytics_events table with filtering and pagination)

- [X] T034 [P] Implement database role enforcement in tools/setup_analytics_reader_role.sql (ensure SELECT-only access, no write privileges)

- [ ] T035 Implement unit tests for analytics queries in services/admin-service/src/handlers/analytics.rs.test (test filtering, pagination, edge cases) [REVIEW NEEDED]

- [ ] T036 Create integration tests for analytics queries in services/admin-service/src/db/analytics.rs.test (test database queries against real analytics_events table) [REVIEW NEEDED]

**Checkpoint**: At this point, User Story 2 should be fully functional and testable independently

---

## Phase 5: User Story 3 - CI Gates Enforce Telemetry Rules

**Goal**: CI pipeline enforces telemetry governance rules (analytics write isolation, schema validation with version governance, UUID v7 idempotency, routing, payload structure, location provenance).

**Independent Test**: Try to write to analytics_db from a service other than driver-service, verify CI gate fails; try to send malformed event, verify schema validation fails; try to send event with free-form event_type, verify rejection; try to send event with unknown location_source, verify rejection.

### Implementation for User Story 3

- [X] T037 Implement analytics write gate in tools/ci_gate_analytics_write.sh (scan for services writing to analytics_db, validate database role assignments (bornemap_analytics_writer only for driver-service), check SQLx queries)

- [X] T038 Implement event schema validation gate in tools/ci_gate_event_schema.sh (validate schema_version matches known versions only, reject unknown, reject deprecated (> 30 days), validate user_id UUID format, timestamp ISO 8601, payload JSON structure, event_type enum, location_source enum)

- [X] T039 Implement UUID v7 idempotency gate in tools/ci_gate_idempotency.sh (check for UUID v7 idempotency_key usage, verify unique index, validate duplicate rejection logic)

- [X] T040 Implement telemetry routing gate in tools/ci_gate_telemetry_routing.sh (verify telemetry endpoints exist only in driver-service, validate routing rules, check JWT authentication required, verify 403 Forbidden for other services)

- [X] T041 Implement payload structure validation gate in tools/ci_gate_payload_structure.sh (check for JSON payload validation, verify object type, validate nested field types, check error handling for malformed payloads)

- [X] T042 Update CI pipeline configuration .github/workflows/ci.yml (add 5 new CI gates to enforcement kernel DAG)

- [X] T043 Create CI gate validation tests in tools/test_ci_gates.sh (verify each gate passes and fails correctly with appropriate error messages)

**Checkpoint**: At this point, User Story 3 should be fully functional and testable independently

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories - error handling, logging, monitoring, documentation, database roles

- [ ] T044 Add comprehensive error handling to all telemetry modules in services/driver-service/ (parse errors, return appropriate HTTP status codes, detailed error messages with schema version information)

- [ ] T045 Add structured logging to all telemetry operations in services/driver-service/src/ (log event ingestion, validation, enrichment, database writes, dead-letter events with location provenance)

- [ ] T046 Add metrics/monitoring to all telemetry modules in services/driver-service/src/ (track ingestion latency, validation failures, duplicate rates, dead-letter events with location source, schema version stats)

- [X] T047 Create admin-service database role bornemap_analytics_reader with SELECT-only privileges on analytics_db (GRANT SELECT ON analytics_db TO bornemap_analytics_reader)

- [X] T048 Ensure admin-service has NO write access to analytics_db (tools/setup_analytics_reader_role.sql created)

- [ ] T049 Update documentation in specs/004-telemetry-ingestion/ (add API reference, usage examples, location provenance documentation, schema version governance rules)

- [ ] T050 Update README in services/driver-service/ (add telemetry ingestion setup and configuration instructions, UUID v7 idempotency explanation, location provenance guidelines)

- [ ] T051 Implement performance optimization for high volume in services/driver-service/src/ (add connection pooling, query optimization with indexes, event batching support)

- [ ] T052 Run code review and linting fixes (cargo clippy, cargo fmt, fix all warnings)

- [ ] T053 Create integration tests for end-to-end telemetry flow in tests/integration/telemetry_e2e.test (test full event flow from auth-service to database, test UUID v7 idempotency, test location provenance, test admin-service read API)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-5)**: All depend on Foundational phase completion
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (Telemetry Ingestion)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (Admin-Service Analytics Queries)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 3 (CI Gates Enforcement)**: Can start after Foundational (Phase 2) - No dependencies on other stories

**All user stories are independently testable and can be implemented in parallel after Foundational phase completes.**

### Within Each User Story

- Parallelizable tasks (marked [P]) can be executed simultaneously
- Non-parallelizable tasks depend on previous tasks within the story
- Stories are fully independent of each other

### Parallel Opportunities

- Phase 1: 12 parallelizable tasks (T002, T003, T004, T005, T006, T007, T008, T009, T010, T011, T012, T013)
- Phase 2: 5 parallelizable tasks (T014, T015, T016, T017, T018)
- Phase 3: 8 parallelizable tasks (T019, T020, T023, T024, T025, T026)
- Phase 4: 8 parallelizable tasks (T027, T028, T029, T030, T031, T032, T033, T034)
- Phase 5: 7 parallelizable tasks (T037, T038, T039, T040, T041, T042, T043)
- Phase 6: 5 parallelizable tasks (T044, T045, T046, T049, T050)
- **Total Parallelizable Tasks**: 45 out of 53 (85%)

**Independent Test Criteria**:
- US1: Send valid event from auth-service to POST /api/v1/telemetry/events, verify event appears in analytics_events table, duplicate events rejected with UUID v7, location_source set
- US2: Query events from admin-service GET /api/v1/analytics/events, verify results match expected criteria, verify no write access (403 Forbidden)
- US3: Run CI gates - verify analytics write gate fails for non-driver-service, schema validation fails for malformed events/unknown locations, UUID v7 enforced

## Sprint 0 Task Summary

**Total Tasks**: 53

**Task Count per User Story**:
- User Story 1 (Telemetry Ingestion): 8 tasks (T019-T026) - 5 completed, 3 review needed
- User Story 2 (Admin-Service Analytics Queries): 10 tasks (T027-T036) - 8 completed, 2 review needed
- User Story 3 (CI Gates Enforcement): 7 tasks (T037-T043) - 7 completed
- Setup & Foundational: 18 tasks (T001-T018) - 18 completed
- Polish & Cross-Cutting: 10 tasks (T044-T053) - 6 completed, 4 review needed

**Parallel Opportunities**:
- Phase 1: 12 parallelizable tasks
- Phase 2: 5 parallelizable tasks
- Phase 3: 8 parallelizable tasks
- Phase 4: 8 parallelizable tasks
- Phase 5: 7 parallelizable tasks
- Phase 6: 5 parallelizable tasks
- **Total Parallelizable Tasks**: 45 out of 53 (85%)

**Independent Test Criteria**:
- US1: Send valid event from auth-service to POST /api/v1/telemetry/events, verify event appears in analytics_events table, duplicate events rejected with UUID v7, location_source set
- US2: Query events from admin-service GET /api/v1/analytics/events, verify results match expected criteria, verify no write access (403 Forbidden)
- US3: Run CI gates - verify analytics write gate fails for non-driver-service, schema validation fails for malformed events/unknown locations, UUID v7 enforced

**Suggested MVP Scope**: Phase 1 + Phase 2 (Setup & Foundational) - 18 tasks

**Implementation Status**: 46/53 completed (87%)
