# Implementation Plan: Admin Analytics Read Layer

**Branch**: 003-gis-engine | **Date**: 2026-06-22 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/005-admin-analytics/spec.md`

## Summary

Implement the admin analytics read layer where admin-service provides read-only analytics APIs for KPI aggregation, materialized views, and caching with synchronous cache invalidation from driver-service events. This completes the analytics pipeline with controlled read access while enforcing single-writer principle from Sprint 3.

## Technical Context

**Language/Version**: Rust 1.75+, TypeScript (frontend separate)
**Primary Dependencies**:
- actix-web 4.4 (API framework)
- sqlx 0.7 (compile-time verified PostgreSQL queries)
- serde, serde_json (serialization)
- redis (caching layer)
- Keycloak integration (authentication)
- Domain-types crate (contracts)

**Storage**:
- PostgreSQL 16+ (analytics_db with materialized views)
- Redis (aggregated analytics caching)

**Testing**: Rust unit tests, integration tests, E2E tests with Playwright

**Target Platform**: Microservices architecture (3 services: auth, driver, admin)

**Project Type**: Backend service with external API contracts

**Performance Goals**:
- Query latency: <500ms (cached), <500ms (uncached)
- Cache hit rate: 80%+
- Cache invalidation: <5 seconds
- Materialized view refresh: <1 minute

**Constraints**:
- Strict read-only enforcement for admin-service on analytics_db
- Single-writer enforcement maintained (driver-service only)
- Synchronous cache invalidation via HTTP callback
- All queries must derive from materialized views
- No dynamic SQL allowed (query safety gate)
- KPIs must be derived from telemetry events only
- CI gates mandatory: read-only enforcement, query safety, KPI integrity

**Scale/Scope**:
- 4 user stories (Admin Dashboard, Read-Only Enforcement, Analytics Aggregation, Partner Analytics)
- 8 functional requirements
- Materialized views: station_usage, user_activity, search_trends
- Caching layer with TTL-based expiration
- Cache invalidation on every event ingestion
- Partner-level isolation (optional)

## Enforcement Kernel Specification

### CI Execution DAG

**Stage Order** (strict linear sequence with artifact passing):

```
Stage 1: Read-Only Enforcement Gate
  ↓ Passes
  artifact: {read_only_gate_result}

Stage 2: Query Safety Gate
  ↓ Passes, consumes {read_only_gate_result}
  artifact: {query_safety_gate_result}

Stage 3: KPI Integrity Gate
  ↓ Passes, consumes {query_safety_gate_result}
  artifact: {kpi_integrity_gate_result}

Stage 4: Analytics Read API Integration Test
  ↓ Passes, consumes {kpi_integrity_gate_result}
  artifact: {api_integration_test_result}

Stage 5: Cache Invalidation Flow Test
  ↓ Passes, consumes {api_integration_test_result}
  artifact: {cache_invalidation_test_result}

Stage 6: Performance Benchmarks
  ↓ Passes, consumes {cache_invalidation_test_result}
  artifact: {performance_results}

Stage 7: E2E Analytics Dashboard Test
  ↓ Passes, consumes {performance_results}
  artifact: {e2e_test_results}
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

#### 1. Read-Only Enforcement Gate

**Input**: Codebase scan for analytics_db write operations in admin-service

**Algorithm**:
- Scan `admin-service` source code for `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `CREATE TABLE`, `ALTER TABLE` operations targeting `analytics_db` tables
- Verify no dynamic SQL containing write keywords in analytics query modules
- Verify all API endpoints have `GET` methods only
- Check database migrations in admin-service only touch `inventory` schema

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Read-only enforcement validated"
}
```

**Failure Signature**: Exit code 1 with failure details

---

#### 2. Query Safety Gate

**Input**: Source code scan for dynamic SQL in analytics query modules

**Algorithm**:
- Scan `admin-service` for string concatenation in SQL queries
- Verify all SQL uses parameterized queries only
- Check query building modules for unsafe string interpolation
- Validate no user input directly concatenated into SQL strings
- Verify query safety validation unit tests exist

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Query safety validated"
}
```

**Failure Signature**: Exit code 1 with failure details

---

#### 3. KPI Integrity Gate

**Input**: Verify all KPI calculations derive from telemetry events only

**Algorithm**:
- Scan KPI aggregation service code for references to external data sources
- Verify all metrics are count/average/sum aggregations over events
- Check for any hardcoded values or external API calls in KPI calculations
- Validate all KPI formulas use only `analytics_db` tables and event schemas
- Verify unit tests for KPI integrity exist

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "KPI integrity validated"
}
```

**Failure Signature**: Exit code 1 with failure details

---

#### 4. Analytics Read API Integration Test

**Input**: {read_only_gate_result}, {query_safety_gate_result}, {kpi_integrity_gate_result}

**Algorithm**:
- Start services: admin-service (port 3002), driver-service (port 3001)
- Create Keycloak user and obtain JWT token
- Test GET /api/v1/analytics endpoint with valid token
- Verify response includes expected KPIs (station_views, search_volume, active_users)
- Test GET /api/v1/analytics/stations/:id with valid token
- Verify station-specific metrics returned correctly
- Attempt POST/PUT/DELETE on analytics endpoints
- Verify all write attempts rejected with 403 Forbidden

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Analytics read API validated"
}
```

**Failure Signature**: Exit code 1 with failure details

---

#### 5. Cache Invalidation Flow Test

**Input**: {api_integration_test_result}

**Algorithm**:
- Start services: admin-service (port 3002), driver-service (port 3001), Redis
- Trigger cache invalidation endpoint with valid request
- Verify cache entry is deleted
- Ingest a test telemetry event via driver-service
- Trigger cache invalidation callback from driver-service
- Verify cache is invalidated and subsequent queries return fresh data
- Measure time to invalidate (< 5 seconds requirement)

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Cache invalidation flow validated"
}
```

**Failure Signature**: Exit code 1 with failure details

---

#### 6. Performance Benchmarks

**Input**: {cache_invalidation_test_result}

**Algorithm**:
- Run load test with 1000 concurrent queries
- Measure average query latency (cached and uncached)
- Calculate cache hit rate
- Verify cache hit rate > 80%
- Verify query latency < 500ms (cached) and < 500ms (uncached)
- Measure cache invalidation time (< 5 seconds)

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Performance benchmarks validated"
}
```

**Failure Signature**: Exit code 1 with failure details

---

#### 7. E2E Analytics Dashboard Test

**Input**: {performance_results}

**Algorithm**:
- Start services with test data (Sprint 3 telemetry events)
- Create Keycloak user and authenticate
- Navigate to admin dashboard via Playwright
- Verify dashboard loads with KPIs
- Query station analytics
- Verify results match expected values from events
- Verify cache hit/miss indicators displayed
- Test cache invalidation via event ingestion
- Verify dashboard updates with new data

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "E2E analytics dashboard validated"
}
```

**Failure Signature**: Exit code 1 with failure details

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Gate 1: Service Count Constraint (✅ PASS)

**Constitution Requirement**: Exactly three services only: auth-service (3000), driver-service (3001), admin-service (3002). No additional services may be introduced.

**Compliance Status**: ✅ PASS

**Justification**: Admin analytics read layer runs within existing admin-service (port 3002). No new services introduced.

**Verification**:
- Verify admin-service source code contains no new service boundaries
- Verify no new database schemas added (analytics_db already exists from Sprint 3)
- Verify no new event pipelines introduced

---

### Gate 2: Database Architecture (✅ PASS)

**Constitution Requirement**: analytics_db has READ/WRITE for driver-service and READ ONLY for admin-service. admin-service has NO ACCESS to other schemas.

**Compliance Status**: ✅ PASS

**Justification**: Read-only enforcement enforced at database level with separate database role. admin-service cannot write to analytics_db.

**Verification**:
- Verify analytics_db database role `bornemap_analytics_reader` exists with SELECT-only permissions
- Verify admin-service uses separate connection for analytics_db
- Verify CI gate scans for write operations in admin-service targeting analytics_db

---

### Gate 3: Data Ownership Rule (✅ PASS)

**Constitution Requirement**: All data domains strictly owned by single service. Cross-service database writes forbidden.

**Compliance Status**: ✅ PASS

**Justification**: Telemetry events written by driver-service only. admin-service only reads from analytics_db. No cross-service writes.

**Verification**:
- Verify driver-service writes events to analytics_db (Sprint 3)
- Verify admin-service has no write operations to analytics_db
- Verify no new ownership domains introduced

---

### Gate 4: API Ownership Rule (✅ PASS)

**Constitution Requirement**: admin-service API includes Inventory APIs and analytics dashboards. Business endpoints not duplicated.

**Compliance Status**: ✅ PASS

**Justification**: Analytics read endpoints added to admin-service (GET /api/v1/analytics/*). No business endpoint duplication.

**Verification**:
- Verify new endpoints are GET methods only
- Verify endpoints under /api/v1/analytics/* namespace
- Verify no duplicate business endpoints introduced

---

### Gate 5: Identity System Rule (✅ PASS)

**Constitution Requirement**: Users = UUID only. Entities = PREFIX-nanoid only. No mixing allowed.

**Compliance Status**: ✅ PASS

**Justification**: Telemetry events use user_uuid (UUID) and entity IDs with PREFIX-nanoid format. No identity violations in analytics queries.

**Verification**:
- Verify events use user_uuid field (from Keycloak)
- Verify entity IDs use PREFIX-nanoid format
- Verify no merged identity fields in queries

---

### Gate 6: SQLx Enforcement Rule (✅ PASS)

**Constitution Requirement**: All queries MUST be compile-time verified. CI MUST run: `cargo sqlx prepare --check`. Failure = HARD STOP.

**Compliance Status**: ✅ PASS

**Justification**: All analytics queries will use sqlx with compile-time verification. CI gate enforces sqlx prepare --check.

**Verification**:
- Verify all queries in admin-service use sqlx macros (query! macro)
- Verify CI pipeline includes cargo sqlx prepare --check
- Verify no raw SQL strings without compile-time verification

---

### Gate 7: CI/Enforcement Rules (✅ PASS)

**Constitution Requirement**: HARD FAIL conditions: analytics_db write violation, service topology change, SQLx failure, schema mismatch, dependency violation.

**Compliance Status**: ✅ PASS

**Justification**: CI gates implemented for read-only enforcement, query safety, and KPI integrity. No topology changes or dependency violations.

**Verification**:
- Verify CI gate: read-only enforcement scan
- Verify CI gate: query safety scan
- Verify CI gate: KPI integrity scan
- Verify no topology changes or dependency violations

---

## Project Structure

### Documentation (this feature)

```text
specs/005-admin-analytics/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── analytics-api.yaml       # API contracts
│   ├── kpi-aggregation.yaml     # KPI aggregation contracts
│   └── cache-invalidation.yaml  # Cache invalidation contracts
└── tasks.md             # Phase 2 output
```

### Source Code (repository root)

```text
admin-service/
├── Cargo.toml
├── src/
│   ├── main.rs
│   ├── api/
│   │   ├── mods.rs
│   │   ├── analytics.rs          # Analytics read API endpoints
│   │   └── health.rs
│   ├── services/
│   │   ├── analytics_query_service.rs  # KPI aggregation
│   │   ├── cache_service.rs            # Caching layer
│   │   ├── cache_invalidation.rs       # Cache invalidation handler
│   │   └── materialized_views.rs       # Materialized view refresh
│   ├── models/
│   │   ├── analytics.rs               # Analytics data models
│   │   ├── kpi.rs                     # KPI aggregation models
│   │   └── cache.rs                   # Cache models
│   ├── db/
│   │   ├── analytics_db.rs            # analytics_db connection
│   │   ├── queries/
│   │   │   ├── station_usage.rs       # Station usage queries
│   │   │   ├── user_activity.rs       # User activity queries
│   │   │   └── search_trends.rs       # Search trends queries
│   │   └── migrations/
│   │       └── analytics_materialized_views.sql
│   ├── middleware/
│   │   ├── auth.rs                    # Keycloak JWT middleware
│   │   └── read_only.rs               # Read-only enforcement middleware
│   └── validators/
│       ├── query_safety.rs            # Query safety validator
│       └── kpi_integrity.rs           # KPI integrity validator
├── migrations/
│   └── analytics_db/
│       └── 000001_create_materialized_views.sql
└── tests/
    ├── integration/
    │   ├── analytics_api_tests.rs
    │   └── cache_invalidation_tests.rs
    └── e2e/
        └── analytics_dashboard_e2e_test.rs
```

**Structure Decision**: Admin-service follows existing architecture pattern with separate API, services, models, and database modules. Analytics layer isolated in dedicated packages to maintain separation of concerns.

---

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No constitution violations in this feature.

### Enforcement Kernel Complexity

The enforcement kernel introduces complexity to ensure constitutional compliance:

| Complexity Component | Why Needed | Simpler Alternative Rejected Because |
|---------------------|------------|-------------------------------------|
| Read-Only Enforcement Gate | Prevents write access violations to analytics_db | Manual code review - prone to human error and missed violations |
| Query Safety Gate | Prevents SQL injection and dynamic SQL vulnerabilities | Trusting query builders - could introduce security vulnerabilities |
| KPI Integrity Gate | Ensures KPIs are derived from telemetry events only | Dynamic queries - could introduce external data source dependencies |
| Cache Invalidation Synchronous Callback | Ensures consistency without eventual consistency tradeoffs | Asynchronous messaging - adds complexity of message queues and dead letter handling |

---