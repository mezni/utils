# Feature Specification: MVP-2 Hardening

**Feature Branch**: `012-mvp2-hardening`

**Created**: 2026-06-09

**Status**: Draft

**Input**: User description: "Sprint 2.6 — cargo test --all passes. cargo clippy --all-targets -- -D warnings clean. Docker Compose starts from zero cleanly. ST_DWithin confirmed by EXPLAIN ANALYZE. Visibility rule confirmed in integration tests. Full loop verified with Rust services. CI green on main branch."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - All Tests and Linting Pass Cleanly (Priority: P1)

A developer runs the full test suite and lint checker on the workspace. Every test passes and every lint rule is satisfied with zero warnings.

**Why this priority**: Failed tests and lint warnings are blocking gates for all further development. This must be clean before any other verification can proceed.

**Independent Test**: Run `cargo test --all` and `cargo clippy --all-targets -- -D warnings` from the workspace root. Both commands exit with code 0 and produce no warnings.

**Acceptance Scenarios**:

1. **Given** the Rust workspace, **When** running `cargo test --all`, **Then** all unit and integration tests pass with zero failures
2. **Given** the Rust workspace, **When** running `cargo clippy --all-targets -- -D warnings`, **Then** no warnings or errors are emitted
3. **Given** a failing test, **When** investigating, **Then** the test failure message clearly identifies the failing assertion and location

---

### User Story 2 - Docker Compose Starts Cleanly from Zero (Priority: P1)

An operator or developer runs `docker compose up --build -d` on a machine with no cached Docker data. All services start, pass health checks, and the database migrations run automatically.

**Why this priority**: A clean start from zero is the foundational reliability test for the deployment stack. If this fails, no deployment can be trusted.

**Independent Test**: On a clean Docker environment, run `docker compose down -v` (to remove volumes), then `docker compose up --build -d`. Wait for all health checks to pass and verify all endpoints respond.

**Acceptance Scenarios**:

1. **Given** no existing Docker volumes or images, **When** running `docker compose up --build -d`, **Then** all 6 services start within 120 seconds
2. **Given** all services are healthy, **When** querying `/api/health` on both driver-service (`:8080`) and admin-service (`:8081`), **Then** each returns `200 OK`
3. **Given** PostgreSQL is fresh, **When** the Rust services start, **Then** database migrations (0001–0004) run automatically via `sqlx::migrate!`
4. **Given** migrations have run, **When** querying any API endpoint that reads the database, **Then** seeded data is present (3 partners, 15 stations, 24 chargers)

---

### User Story 3 - Spatial Query and Visibility Rules Verified (Priority: P2)

A QA engineer or developer confirms that the Driver Service's spatial query (ST_DWithin) uses the database index and that partner visibility rules are correctly enforced.

**Why this priority**: Spatial query performance and data security (visibility rules) are core architectural guarantees of the MVP-2 backend. These must be verified, not assumed.

**Independent Test**: Run `EXPLAIN ANALYZE` on the nearby stations query to confirm index usage. Run integration tests that verify deactivated/unverified/non-live partner stations are excluded from driver-facing endpoints.

**Acceptance Scenarios**:

1. **Given** the Driver Service nearby endpoint, **When** running `EXPLAIN ANALYZE` on the generated SQL, **Then** the query plan shows an index scan (not sequential scan) on the station coordinates
2. **Given** a partner with `is_active = false`, **When** querying driver endpoints, **Then** none of that partner's stations appear in results
3. **Given** a partner with `is_verified = false`, **When** querying driver endpoints, **Then** none of that partner's stations appear in results
4. **Given** a partner with `is_live = false`, **When** querying driver endpoints, **Then** none of that partner's stations appear in results

---

### User Story 4 - Full Product Loop Verified with Rust Services (Priority: P2)

A product owner or QA engineer verifies the complete end-to-end workflow: admin creates and manages partners/stations/chargers via Admin Service, partner manages their own data, and driver discovers stations via Driver Service.

**Why this priority**: The full loop is the primary integration test for MVP-2. If the admin cannot manage data and the driver cannot discover it, the backend is not ready.

**Independent Test**: Walk through the complete workflow manually using curl or a REST client, then verify that frontend apps connected to the Rust services show correct data.

**Acceptance Scenarios**:

1. **Given** the Admin Service is running, **When** creating a partner (POST /api/partners), **Then** the partner is created with default flags (is_verified=false, is_live=false, is_active=true)
2. **Given** an unverified partner, **When** verifying the partner (PATCH /api/partners/{id}/verify), **Then** is_verified becomes true
3. **Given** a verified partner, **When** setting is_live (PATCH /api/partners/{id}), **Then** is_live becomes true
4. **Given** a live verified partner, **When** querying the Driver Service nearby endpoint, **Then** the partner's stations appear in results
5. **Given** a partner's stations are visible to drivers, **When** deactivating the partner, **Then** their stations immediately disappear from Driver Service results

---

### User Story 5 - CI Pipelines Pass on Main Branch (Priority: P2)

A developer opens a pull request or pushes to main. All GitHub Actions workflows run and pass without manual intervention.

**Why this priority**: Green CI on main is the final gate for MVP-2 completion. All prior sprints must integrate correctly.

**Independent Test**: Check the latest CI run on the main branch. Verify all workflows (driver-service, admin-service) show green.

**Acceptance Scenarios**:

1. **Given** a push to main, **When** CI workflows trigger, **Then** both driver-service and admin-service workflows run and pass
2. **Given** CI passes, **When** reviewing workflow logs, **Then** cargo build, cargo test, cargo clippy, and Docker build all succeed

---

### Edge Cases

- **Stale test database**: If integration tests leave behind stale data, subsequent test runs may produce false positives or failures. Tests must clean up after themselves or use transactions that roll back.
- **Docker environment without curl**: Docker images include curl in their runtime stage (added in Sprint 2.5). If the health check command fails, services are marked unhealthy and dependent services do not start.
- **Integration tests requiring PostgreSQL**: Tests that need a live database connection should be gated behind a `DATABASE_URL` environment variable check and skipped with a clear message when absent.
- **Partial CI run**: If one workflow fails and another succeeds, the developer must inspect the failed workflow logs. CI is green only when ALL configured workflows pass.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST pass `cargo test --all` with zero failures across the entire workspace
- **FR-002**: System MUST pass `cargo clippy --all-targets -- -D warnings` with zero warnings
- **FR-003**: Docker Compose MUST start all services cleanly from zero (no cached volumes or images) in under 120 seconds
- **FR-004**: Both Rust services MUST run `sqlx::migrate!` on startup, applying all 4 migrations automatically
- **FR-005**: Driver Service nearby endpoint MUST use index scan (confirmed by EXPLAIN ANALYZE) for ST_DWithin spatial queries
- **FR-006**: Driver Service MUST exclude stations belonging to partners where `is_active = false`, `is_verified = false`, OR `is_live = false`
- **FR-007**: Admin Service MUST support the full partner lifecycle: create, verify, activate/deactivate, edit
- **FR-008**: Full product loop MUST be verified: admin creates partner → admin verifies → admin sets live → driver discovers stations → admin deactivates → stations disappear
- **FR-009**: CI workflows on main branch MUST pass with green status on all configured pipelines

### Key Entities *(include if feature involves data)*

This sprint introduces no new entities. Verification covers the existing entities from prior sprints:

- **Partner**: Verified for flag management lifecycle (create → verify → activate → deactivate)
- **Station**: Verified for spatial query performance and visibility rule enforcement
- **Charger**: Verified for inclusion/exclusion based on parent station visibility
- **Availability**: Verified via Admin Service availability endpoint

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `cargo test --all` produces 0 failures and 0 errors in every crate (ev-core, ev-db, driver-service, admin-service)
- **SC-002**: `cargo clippy --all-targets -- -D warnings` produces 0 warnings, 0 errors
- **SC-003**: `docker compose up --build -d` on a clean environment completes with all 6 services healthy within 120 seconds
- **SC-004**: Spatial queries returning nearby stations complete in under 100ms on a dataset of 10,000+ stations, confirming efficient query execution
- **SC-005**: All 3 visibility rule scenarios (inactive, unverified, not-live) return zero results for that partner's stations
- **SC-006**: Full product loop completes successfully — partner creation through driver discovery through deactivation — verified end to end
- **SC-007**: All CI workflows on main branch show green (passing) status

## Assumptions

- Integration tests that require a live PostgreSQL connection will be skipped or marked `#[ignore]` when no `DATABASE_URL` is available
- `cargo clippy --all-targets` covers test code (`--all-targets` includes `tests/` and `benches/`)
- Docker Compose version 2.x+ is available on the test environment
- `EXPLAIN ANALYZE` will be run against a development or CI PostgreSQL instance, not production
- No new code changes are introduced during hardening — only bug fixes and test additions
- Frontend apps remain pointed at Rust services via `API_BASE_URL` (configured in Sprint 2.5)
- CI workflows are the ones defined in Sprint 2.5: `.github/workflows/driver-service.yml` and `admin-service.yml`
