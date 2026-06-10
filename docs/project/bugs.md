# Known Bugs — BorneMap

**Last updated:** 2026-06-09

This file tracks all known bugs classified by severity (Class A / B / C).

## Class A

### A-001: Dynamic SQL injection in test helper (`update_partner_flag`)

**File:** `source/services/driver-service/tests/visibility.rs:50-53`

**Description:** `update_partner_flag` builds SQL by interpolating a column name via `format!()` into the query string. While callers currently pass only controlled values (`is_active`, `is_verified`, `is_live`), the function has no guard against arbitrary column injection. This bypasses sqlx's compile-time query validation entirely.

**Fix:** Replace string parameter with a typed enum and match to generate the correct SQL per variant.

### A-002: Integration test data persists on panic (no transaction rollback)

**File:** `source/services/driver-service/tests/visibility.rs` (all tests)

**Description:** Test data is cleaned up via `DELETE` at the end of each test. If a test panics mid-way (e.g., assertion failure), cleanup code is skipped and rows remain in the database. Subsequent runs encounter stale data, and static test IDs (`TEST_PRT_HIDE_active`, etc.) can collide via `ON CONFLICT DO NOTHING`, producing silent false passes.

**Fix:** Wrap test setup/execution in a database transaction and roll back unconditionally after each test (e.g., `pool.begin().await` + drop/rollback guard).

## Class B

### B-001: Docker images built in debug mode

**Files:** `source/services/driver-service/Dockerfile:10`, `source/services/admin-service/Dockerfile:10`

**Description:** Both Dockerfiles use `cargo build` (debug profile) instead of `cargo build --release`. Debug binaries are ~3x larger and ~10x slower. Any integration or performance testing against Docker containers will produce misleading results.

**Fix:** Revert to `cargo build --release`. If build time is the concern, implement `cargo-chef` multi-stage builds or mount a persistent cargo registry volume.

### B-002: Fixed 90s sleep in zero-state verification

**File:** `scripts/verify-zero-state.sh:13-14`

**Description:** The script sleeps a fixed 90 seconds before checking health. This is unreliable: services may start faster (wasted time) or slower (false failure). Docker health checks can take longer than 90s on cold start.

**Fix:** Replace with a polling loop: check health every 5 seconds for up to 120 seconds.

### B-003: `connect_lazy` hides database connection failures

**File:** `source/services/driver-service/tests/visibility.rs:5-8`

**Description:** `PgPoolOptions::connect_lazy()` defers the actual TCP connection to the first query. If `DATABASE_URL` has a bad password, wrong host, or unreachable server, the failure surfaces as a cryptic `expect("failed to upsert test partner")` inside `insert_partner` rather than a clean "cannot connect" skip or error.

**Fix:** Use `PgPoolOptions::connect()` (async) with its own `await` and error handling, so connection failures are surfaced distinctly from query failures.

### B-004: Static test IDs survive panicked runs

**File:** `source/services/driver-service/tests/visibility.rs` (all test fixtures)

**Description:** All test partners and stations use static IDs (`TEST_PRT_HIDE_active`, `TEST_STN_HIDE_active`, etc.). If a test panics before cleanup, these rows persist. `insert_station` uses `ON CONFLICT DO NOTHING`, so a re-run silently skips the insert and tests against stale data. This can produce false positives (a station appears because a previously-panicked run left it with the wrong flag values).

**Fix:** Use UUID or timestamp-suffixed IDs, delete stale rows at test start, or (preferred) use transaction rollback (see A-002).

### B-005: Missing .dockerignore for test artifacts

**File:** `source/.dockerignore`

**Description:** The `tests/` directory in driver-service is copied into both Docker build contexts. This adds unnecessary files and can cause cache invalidation when tests change. No `.dockerignore` file exists at the `source/` level used as build context.

**Fix:** Create `source/.dockerignore` excluding `**/tests/`, `**/*.rs.bk`, `target/`, and other non-runtime artifacts.

## Class C

### C-001: Unpinned runtime base image digest

**Files:** `source/services/driver-service/Dockerfile:13`, `source/services/admin-service/Dockerfile:13`

**Description:** `FROM debian:bookworm-slim` is not pinned to a digest. A new release of the image could introduce different library versions, breaking the service silently.

**Fix:** Pin to the known-good digest: `FROM debian:bookworm-slim@sha256:...`

### C-002: Visibility integration test only validates one endpoint SQL

**File:** `source/services/driver-service/tests/visibility.rs:78-93`

**Description:** `station_is_visible()` replicates the JOIN+filter pattern from `detail.rs`. The other three endpoints (`nearby.rs`, `search.rs`, `markers.rs`) each repeat this same visibility JOIN in their own SQL. The test does not verify that all four endpoints apply the filter identically.

**Fix:** Either test the exact SQL from each endpoint, or add comments in the test documenting which endpoint's contract is being verified.

## Resolved

| ID | Description | Class | Resolved | Sprint |
|---|---|---|---|---|
