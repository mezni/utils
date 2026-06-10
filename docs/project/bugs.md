# Known Bugs — BorneMap

**Last updated:** 2026-06-10

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

### A-003: No authentication — `X-Partner-Id` header trivially spoofed

**File:** `source/services/admin-service/src/config.rs:29-35`

**Description:** `x_partner_id()` reads the `X-Partner-Id` header and defaults to `"admin"`. Any client — including external unauthenticated callers — can set this header to any value, impersonating any partner or admin. This is a privilege escalation vulnerability. The intended MVP-3 Keycloak auth is not yet integrated.

**Fix:** Remove the default `"admin"` fallback in dev/CI. In MVP-2 scope, require an explicit header (fail closed, not open). Document that this is placeholder auth for development only.

## Class B

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

### B-007: Schema name mismatch — Constitution vs implementation

**Files:** `database/migrations/*.sql`, `docs/constitution.md:508-570`

**Description:** The Constitution defines the schema as `inventory` (e.g., `inventory.partner`). All four migrations use `"ev-platform"` as the schema name. Every Rust service query also references `"ev-platform"`. Anyone reading the Constitution to understand the data model will find the wrong schema name. This is a documentation drift that will compound with every new migration.

**Fix:** Either update the Constitution to reference `"ev-platform"` or rename the schema in migrations. The former is lower risk.

### B-008: Column type and nullability discrepancies between Constitution and migrations

**Files:** `database/migrations/0002_*.sql`, `database/migrations/0003_*.sql`, `database/migrations/0004_*.sql`, `docs/constitution.md:508-570`

**Description:** The Constitution declares `NUMERIC(10,7)` for lat/lng and `NUMERIC(6,2)` for `power_kw`, but migrations use `DOUBLE PRECISION`. The Constitution marks audit fields (`created_by`, `updated_by`) as nullable (`TEXT` with implied null), but migrations enforce `NOT NULL`.

**Fix:** Update the Constitution to match actual implementation types. `DOUBLE PRECISION` for coordinates is acceptable (PostGIS prefers it).

### B-009: No `ON DELETE CASCADE` on foreign key constraints

**Files:** `database/migrations/0003_create_station_table.sql:18`, `database/migrations/0004_create_charger_and_availability_tables.sql:17,27`

**Description:** `charger.station_id` and `station_availability.station_id` reference `station.id` with no `ON DELETE CASCADE`. Deleting a station that has chargers will fail with a foreign key violation. Deleting a partner that has stations also fails. The admin-service's hard-delete for stations/chargers (B-008) combined with no cascade means deletes often fail with opaque errors.

**Fix:** Add `ON DELETE CASCADE` to `fk_charger_station` and `fk_availability_station`, or implement application-level cascading deletes.

### B-016: Database name mismatch — Constitution vs docker-compose

**Files:** `docker-compose.yml:10`, `docs/constitution.md:498`

**Description:** The Constitution (section 12) states the database name is `ev_platform`. The docker-compose file creates a database named `borne_map`. The `DATABASE_URL` in docker-compose points to `borne_map`. Anyone reading the Constitution will expect to connect to `ev_platform`.

**Fix:** Align — either rename the database in docker-compose to `ev_platform`, or update the Constitution.

## Sprint 3.1 — Keycloak Setup

### K-001: Keycloak 24.0 image lacks curl — health check must use bash TCP

**File:** `docker-compose.yml:42`

**Description:** The `quay.io/keycloak/keycloak:24.0` image does not include `curl`. Docker Compose health checks that use `curl -f` will fail with `/bin/sh: curl: command not found`. The service still runs correctly but `docker compose ps` shows `unhealthy`.

**Fix:** Use `["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/localhost:8180' || exit 1"]` for health checks. Applied in Sprint 3.1.

### K-002: `--import-realm` crashes on empty/placeholder JSON

**File:** `infra/keycloak/realm-export.json`, `docker-compose.yml`

**Description:** When `--import-realm` is used and the import file is `{}` (empty placeholder), Keycloak fails at startup with `ERROR: Realm name cannot be empty`. This happens on first run before the realm has been configured and exported.

**Fix:** Omit `--import-realm` and the volume mount on first run. Add them back only after a real realm export exists at `infra/keycloak/realm-export.json`. Applied in Sprint 3.1 via conditional setup instructions.

### K-003: Keycloak schema may be created in wrong database

**File:** `database/migrations/0007_keycloak_schema.sql`

**Description:** The `CREATE SCHEMA IF NOT EXISTS keycloak` migration targets the default database when run manually via `psql -U postgres -c "CREATE SCHEMA..."` — it connects to the `postgres` maintenance database, not `borne_map`. The schema must be created in `borne_map` where Keycloak's `KC_DB_URL` points.

**Fix:** Always specify the database: `psql -U postgres -d borne_map -c "CREATE SCHEMA IF NOT EXISTS keycloak;"`. Migration 0007 runs as part of the sqlx pipeline which correctly targets `borne_map`.

### K-004: Admin console defaults to master realm — users created in wrong realm

**File:** N/A (UI behavior)

**Description:** When creating users via the Keycloak admin console, the UI defaults to the `master` realm (current admin realm). Users created this way end up in `master` instead of the `ev-platform` realm. This is easy to miss because the admin console shows the realm selector as a small dropdown at top-left.

**Fix:** Manually switch to `ev-platform` in the realm selector before creating users. Document in quickstart.md.

### K-005: User Profile feature blocks custom attributes in Keycloak 24

**File:** N/A (API behavior)

**Description:** Keycloak 24 introduced a User Profile configuration that restricts which attributes can be set on users. Custom attributes like `partner_id` are silently dropped from API calls (PUT `/users/{id}` returns 204 but attributes are not stored) unless they are first added to the realm's User Profile specification. The User Profile must define the attribute name, validations, and permissions before any user can be assigned the attribute.

**Fix:** Add the custom attribute to the realm's User Profile via PUT `/admin/realms/{realm}/users/profile` before setting it on individual users. Applied in Sprint 3.1 for `partner_id`.

## Class C

### C-001: Unpinned runtime base image digest

**Files:** `source/services/driver-service/Dockerfile:13`, `source/services/admin-service/Dockerfile:13`

**Description:** `FROM debian:bookworm-slim` is not pinned to a digest. A new release of the image could introduce different library versions, breaking the service silently.

**Fix:** Pin to the known-good digest: `FROM debian:bookworm-slim@sha256:...`

### C-002: Visibility integration test only validates one endpoint SQL

**File:** `source/services/driver-service/tests/visibility.rs:78-93`

**Description:** `station_is_visible()` replicates the JOIN+filter pattern from `detail.rs`. The other three endpoints (`nearby.rs`, `search.rs`, `markers.rs`) each repeat this same visibility JOIN in their own SQL. The test does not verify that all four endpoints apply the filter identically.

**Fix:** Either test the exact SQL from each endpoint, or add comments in the test documenting which endpoint's contract is being verified.

### C-003: Dangerously low NanoID entropy (length=3)

**File:** `source/crates/ev-core/src/id.rs:24`

**Description:** `generate_id()` is called with `length=3` for all entity prefixes (`PRT`, `STN`, `CHG`, `SA`). With 62 characters in the alphabet, this gives only `62^3 = 238,328` possible values per prefix. Collisions become likely after just a few thousand IDs. Acceptable for MVP but must be increased for production.

**Fix:** Increase default length to at least 12 (or accept as technical debt for MVP-2).

### C-004: `Paginated::new()` panics instead of returning `Result`

**File:** `source/crates/ev-db/src/pagination.rs:34-35`

**Description:** `Paginated::new()` calls `assert!(page > 0)` and `assert!(page_size > 0)` which will crash the entire process if called with invalid arguments. While current callers pass validated values, a future caller that forgets validation could crash the service.

**Fix:** Change return type to `Result<Self, PaginationError>` and remove assertions.

### C-005: CI path triggers missing workspace-level files

**Files:** `.github/workflows/driver-service.yml:8-9`, `.github/workflows/admin-service.yml:8-9`

**Description:** Both CI workflows trigger only on `source/services/*/src/**` and `source/crates/**` changes. Changes to `source/Cargo.toml` or `source/Cargo.lock` (workspace configuration) will not trigger CI, potentially allowing a broken workspace to be merged.

**Fix:** Add `source/Cargo.toml` and `source/Cargo.lock` to both workflows' path triggers.

### C-006: No `cargo fmt --check` in CI

**Files:** `.github/workflows/driver-service.yml`, `.github/workflows/admin-service.yml`

**Description:** Neither CI workflow enforces Rust code formatting. Only `clippy` is run for linting. Unformatted code can be merged without CI catching it.

**Fix:** Add `cargo fmt --all --check` as a step before or after clippy in both workflows.

### C-007: Frontend services missing health checks in docker-compose

**File:** `docker-compose.yml`

**Description:** `dashboard`, `driver-web`, and `driver-mobile` have no health checks in docker-compose.yml. The Rust services and postgres all have health checks. Without them, `docker compose ps` cannot report the true status of the frontend services.

**Fix:** Add HTTP health checks for frontend apps (e.g., `curl -f http://localhost:5173/` for dashboard).

### C-008: driver-mobile Dockerfile uses single-stage build

**File:** `source/apps/driver-mobile/Dockerfile`

**Description:** Unlike the dashboard and driver-web Dockerfiles which use multi-stage builds (deps → build → runtime), driver-mobile uses a single stage. Any source code change invalidates the entire `node_modules` cache, making rebuilds slow.

**Fix:** Adopt the multi-stage pattern from the other frontend Dockerfiles (separate `deps` and `build` stages).

### C-009: `station_availability` missing audit trail fields

**File:** `database/migrations/0004_create_charger_and_availability_tables.sql:20-28`

**Description:** All other tables (`partner`, `station`, `charger`) have full audit trails: `created_at`, `created_by`, `updated_at`, `updated_by`. `station_availability` only has `updated_by` and `updated_at` (no `created_at`/`created_by`). This is inconsistent with the Constitution's audit trail pattern (section 4).

**Fix:** Add `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` and `created_by TEXT` to the migration.

### C-010: Seed data does not test all partner visibility states

**Files:** `database/seeds/001_partners.sql`, `database/seeds/002_stations.sql`

**Description:** All 15 stations belong to `PRT001` (verified, live, active). Partners `PRT002` (verified, not live, active) and `PRT003` (not verified, not live, active) have zero stations. The "verified but not live" and "unverified" visibility states cannot be exercised through the seed data alone.

**Fix:** Add at least one station to PRT002 and PRT003 so the visibility filter differences can be observed in seed data.

### C-011: `generate_id_with_alphabet` is public but unused

**File:** `source/crates/ev-core/src/id.rs:43`

**Description:** The function is exported as `pub` but never called anywhere in the codebase. It exists for future extensibility but adds dead code surface area.

**Fix:** Either remove it (add back when needed) or mark `#[doc(hidden)]` with a comment explaining intent.

### C-012: No request body size limits on any endpoint

**Files:** Both services' `main.rs`

**Description:** Neither Actix-web service configures a request body size limit (`web::JsonConfig::default().limit()`). An attacker could send a multi-gigabyte JSON payload, consuming server memory before deserialization. The default Actix limit is 256KB for most configurations, but it's not explicitly set.

**Fix:** Add explicit size limits (e.g., 16KB for small payloads, 64KB for station/partner creates) via `App::app_data(web::JsonConfig::default().limit(65536))`.

### C-013: No `DEFAULT NOW()` on `created_at` / `updated_at` columns

**Files:** All four migration files

**Description:** None of the `created_at` or `updated_at` columns have `DEFAULT NOW()` set. The column is `NOT NULL` but relies entirely on application code to provide values. Any raw SQL insert (e.g., manual debugging, ad-hoc scripts) will fail. The Constitution also lacks DEFAULT clauses, so this aligns with design intent — but it makes the schema fragile for manual operations.

**Fix:** Add `DEFAULT NOW()` to all `created_at` and `updated_at` columns. This is backward-compatible since application code already provides these values.

## Resolved

| ID | Description | Class | Resolved | Sprint |
|---|---|---|---|---|
| A-004 | Database error messages leaked to HTTP clients | A | 2026-06-10 | 2.6 |
| A-005 | Missing CHECK constraint for partner visibility rule | A | 2026-06-10 | 2.6 |
| A-006 | station_availability INSERT fails — missing updated_at | A | 2026-06-10 | 2.6 |
| B-001 | Docker images built in debug mode | B | 2026-06-10 | 2.6 |
| B-006 | Markers endpoint has no LIMIT | B | 2026-06-10 | 2.6 |
| B-010 | Rust Docker builds bypass root .dockerignore | B | 2026-06-10 | 2.6 |
| B-011 | No CREATE EXTENSION IF NOT EXISTS postgis | B | 2026-06-10 | 2.6 |
| B-012 | #![allow(dead_code)] suppresses legitimate warnings | B | 2026-06-10 | 2.6 |
| B-013 | Hardcoded version in admin-service health endpoint | B | 2026-06-10 | 2.6 |
| B-014 | Enum value duplication — validator arrays hardcode ev-core variants | B | 2026-06-10 | 2.6 |
| B-015 | Neither service calls sqlx::migrate!() on startup | B | 2026-06-10 | 2.6 |
