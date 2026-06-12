# Research: Backend Services

**Feature**: `002-backend-services` | **Date**: 2026-06-11

## 1. Cargo Workspace Structure

**Decision**: Workspace root at `source/services/Cargo.toml` with 5 members, resolver = "2"

**Rationale**:
- Keeps all Rust code under `source/services/` per Constitution IV
- Shared crates (ev-core, ev-db, ev-auth) as path dependencies with zero duplication
- All external versions managed centrally in `[workspace.dependencies]`
- Each binary crate depends only on what it needs

**Alternatives considered**:
- Single flat crate: rejected — no separation of domain, DB, and HTTP concerns
- Workspace at repo root: rejected — mixes runtime and non-runtime code
- Separate workspaces per service: rejected — shared crate compilation would be duplicated

**Structure**:
```
source/services/
├── Cargo.toml               # Workspace root
├── rust-toolchain.toml       # channel = "1.80"
├── shared/
│   ├── ev-core/              # Domain types, errors, ID generation
│   ├── ev-db/                # DB pooling, query helpers, test_db
│   └── ev-auth/              # Stub (MVP-3)
├── driver-service/           # Binary :8080
└── admin-service/            # Binary :8081
```

## 2. sqlx + PostGIS Patterns

**Decision**: Runtime `sqlx::query_as` for all PostGIS queries (no compile-time macros). Cast `location::geography` for meter-accurate ST_DWithin.

**Rationale**:
- PostGIS types (geometry, geography) not natively understood by sqlx compile-time macros
- Expression columns (ST_Distance, ST_DWithin) cannot be inferred
- geozero's `wkb::Decode` implements sqlx's `Decode` trait only for runtime queries
- Existing schema uses `GEOMETRY(Point, 4326)` with generated `location` column from `lat`/`lng` doubles
- `lat`/`lng` doubles are plain `f64` in Rust — no geometry type needed for reading
- ST_DWithin with `::geography` cast gives meter-accurate results and uses the GiST index

**Key SQL pattern** for nearby search:
```sql
SELECT id, name, lat, lng, status, partner_id,
       ST_Distance(location::geography, ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography) AS distance_meters
FROM inventory.station
WHERE deleted_at IS NULL
  AND ST_DWithin(location::geography, ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography, $3)
ORDER BY distance_meters ASC
```

**Pool config**: max_connections=20, min_connections=2, acquire_timeout=5s, idle_timeout=10min, max_lifetime=30min, test_before_acquire=true

**Dependencies**: sqlx 0.8 with `runtime-tokio`, `postgres`, `chrono`, `uuid` features; geozero 0.15 with `with-postgis-sqlx`, `with-geo` features; geo-types 0.7

## 3. Actix-web 4 Patterns

**Decision**: AppError enum in ev-core with ResponseError impl for consistent JSON errors. web::Data<AppState> for shared state. web::scope("/api/v1") for route prefixing.

**Rationale**:
- Single error type across both services ensures consistent error shape (FR-014)
- web::Data uses Arc internally — no manual wrapping needed
- Route ordering: `/stations/nearby` before `/stations/{id}` to avoid capture issue
- Health check at GET `/health` outside /api/v1/ scope
- TracingLogger + custom RequestId middleware for FR-013 logging requirements

**Error shape**:
```json
{ "error": { "code": "NOT_FOUND", "message": "Station not found" } }
{ "error": { "code": "VALIDATION_ERROR", "message": "...", "details": [{ "field": "name", "message": "Required" }] } }
```

## 4. Contract Testing in Rust

**Decision**: Two-layer strategy — `actix_web::test` (in-process, per-commit) + `reqwest` (E2E, opt-in with `#[ignore]`).

**Rationale**:
- In-process tests are fast (~10ms), run with every `cargo test`, catch routing/middleware/JSON shape issues
- E2E tests validate the full binary lifecycle (startup, DB connection, port binding)
- Test DB: create once per test run via shared helper in ev-db/test_db.rs, drop/recreate with migrations, seed known data
- 100% contract coverage requirement (FR-019) is achievable with in-process tests alone
- E2E tests reserved for cross-service contracts (admin creates → driver reads)

**Test file organization**:
```
driver-service/tests/
├── common/mod.rs            # create_test_app, test pool, seed data
├── contract_stations.rs     # GET /stations, /nearby, /{id}
├── contract_health.rs       # GET /health
└── e2e_contract_driver.rs   # #[ignore] — full stack tests

admin-service/tests/
├── common/mod.rs
├── contract_stations.rs     # POST/PUT/DELETE /stations
├── contract_events.rs       # POST /events, /events/batch
└── e2e_contract_admin.rs
```

## 5. Docker Packaging

**Decision**: Multi-stage Dockerfiles with cargo-chef, SQLX_OFFLINE=true, lld linker, alpine:3.20 runtime.

**Rationale**:
- cargo-chef caches dependency compilation across builds — ~150s vs ~370s on rebuild
- SQLX_OFFLINE=true avoids needing a live database during Docker build
- lld linker reduces linking time by 2-3x
- Alpine runtime yields ~15MB images
- Non-root user (app) in runtime for security

**Each service gets its own Dockerfile** placed at `infra/docker/{service}.Dockerfile`:
- Build context: repo root (for workspace access)
- ARG SERVICE_NAME, ARG APP_PORT
- Stages: chef → planner → builder → runtime

**Compose additions**: Two new services (driver-service, admin-service) with depends_on conditions on database health checks.

## 6. Nanoid Generation

**Decision**: `nanoid` crate 0.4 in `ev-core`. Format: `{PREFIX}-{nanoid21}`. Application-layer generation.

**Rationale**:
- nanoid produces short, clean, URL-safe IDs (default 21 chars of A-Za-z0-9_-)
- With prefix + dash: ~26 chars total → well within VARCHAR(50)
- Application-layer: better testability, no extra DB round-trips, Rust type safety
- Enum-based EntityPrefix prevents typos in prefix strings

**Prefixes**: STA- (Station), CHR- (Charger), PRT- (Partner), USR- (User), OPR- (Operator)

**Example**: `STA-V1StGXR8_Z5jdHi6B-myT`

**Alternatives considered**:
- uuid v7: rejected — produces 36-char strings with dashes, visually noisy with prefix
- DB trigger: rejected — hard to test, hidden logic in migrations, extra DB call for RETURNING
- ulid: rejected — fixed 26 chars, less common in Rust ecosystem

## Summary of Cargo Dependencies

```toml
# Workspace-level shared dependencies
actix-web = "4"
actix-cors = "0.7"
actix-rt = "2"
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "chrono", "uuid"] }
geozero = { version = "0.15", features = ["with-postgis-sqlx", "with-geo"] }
geo-types = "0.7"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
chrono = { version = "0.4", features = ["serde"] }
uuid = { version = "1", features = ["v4", "serde"] }
nanoid = "0.4"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
tracing-actix-web = "0.7"
thiserror = "2"
anyhow = "1"
dotenvy = "0.15"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }

# Dev dependencies (optional)
reqwest = { version = "0.12", features = ["json"] }
