# Research: Backend Core — Schema, Identity & CRUD

**Phase**: 0 (Outline & Research)

## Decisions

### Decision 1: JWT Library — `jsonwebtoken` 10.x

- **Decision**: Use `jsonwebtoken` 10.x with `rust_crypto` feature
- **Rationale**: De facto standard in the Rust ecosystem (130M+ downloads). Minimal
  API surface: `encode(header, claims, key)` / `decode(token, key, validation)`.
  Built-in `exp` validation with configurable leeway. Custom claims derive
  `Serialize`/`Deserialize` — zero friction for `sub` (USR- prefixed ID) and
  `role` (admin/partner/driver enum). Easy to wrap in an Actix-web
  `FromRequest` extractor. The `rust_crypto` feature avoids the `aws-lc-rs`
  native build dependency, simplifying CI.
- **Alternatives considered**: `biscuit` (JOSE spec ceremony, verbose for simple
  JWT), `frank-jwt` (unmaintained, no type-safe claims, no built-in expiry)

### Decision 2: Password Hashing — `argon2` 0.5 (RustCrypto)

- **Decision**: Use `argon2` 0.5 with `password-hash` traits
- **Rationale**: Argon2id is the OWASP-recommended algorithm. The RustCrypto
  crate produces standard PHC strings, is pure Rust (no C FFI), and has no
  72-byte silent truncation pitfall (unlike bcrypt). All hash/verify calls
  are wrapped in `tokio::task::spawn_blocking` to avoid starving the async
  runtime. Default parameters (m=19456, t=2, p=1) produce ~250-500ms per hash.
- **Alternatives considered**: `bcrypt` (4 KiB memory cost, GPU-parallelizable,
  72-byte truncation), `rust-argon2` (standalone, no PHC format, less
  maintained), `scrypt` (superseded by Argon2id for password storage)

### Decision 3: Cursor-Based Pagination — Composite `(created_at, id)` Keyset

- **Decision**: Keyset pagination using composite `(created_at, id)` row-value
  comparison, cursor encoded as URL-safe base64 JSON (no padding)
- **Rationale**: The existing `id_generator.rs` uses `fastrand` (purely random)
  — IDs have NO chronological ordering. Single-column cursors on `id` or
  `created_at` alone are insufficient (random IDs can't be ordered; same-second
  timestamps cause skips/dupes). PostgreSQL row-value comparison
  `(created_at, id) > ($1, $2)` is a first-class feature that uses composite
  B-tree index access efficiently. Base64url encoding makes cursors opaque and
  URL-safe. `LIMIT + 1` fetch detects `has_more` without a separate COUNT query.
- **Alternatives considered**: Offset pagination (O(offset) scan, inconsistent
  with concurrent writes), cursor = ID only (IDs are random, not ordered),
  cursor = created_at only (non-unique, same-second collisions), Relay-style
  connections (over-engineered for REST)

### Decision 4: Optimistic Locking — `updated_at` Timestamp as Version Token

- **Decision**: Use `updated_at TIMESTAMPTZ` as the concurrency token. Clients
  send the `updated_at` value from their last read in the request body. Server
  includes `WHERE updated_at = $N` in UPDATE; if 0 rows affected, distinguish
  404 (entity doesn't exist / soft-deleted) from 409 (concurrent modification)
  via an existence check.
- **Rationale**: `updated_at` already exists on every table — zero schema
  change. PostgreSQL `TIMESTAMPTZ` has 1µs resolution; collision probability
  in an admin CRUD system is negligible. SQLx + chrono preserves microsecond
  precision through full round-trip. Single source of truth (business metadata
  + concurrency token combined).
- **Alternatives considered**: `version INTEGER` column (extra migration, extra
  index, duplicates `updated_at`'s role), ETag/If-Match headers (adds header
  complexity for Expo/React clients), pessimistic locking (overkill for
  low-contention admin CRUD)

### Decision 5: PostGIS Coordinate Storage — `GEOGRAPHY(Point, 4326)`

- **Decision**: Store station coordinates as `coordinates GEOGRAPHY(Point, 4326)
  NOT NULL` with a GIST spatial index. Insert via `ST_SetSRID(ST_MakePoint($lng,
  $lat), 4326)` (longitude-first). Select via `ST_X(coordinates::geometry) AS
  longitude, ST_Y(coordinates::geometry) AS latitude`. Validate lat (-90 to 90)
  and lng (-180 to 180) at the application layer with a database CHECK
  constraint as defense-in-depth.
- **Rationale**: `GEOGRAPHY` uses meter-based calculations natively (required
  for `ST_DWithin` in Phase 2). Constitution mandates `GEOGRAPHY(Point, 4326)`
  in longitude-first notation. Rust model uses separate `longitude: f64` +
  `latitude: f64` fields — the GEOGRAPHY type never surfaces to Rust code.
  GIST index supports `ST_DWithin`, `ST_Distance`, and KNN `<->` without
  schema migration in Phase 2.
- **Alternatives considered**: Separate `latitude FLOAT8` + `longitude FLOAT8`
  columns (no spatial indexing, no native distance functions), `GEOMETRY`
  instead of `GEOGRAPHY` (planar calculations in degrees, not meters —
  incorrect for distance queries)

### Decision 6: Seed Data — Deterministic SQL Migration

- **Decision**: Use a SQL migration (`20260527000001_seed_sandbox.up.sql`)
  with hardcoded IDs and data. No random generation in the seed script.
- **Rationale**: Constitution Principle V requires deterministic, repeatable
  seed data. SQL with fixed values produces identical results across
  environments. All seed records carry `is_test = true`.
- **Alternatives considered**: Rust-based seeder with fixed RNG seed (more
  complex, harder to inspect, not idempotent by default)

### Decision 7: Error Response Format — RFC 7807 Problem Details

- **Decision**: All error responses use RFC 7807 `application/problem+json`
  format with `type`, `title`, `status`, and `detail` fields.
- **Rationale**: Machine-readable, standardized, extensible. Consistent format
  across validation errors (422), not-found (404), conflict (409), and
  unauthorized (401). Widely adopted in REST API design.
- **Alternatives considered**: Flat `{ "error": "message" }` (no structure,
  not standardized), per-endpoint custom error shapes (inconsistent, harder
  for clients)

### Decision 8: Cargo.toml Additions

- **Decision**: Add the following dependencies to `sources/backend/Cargo.toml`:
  - `jsonwebtoken = { version = "10", default-features = false, features = ["rust_crypto"] }`
  - `argon2 = { version = "0.5", features = ["std"] }`
  - `base64 = "0.22"`
  - `thiserror = "2"` (error type derivation)
  - `validator = { version = "0.18", features = ["derive"] }` (request validation)
- **Rationale**: `jsonwebtoken` and `argon2` are dictated by Decisions 1 and 2.
  `base64` is needed for cursor encoding (Decision 3). `thiserror` provides
  ergonomic error types for the repository/handler layers. `validator` provides
  declarative request validation with derive macros (email format, min length,
  range constraints).
- **Alternatives considered**: `anyhow` for errors (too generic for API error
  responses), manual validation (verbose, error-prone), `snafu` (less
  idiomatic than `thiserror` for this use case)
