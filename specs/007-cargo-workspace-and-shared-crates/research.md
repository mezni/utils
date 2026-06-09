# Research: Cargo Workspace and Shared Crates

**Phase**: Phase 0 — Technology & pattern research for Sprint 2.1

**Date**: 2026-06-09

## Technology Decisions

### NanoID Generation

- **Decision**: Use the `nanoid` crate with custom alphabet (URL-safe: `A-Za-z0-9`) and configurable length + prefix.
- **Rationale**: `nanoid` is the most popular NanoID implementation in the Rust ecosystem (2M+ downloads). It uses `getrandom` for cryptographically secure randomness. Lightweight — zero required dependencies beyond `getrandom`.
- **Alternatives considered**: Rolling custom NanoID implementation (rejected — unnecessary maintenance burden), `uuid` crate (rejected — UUID format is harder to read/type than NanoID).

### Enum Serialization

- **Decision**: Use `serde` with `#[derive(Serialize, Deserialize)]` and `#[serde(rename_all = "lowercase")]`.
- **Rationale**: Matches the existing MVP-1 json-server API convention where enum values are lowercase strings (`"available"`, `"in_use"`, `"business"`). Downstream services will serialize/deserialize enums in HTTP JSON bodies.
- **Alternatives considered**: `strum` + `Display`/`FromStr` (rejected — requires manual mapping instead of derive), integer-backed enums (rejected — break API compatibility with MVP-1 mock data).

### Database Pool

- **Decision**: Use `sqlx::PgPool` with `sqlx 0.8` + `postgres` + `runtime-tokio` features.
- **Rationale**: sqlx is the de facto standard async PostgreSQL driver for Rust. Both Driver Service (Sprint 2.3) and Admin Service (Sprint 2.4) plan to use sqlx. Wrapping `PgPool` in ev-db allows shared pool configuration (connection pool size, timeouts, etc.) without duplicating setup code.
- **Alternatives considered**: `diesel` (ORM — rejected for Sprint 2.1; may be evaluated later for query building), `tokio-postgres` (lower-level — rejected; sqlx provides built-in compile-time query checking which is valuable for downstream services).

### Async Runtime

- **Decision**: Use `tokio` with default features (`rt-multi-thread`, `macros`, `sync`, `time`).
- **Rationale**: Required by sqlx. Tokio is the standard async runtime in the Rust ecosystem. Multi-thread runtime is needed for handling concurrent database requests in services.
- **Alternatives considered**: `async-std` (rejected — smaller ecosystem, less compatible with downstream dependencies), `smol` (rejected — need standard runtime features).

### Error Handling

- **Decision**: Use `thiserror` for deriving `std::error::Error` on custom error enums.
- **Rationale**: Idiomatic Rust pattern. Provides `Display` and `Error` implementations with zero boilerplate. Both crates will have custom error types for validation failures, connection errors, etc.
- **Alternatives considered**: `anyhow` (rejected — designed for application-level error handling, not library error types), custom manual `Error` impls (rejected — boilerplate-heavy).

### Workspace Layout

- **Decision**: `source/` as Cargo workspace root; `source/crates/ev-core/` and `source/crates/ev-db/` as members.
- **Rationale**: Mirrors the existing workspace structure where `source/apps/` holds JS apps. Keeping Rust crates under `source/crates/` avoids confusion and keeps the repo root clean. The workspace `Cargo.toml` sits at `source/Cargo.toml`.
- **Alternatives considered**: Top-level `Cargo.toml` at repo root (rejected — conflicts with JS tooling expecting root-level configs), `source/rust/` prefix (rejected — adds unnecessary nesting depth).

## Dependency Versions

| Dependency | Minimum Version | Notes |
|------------|----------------|-------|
| rustc | 1.85 | Edition 2024; stable toolchain |
| nanoid | 0.4 | Latest stable; no breaking changes expected |
| serde | 1.0 | With `derive` feature |
| serde_json | 1.0 | For enum round-trip tests |
| sqlx | 0.8 | With `postgres`, `runtime-tokio`, `macros` features |
| tokio | 1.0 | With `macros`, `rt-multi-thread` features |
| thiserror | 2.0 | Latest stable |

## Best Practices

- **`#![deny(warnings)]`** in all crates to enforce zero-warning policy.
- **Documentation comments** (`///`) on all public items — enforced by `#![deny(missing_docs)]` in library crates.
- **Unit tests** in same file (inline `#[cfg(test)] mod tests`) for tight code-test proximity.
- **`pub use`** re-exports in `lib.rs` for clean public API surface.
- **`Cargo.lock`** committed for workspace-level dependency resolution.
- **`clippy`** run in CI — `cargo clippy --all-targets -- -D warnings`.
