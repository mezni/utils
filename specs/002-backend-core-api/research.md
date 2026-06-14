# Research: MVP-1 Sprint 1 — Backend Core API (driver-service)

**Date**: 2026-06-13  
**Status**: Consolidated — all unknowns resolved

## 1. Crate Versions

| Crate | Version | Notes |
|---|---|---|
| tokio | `1.52` | LTS patch stream; feature `"full"` |
| axum | `0.8` | Requires MSRV 1.80; uses `axum::serve()` (no axum::Server) |
| sea-orm | `2.0` | Use latest 2.0.x RC or stable; features: `sqlx-postgres`, `runtime-tokio-native-tls`, `macros` |
| serde | `1.0` | Feature `"derive"` |
| serde_json | `1.0` | — |
| tracing | `0.1` | — |
| tracing-subscriber | `0.3` | Feature `"env-filter"` |
| thiserror | `2.0` | — |
| dotenvy | `0.15` | Lightweight .env loader |
| utoipa | `5.5` | Feature `"axum"` for axum integration |
| utoipa-swagger-ui | `9.0` | Feature `"axum"` |
| tower | `0.5` | For `ServiceExt::oneshot` in integration tests |
| mockall | `0.14` | Repository trait mocking |

**Decision**: Use SeaORM 2.x (latest stable or RC). SeaORM manages its own sqlx dependency.

## 2. Testing Strategy

| Layer | Approach | Tooling |
|---|---|---|
| Handler unit | Tower `oneshot` with mock state | `tower::ServiceExt` |
| Service unit | Injected mocked repository trait | `mockall` |
| Repository unit | Real PG via `#[sqlx::test]` | `sqlx::test` + fixtures |
| API integration | Tower `oneshot` + real PG | `sqlx::test` + `tower` |

**Database**: Use `#[sqlx::test]` macro with SQL fixture files (`.sql` under `tests/fixtures/`). This creates isolated temp databases per test, runs migrations, then seeds fixture data. Simpler than testcontainers for MVP-1.

**Key principle**: `mockall` for fast unit tests (service logic validation), `#[sqlx::test]` with real PostGIS for integration tests (query correctness, geospatial behavior).

## 3. Configuration Approach

**Pattern**: `dotenvy` for `.env` loading + `std::env` for runtime access + typed `Config` struct with defaults.

**Why NOT figment**: Only 6-7 env vars at MVP-1 scale. Manual `std::env::var()` with a `Config::from_env()` constructor is clearer, faster to compile, and has zero dependency overhead.

**Env vars**:

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | — | PostGIS connection string |
| `HOST` | No | `0.0.0.0` | Server bind address |
| `PORT` | No | `3000` | Server listen port |
| `DB_POOL_SIZE` | No | `10` | Max connections in pool |
| `DB_CONNECT_RETRIES` | No | `3` | Startup retry count (exponential backoff) |
| `DB_RETRY_BASE_DELAY_MS` | No | `1000` | Initial retry delay in ms |
| `RUST_LOG` | No | `info` | Tracing log level filter |

## 4. Logging Configuration

**Stack**: `tracing` + `tracing-subscriber` with `EnvFilter`.

Initialization pattern:
```rust
tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::new(&config.rust_log))
    .init();
```

Log output format: Human-readable text (default) for development. JSON structured logging can be enabled later (MVP-3+) by switching to `tracing_subscriber::fmt::json()`.

## 5. API Documentation (OpenAPI)

**Decision**: Use `utoipa` (5.x) + `utoipa-swagger-ui` (9.x) for automatic OpenAPI 3.0 generation.

**Rationale**: Provides compile-time OpenAPI spec generation from Rust types, with a Swagger UI endpoint. The `"axum"` features on both crates enable native axum integration with path/query parameter extraction. This is the de facto standard for axum API documentation.

## 6. Error Handling Pattern

Per Constitution V (Architecture Discipline):
- Typed errors via `thiserror` deriving `Display` + `Error`
- Wrap in domain-specific error enum (`DomainError`)
- Handler layer maps domain errors to HTTP status codes
- Never `unwrap()` or `expect()` (enforced by linting)

Response body shape for errors:
```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Station with id 'STA-99999' not found"
  }
}
```

## 7. Architecture Pattern Confirmation

Per Constitution V and spec:
- **Handler layer**: Axum async handlers, validate input, call service, map to HTTP response
- **Service layer**: Business logic, validation rules, orchestration
- **Repository layer**: SeaORM query execution, PostGIS isolation (`ST_DWithin`, `ST_Distance`)
- **Models**: `serde`-serializable domain structs with `utoipa` schema attributes

## 8. Key Decisions Summary

| Decision | Choice | Rationale |
|---|---|---|
| Async runtime | tokio 1.52 | Required by spec (FR-008) and constitution |
| HTTP framework | axum 0.8 | Constitution-mandated via tokio ecosystem, best PostGIS support |
| ORM | SeaORM 2.x | Constitution-mandated, manages sqlx/pooling internally |
| Test DB | `#[sqlx::test]` + fixtures | Isolated per-test DBs, no external dependency (Docker) |
| Config loading | dotenvy + std::env | Simple, 0 extra deps for 6 vars |
| Logging | tracing + tracing-subscriber | Standard for tokio ecosystem |
| API docs | utoipa 5.x | Compile-time OpenAPI spec, zero-runtime overhead |
| Mock repository | mockall | De facto Rust mocking library, async-friendly |
| Error type | thiserror | Minimal boilerplate, clean enum-based domain errors |
