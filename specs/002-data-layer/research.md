# Research: Data Layer

**Feature**: Data Layer | **Branch**: `002-data-layer` | **Date**: 2026-06-10

## Decisions

### Database Driver: SQLx

- **Decision**: SQLx (async, compile-time checked queries)
- **Rationale**: SQLx provides compile-time query verification against the actual database schema, native async/tokio support, and direct PostgreSQL/PostGIS type mapping without an ORM layer. The inventory schema is simple enough that an ORM (Diesel) adds unnecessary complexity. SQLx's `pgvector` crate also supports PostGIS geography types via `postgis` feature.
- **Alternatives considered**: Diesel (heavy ORM, complex setup for spatial queries), tokio-postgres (raw — no compile-time checking, more boilerplate)

### Migration Framework: SQLx Migrate (built-in)

- **Decision**: SQLx built-in migration system (`sqlx::migrate!`)
- **Rationale**: SQLx ships with a file-based migration runner that exactly matches the Assumptions section (plain SQL files, timestamp prefix, tracking table). No external tool needed. Runs at application startup or via CLI.
- **Alternatives considered**: refinery (external crate, similar features, extra dependency), custom runner (unnecessary reinvention)

### Integration Test Database: testcontainers

- **Decision**: testcontainers crate with `postgis/postgis:16-3.4` image
- **Rationale**: Spins up a disposable PostGIS container per test run, matching US3 requirement ("no manual setup"). Avoids coupling tests to running Docker Compose state. Faster for CI than full compose stack.
- **Alternatives considered**: docker-compose in CI (slower, stateful), shared test DB (parallel test conflicts)

### Async Runtime: tokio

- **Decision**: tokio (current stable async runtime)
- **Rationale**: Industry standard for Rust async PostgreSQL. SQLx's async API is built on tokio by default. No competing runtime (async-std) needed.
- **Alternatives considered**: async-std (less ecosystem support)

### Workspace Layout

- **Decision**: Single workspace at `source/services/` with shared `libs/borne-data/` and per-service binaries
- **Rationale**: Minimal monorepo — shared library consumed via `path` dependency. Each service is a separate crate for independent compilation and testing.
- **Alternatives considered**: Separate repos per service (overhead for MVP), single crate (no separation of concerns)

## Open Questions

- None — all technology choices resolved against spec requirements and project context.

