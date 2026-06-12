# Implementation Plan: Backend Services

**Branch**: `002-backend-services` | **Date**: 2026-06-11 | **Spec**: specs/002-backend-services/spec.md

**Input**: Feature specification from `specs/002-backend-services/spec.md`

## Summary

Implement two Rust/Actix microservices (driver-service :8080, admin-service :8081) against the existing Phase 1 databases. Driver-service exposes station discovery APIs (list, nearby, detail — read-only). Admin-service exposes station management CRUD (create, update, soft-delete) and event ingestion (single + batch all-or-nothing). Both services share the `ev-db`, `ev-core` crates from `source/services/shared/`. Tests follow Constitution III (TDD, 80%+ unit, 100% contract, integration).

## Technical Context

**Language/Version**: Rust 1.80+ (stable channel, 2024 edition)

**Primary Dependencies**:
- actix-web 4 — HTTP framework
- sqlx 0.8 — async PostgreSQL driver with compile-time query checking
- serde / serde_json — JSON serialization
- chrono — ISO 8601 timestamps
- geozero / postgis — spatial types and ST_DWithin queries
- uuid — nanoid generation (or custom nanoid crate)
- tokio — async runtime
- tracing + tracing-actix-web — structured logging

**Storage**: PostgreSQL 16 + PostGIS 3.4
- platform_db (port 5432) — inventory schema (stations, chargers, partners)
- analytics_db (port 5433) — raw_events (append-only)
- driver-service connects ONLY to platform_db (Constitution II)
- admin-service connects to platform_db (writes) + analytics_db (writes)

**Testing**:
- Unit tests: `cargo test` with 80%+ line coverage
- Contract tests: `cargo test --test contract_*` covering all API endpoints (100%)
- Integration tests: separate test database, verify inter-service contracts
- TDD workflow: test-first, Red-Green-Refactor (Constitution III)

**Target Platform**: Linux Docker containers (alpine-based Rust images)

**Project Type**: Web service (two microservices + shared library crate)

**Performance Goals**:
- Discovery endpoints: <100ms p95 with 1000 stations
- Station create (5 chargers): <200ms
- Batch ingestion (100 events): <500ms

**Constraints**:
- All endpoints prefixed `/api/v1/` (Constitution V)
- Consistent error shape: `{ "error": { "code", "message", "details?" } }`
- Batch events all-or-nothing (FR-017)
- Soft-delete on stations (deleted_at), hard-exclude from discovery
- Station IDs: server-generated nanoids with STA- prefix
- No authentication in Phase 2
- No rate limiting in Phase 2

**Scale/Scope**: 2 microservices, 10 API endpoints, MVP-1 Phase 2

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Justification |
|-----------|--------|---------------|
| I. UX-First | ⏭️ N/A | Backend-only phase; UX-First applies to frontends |
| II. Domain-Driven Services | ✅ PASS | driver-service reads platform_db only; admin-service writes platform_db + analytics_db |
| III. Test-First (NON-NEGOTIABLE) | ✅ PASS | Spec includes FR-018 (80%+ unit), FR-019 (100% contract), FR-020 (integration) |
| IV. Source-Rooted Codebase | ✅ PASS | All code under `source/services/{driver-service,admin-service,shared}` |
| V. Immutable Data & Append-Only | ✅ PASS | gis read-only, analytics_db append-only, soft-delete, nanoids, /api/v1/ |

**No violations. All gates pass.**

### Re-Check Post Phase 1

| Principle | Status | Justification |
|-----------|--------|---------------|
| I. UX-First | ⏭️ N/A | Backend-only phase |
| II. Domain-Driven Services | ✅ PASS | driver-service reads platform_db (discovery); admin-service writes platform_db (management) + analytics_db (events). No overlap. |
| III. Test-First | ✅ PASS | Spec FR-018/019/020 address all targets. Contract tests with actix_web::test. Test DB via ev-db/test_db helper. TDD workflow defined. |
| IV. Source-Rooted Codebase | ✅ PASS | All Rust code under `source/services/`. Dockerfiles in `infra/docker/`. |
| V. Immutable Data & Append-Only | ✅ PASS | gis schema read-only. analytics_db RULEs enforced. Soft-delete on station. Nanoid IDs with entity prefixes. ISO 8601. /api/v1/ prefix on all endpoints. |

**No violations. All gates pass.**

## Project Structure

### Documentation (this feature)

```text
specs/002-backend-services/
├── plan.md              # This file
├── research.md          # Phase 0 output — technology decisions, patterns
├── data-model.md        # Phase 1 output — entities, fields, relationships
├── quickstart.md        # Phase 1 output — how to run and test
├── contracts/           # Phase 1 output — API contracts, environment contracts
└── tasks.md             # Created by /speckit.tasks
```

### Source Code (repository root)

```text
source/
├── services/
│   ├── shared/
│   │   ├── ev-core/        # Domain types: Station, Charger, Event, errors
│   │   ├── ev-db/          # DB connection pooling, migration runner, query helpers
│   │   └── ev-auth/        # (stub — Keycloak integration is MVP-3)
│   ├── driver-service/     # Actix-web :8080 — discovery APIs (read-only)
│   │   └── src/
│   │       ├── main.rs
│   │       ├── routes/     # Route handlers
│   │       ├── services/   # Business logic
│   │       └── tests/      # Unit + contract tests
│   └── admin-service/      # Actix-web :8081 — management + events
│       └── src/
│           ├── main.rs
│           ├── routes/     # Route handlers
│           ├── services/   # Business logic
│           └── tests/      # Unit + contract tests

infra/
├── docker-compose.yml      # Adds service containers (Phase 2 update)
└── migrations/             # Existing Phase 1 migrations (no new ones needed)
```

## Complexity Tracking

No violations — all 5 constitution gates pass without justification needed.

## Phase 0: Research

### Unknowns to Resolve

1. Determines whether to use sqlx compile-time checked queries vs runtime queries vs diesel ORM
2. Cargo workspace structure — how to organize shared crates + services in one workspace
3. Contract test framework — reqwest vs actix-test vs pact-verifier
4. Nanoid generation — uuid v7 vs nanoid crate vs custom
5. Database connection pool sizing for two services
6. Structured logging setup — tracing-subscriber vs slog vs actix default
7. Actix-web middleware stack — CORS, request ID, logging, error handling
8. Migration strategy — do Phase 1 migrations need to be run by services or externally?

### Dependencies

- sqlx best practices with PostGIS / geography types
- Actix-web 4 request-scoped state and middleware patterns
- Docker multi-stage builds for Rust services (alpine + sccache)
- Test database setup — sqlx migrate run in test harness

### Research Plan

1. **Cargo workspace structure** — review existing `source/services/shared/` and design workspace Cargo.toml
2. **sqlx + PostGIS** — how to query geography columns, ST_DWithin, distance ordering
3. **Actix-web patterns** — shared state, middleware, error response helpers
4. **Contract testing** — actix-test vs reqwest vs pact for Rust HTTP contract tests
5. **Docker packaging** — multi-stage Rust builds with sqlx compile-time checks
6. **Nanoid strategy** — server-side ID generation for stations and chargers

### Generate Research

Launch research agents for each unknown above, consolidate findings in research.md.
