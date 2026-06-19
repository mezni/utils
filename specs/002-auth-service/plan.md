# Implementation Plan: Auth Service — Login, Refresh & Logout

**Branch**: `002-auth-service` | **Date**: 2026-06-19 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-auth-service/spec.md`

## Summary

Build the Auth Service — a Rust web service providing four endpoints (login, refresh, logout, and profile). Acts as the sole Keycloak proxy: no client or other service talks to Keycloak directly. Upserts a USR- user profile in the `users` schema on every successful authentication. Validates and propagates token audience claims without minting tokens.

## Technical Context

**Language/Version**: Rust (stable) — per constitution Section III

**Primary Dependencies**: Actix-web (HTTP framework), sqlx (compile-time checked queries), reqwest (HTTP client to Keycloak), serde (JSON), jsonwebtoken (JWT introspection), tokio (async runtime)

**Storage**: PostgreSQL 16 + PostGIS — `platform_db.users` schema, accessed via `auth_service_role` DB role

**Testing**: `cargo test` (unit + integration), `cargo clippy -- -D warnings`, integration tests against live Keycloak + Postgres in Docker

**Target Platform**: Linux (Docker container, `source/services/auth-service/`)

**Project Type**: Web service (Actix-web microservice)

**Performance Goals**: Login <2s p95, refresh <1s p95, 100 concurrent requests

**Constraints**: No service touches Keycloak except Auth Service; no raw SQL strings (sqlx macros only); no `unwrap()`/`expect()` outside test code; credentials, access_token, and refresh_token never logged

**Scale/Scope**: 4 endpoints (login, refresh, logout, me), 1 DB schema (`users`), single external integration (Keycloak), ~1.2K lines of service code

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Rule | Status | Notes |
|------|--------|-------|
| Validation before optimization — no Kafka/RabbitMQ/MQTT, no OCPP, no billing | ✅ PASS | Async patterns not needed; pure synchronous HTTP |
| Exactly 3 Actix-web services (Auth, Driver, Admin) | ✅ PASS | Auth Service is the first; within the 3-service topology |
| Compile-time sqlx macros, no raw SQL strings | ✅ PASS | All DB queries via sqlx macros |
| No service bypasses Traefik | ✅ PASS | Clients reach Auth Service through Traefik only |
| No service touches Keycloak except Auth Service | ✅ PASS | Core design principle |
| Auth Service does NOT mint tokens — Keycloak is sole issuer | ✅ PASS | Auth Service proxies Keycloak token responses |
| Credentials never logged or stored | ✅ PASS | Request body discarded after authentication |
| `users` schema owned exclusively by Auth Service | ✅ PASS | No other service reads/writes `users` |
| No `unwrap()`/`expect()` outside tests | ✅ PASS | To enforce in code review |
| No event bus / async outbox patterns | ✅ PASS | All sync HTTP |

## Project Structure

### Documentation (this feature)

```text
specs/002-auth-service/
├── plan.md              # This file
├── research.md          # Phase 0 — technology decisions
├── data-model.md        # Phase 1 — user profile entity
├── quickstart.md        # Phase 1 — dev setup guide
├── contracts/           # Phase 1 — API contracts
└── tasks.md             # Phase 2 — task breakdown
```

### Source Code

Backend microservice (single service — no frontend in Sprint 1):

```text
source/services/auth-service/
├── Cargo.toml
├── Dockerfile
├── src/
│   ├── main.rs                  # Actix-web entrypoint, router setup
│   ├── routes/                  # Request handlers
│   │   ├── mod.rs
│   │   ├── login.rs
│   │   ├── refresh.rs
│   │   ├── logout.rs
│   │   └── me.rs
│   ├── keycloak/               # Keycloak HTTP client
│   │   ├── mod.rs
│   │   └── client.rs
│   ├── db/                     # Database layer (sqlx)
│   │   ├── mod.rs
│   │   └── users.rs
│   ├── models/                 # Domain types
│   │   ├── mod.rs
│   │   ├── user.rs
│   │   └── auth.rs
│   └── error.rs                # Unified error handling
└── tests/
    ├── integration/
    │   ├── login_test.rs
│   ├── refresh_test.rs
│   └── logout_test.rs
```

**Structure Decision**: Single Cargo project under `source/services/auth-service/`. Shared crates (`source/crates/db-models/`, `source/crates/validation/`) will be introduced when both Auth and Admin services need them (Sprint 2).

## Complexity Tracking

No constitution violations. Standard 3-service topology, no additional complexity.
