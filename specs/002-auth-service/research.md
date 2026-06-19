# Research: Auth Service Technology Decisions

## Decision 1: Web Framework

**Decision**: Actix-web

**Rationale**: The constitution (Section III) mandates Actix-web for all backend services. Actix-web provides actor-based concurrency, strong compile-time type checking, and mature middleware ecosystem. Using a single framework across all three services ensures consistent patterns and shared knowledge.

**Alternatives considered**:
- Axum (Tokio-native) — not selected; constitution mandates Actix-web
- Rocket — less mature async ecosystem
- warp — lower-level, more boilerplate for the HTTP proxy pattern

## Decision 2: Database Access

**Decision**: sqlx with compile-time query checking

**Rationale**: The constitution requires compile-time type-checked sqlx macros. This catches schema mismatches at build time rather than runtime. The `auth_service_role` DB role enforces schema-level access to the `users` schema only.

**Alternatives considered**:
- Diesel ORM — heavier, more complex for simple upsert patterns
- SeaORM — additional abstraction layer; constitution requires sqlx macros

## Decision 3: Keycloak HTTP Client

**Decision**: reqwest with connection pooling

**Rationale**: reqwest is the de facto standard Rust HTTP client, providing connection pooling, TLS support, and async/await. The Auth Service acts as a reverse proxy to Keycloak's token endpoint — reqwest's streaming response support minimizes latency.

**Alternatives considered**:
- Hyper (lower-level) — unnecessary complexity for simple HTTP proxy
- ureq (blocking) — conflicts with Actix-web's async runtime

## Decision 4: JWT Handling

**Decision**: jsonwebtoken crate for token introspection, Keycloak JWKS as source of truth

**Rationale**: While the Auth Service does not mint tokens, it validates the audience (`aud`) claim on tokens returned from Keycloak. The `jsonwebtoken` crate provides decoding and validation without needing to verify signatures (Traefik handles signature validation at the gateway). The Auth Service focuses on audience propagation.

**Alternatives considered**:
- biscuit — more complex JOSE implementation
- Manual base64 decode — fragile, no schema validation

## Decision 5: Error Handling Pattern

**Decision**: Unified error enum with Actix-web `ResponseError` trait

**Rationale**: The spec defines four error contracts (400, 401, 503, 500) with specific JSON bodies. A single `AuthError` enum implementing `actix_web::ResponseError` ensures consistent serialization and HTTP status codes across all endpoints.

## Decision 6: Integration Testing

**Decision**: Integration tests against live Keycloak + Postgres in Docker

**Rationale**: The auth flow involves three systems (Auth Service, Keycloak, Postgres) — unit testing with mocks would miss critical integration bugs. Tests spin up Keycloak via testcontainers or the existing Compose stack, perform real token exchanges, and verify DB upserts.

**Test scenarios**:
- Login with valid credentials → token pair + USR- row
- Login with invalid password → 401
- Login with unknown user → 401
- Refresh with valid token → new token pair
- Refresh with expired token → 401
- Refresh with malformed token → 400
- Logout with valid token → session revoked
- Logout with already-expired token → 200 (idempotent)
- Keycloak unreachable → 503
- All 3 endpoints defined in the error contract table

## Decision 7: Cargo Workspace Placement

**Decision**: Standalone Cargo project under `source/services/auth-service/`

**Rationale**: Shared crates (`db-models`, `validation`) are introduced when the Admin Service begins (Sprint 2). Starting with a standalone project avoids premature abstraction while keeping the path consistent with the final layout.

**Alternatives considered**:
- Workspace from day one — adds `Cargo.toml` workspace overhead for a single-member workspace
- Into existing `source/crates/` — Auth Service is a runnable binary, not a library
