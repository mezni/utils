# Implementation Plan: Identity Core (MVP-2)

**Branch**: `001-identity-core` | **Date**: 2026-06-15 | **Spec**: `specs/001-identity-core/spec.md`

**Input**: Feature specification from `/specs/001-identity-core/spec.md`

## Summary

Implement authentication and authorization for the BorneMap platform using Keycloak as the identity provider. New users (drivers) register via email/password through auth-service → Keycloak integration. Local JWT validation via JWKS caching enables all services to authorize requests without runtime calls to Keycloak. Two realms: `bm-drivers` (driver accounts) and `bm-control` (admin/partner accounts). Account statuses: ACTIVE and DISABLED. Rate limiting (per-IP and per-account) protects login. Email verification deferred to post-MVP-2.

## Technical Context

**Language/Version**: Rust (edition 2021), Actix-Web 4, SQLx 0.7

**Primary Dependencies**:
- `jsonwebtoken` — JWT validation with JWKS
- `reqwest` — HTTP client for Keycloak Admin API
- `serde` / `serde_json` — serialization
- `tracing` / `tracing-subscriber` — structured JSON logging
- `nanoid` — USR-rol identifier generation
- `thiserror` / `anyhow` — error handling
- `actix-web` — web framework (matching existing driver-service)
- `actix-rt` — async runtime

**Storage**: PostgreSQL 17 + PostGIS, new `users` schema (5 tables: accounts, roles, account_roles, identity_providers, audit_log). SQLx migrations.

**Testing**: `cargo test`, SQLx for DB integration tests. Wiremock or mockito for Keycloak API mocking.

**Target Platform**: Linux server (Docker container, same stack as MVP-1)

**Project Type**: Web service (auth-service) + shared library crate (identity-core) for cross-service JWT validation

**Performance Goals**:
- Token validation <50ms (local JWKS cache, no network call)
- Registration under 30 seconds end-to-end (SC-001)
- 100 concurrent registration requests without errors (SC-005)
- Session invalidation within 5 seconds of logout (SC-006)

**Constraints**:
- No runtime calls to Keycloak for token validation (FR-012)
- Authorization Code + PKCE only, no password grant (FR-011)
- In-memory rate limiting (Article VII bans Redis)
- No business workflows in auth-service (FR-015)
- Secrets via environment variables only (FR-016, FR-019)

**Scale/Scope**: MVP-2 — initial production deployment. Single instance of each service.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Article | Status | Rationale |
|---------|--------|-----------|
| I. Database-First | ✅ PASS | Schema defined in data-model.md, migrations planned first |
| II. GIS Isolation | ✅ N/A | No GIS changes in this feature |
| III. Contract-First API | ✅ PASS | Contracts defined in contracts/api.md |
| IV. Mock Identity | ⚠️ SUPERSEDED | MVP-2 explicitly replaces mock identity. `usr-mvp1-fallback` phase-out acknowledged in spec assumptions. Exception granted by feature scope. |
| V. No Business Logic in Rust | ✅ PASS | Auth-service is thin HTTP-to-Keycloak bridge, no geospatial logic |
| VI. Single Service | ⚠️ SUPERSEDED | MVP-2 introduces auth-service. Exception granted by feature scope. |
| VII. No Microservice Sprawl | ⚠️ EXTENSION | Keycloak added as allowed component. No Redis/kafka/queues added. |
| VIII. OSM Import to gis Only | ✅ N/A | No OSM import changes |
| Entity ID Standard | ✅ PASS | USR- prefix added for accounts, ROL- for roles |
| Technology Stack | ✅ PASS | Actix-Web 4, SQLx 0.7, Rust edition 2021 all maintained |

**Gates requiring justification**:
- Article IV (Mock Identity) — Superseded: MVP-2 implements real identity
- Article VI (Single Service) — Superseded: auth-service is the second service
- Article VII — Extended: Keycloak is an allowed addition, no banned components

## Project Structure

### Documentation (this feature)

```text
specs/001-identity-core/
├── spec.md              # Feature specification (clarified)
├── plan.md              # This file
├── research.md          # Research and technology decisions
├── data-model.md        # Database schema and entities
├── quickstart.md        # Setup and verification guide
├── contracts/
│   ├── api.md           # REST API contracts
│   └── lib.md           # Shared identity crate interface
└── tasks.md             # Phase 2 output (NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
source/
├── services/
│   ├── Cargo.toml               # Workspace config (add auth-service, identity-core)
│   ├── auth-service/
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── routes/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── register.rs
│   │   │   │   ├── login.rs
│   │   │   │   ├── logout.rs
│   │   │   │   ├── refresh.rs
│   │   │   │   ├── me.rs
│   │   │   │   ├── admin.rs
│   │   │   │   └── health.rs
│   │   │   ├── services/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── registration.rs
│   │   │   │   └── session.rs
│   │   │   ├── middleware/
│   │   │   │   └── rate_limiter.rs
│   │   │   └── errors.rs
│   │   └── migrations/
│   │       └── 001_create_users_schema.sql
│   ├── libs/
│   │   └── identity-core/
│   │       ├── Cargo.toml
│   │       └── src/
│   │           ├── lib.rs
│   │           ├── jwt.rs
│   │           ├── claims.rs
│   │           ├── admin_client.rs
│   │           └── middleware.rs
│   └── shared/                   # Existing — no changes needed
├── infra/
│   ├── keycloak/
│   │   ├── init-keycloak.sh      # Realm/client/role setup
│   │   ├── create-realm.sh
│   │   ├── create-client.sh
│   │   └── create-role.sh
│   └── docker-compose.yml        # Add keycloak and auth-service
└── database/                     # Existing — no schema changes needed
```

**Structure Decision**: Follows existing workspace pattern — auth-service as a new member alongside driver-service, identity-core as a new lib alongside geo-core/db-core.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Article IV superseded | MVP-2 replaces mock identity with real auth | Using mock identity for MVP-2 would block all P1 user stories |
| Article VI superseded | auth-service is intentionally the 2nd service | Merging into driver-service violates FR-015 (no business logic in identity) |
| Keycloak added (Art VII extension) | External identity provider required for OIDC/PKCE | Building custom auth violates FR-011 (browser redirect flow mandate) |
