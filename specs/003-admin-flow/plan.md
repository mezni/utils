# Implementation Plan: Admin Service Core Operations

**Branch**: `002-auth-service` | **Date**: 2026-06-19 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/003-admin-flow/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Implement the Admin Service core CRUD operations for partner, station, and charger management (Sprint 2 of MVP-1). The service will handle transactional writes to the `inventory` schema, synchronous materialized view refresh and Redis cache busting, and comprehensive audit logging. All mutations must respect scope restrictions (partner isolation) and support idempotency keys for duplicate prevention.

## Technical Context

**Language/Version**: Rust 1.88 (Actix-web framework)

**Primary Dependencies**:
- `actix-web`: Web framework for REST API endpoints
- `sqlx`: Compile-time type-checked database queries (macros only, no raw SQL)
- `serde`/`serde_json`: Serialization/deserialization
- `chrono`: Timestamp handling
- `uuid`: Entity identifier generation with prefix validation
- `reqwest`: For future external integrations (not used in MVP-1)
- `tracing`: Structured logging
- `redis`: Cache and idempotency key storage

**Storage**:
- PostgreSQL 16 with PostGIS (inventory schema for partners, stations, chargers)
- PostgreSQL 16 analytics_db (audit_log table)
- Redis (GIS tile cache invalidation and idempotency key store)

**Testing**: `cargo test` (unit + integration tests), `cargo clippy -- -D warnings`

**Target Platform**: Linux server (Docker container on port 3002)

**Project Type**: Web service (Actix-web microservice in monorepo)

**Performance Goals**:
- CRUD operations complete in under 1 minute
- Cache bust overhead <500ms after transaction commit
- Materialized view refresh within 2-5 seconds
- Support 10 concurrent partner administrators

**Constraints**:
- All multi-table writes wrapped in explicit transactions
- Cache bust happens AFTER tx.commit(), never before
- Redis invalidation in service orchestration layer, not repository
- X-User-Id and X-User-Roles from Traefik headers only (never from client)
- No raw SQL strings — sqlx macros only
- Idempotency keys UUID v4, 24h TTL in Redis
- Geographic data uses PostGIS with SRID 4326
- Entity IDs must use NanoID with prefix format (OPR-/STA-/CHG-)
- Scope enforcement prevents cross-partner mutations
- All mutations logged to analytics_db with before/after snapshots

**Scale/Scope**: MVP-1 Sprint 2 — 3 main CRUD endpoints (partner, station, charger), 2 read endpoints (get by ID), 1 endpoint (update), idempotency support on all POST, transactional consistency for multi-entity operations, comprehensive audit logging, Redis cache busting, materialized view refresh.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Core Principles (from constitution)

1. **Validation before optimization**
   - ✅ PASS: No OCPP, billing, smart charging, distributed events, or advanced observability in MVP-1. Scope is limited to core CRUD operations with basic logging.

2. **Strict service topology**
   - ✅ PASS: Admin Service (:3002) is the third Actix-web microservice as defined. No additional microservices introduced.

3. **Compile-time safety & type strictness**
   - ✅ PASS: All PostgreSQL queries use `sqlx::query!` compile-time macros. No raw SQL strings permitted. All Rust code uses `Option` and `Result` for error handling.

4. **Read/write separation & transactional integrity**
   - ✅ PASS: Driver Service handles read-optimized spatial queries. Admin Service handles all writes in explicit transactions with synchronous post-commit steps (MV refresh, Redis bust, audit log).

5. **Security & identity isolation**
   - ✅ PASS: Single Keycloak realm with granular roles (role:admin, role:partner). Clients only call Auth Service. Traefik validates JWTs locally via JWKS. Scope restrictions prevent cross-partner mutations. Soft delete only on infrastructure entities (stations, chargers, partners), never users or audit logs.

### Authentication & Token Lifecycle

- ✅ PASS: Auth Service is sole owner of `users` schema and Keycloak integration. Admin Service reads X-User-Id and X-User-Roles from Traefik headers (never from client body).

### Tech Stack & Platform Constraints

- ✅ PASS: Rust/Actix-web for backend services. Cargo workspace with db-models and validation crates. PostgreSQL 16 + PostGIS for database. Redis for cache and idempotency. Traefik for routing and JWT validation.

- ✅ PASS: Entity ID prefixes (NanoID) follow convention: OPR- (partner), STA- (station), CHG- (charger), USR- (user — owned by Auth Service only).

### Database Architecture

- ✅ PASS: platform_db with inventory schema (partners, stations, chargers, materialized views). keycloak_db owned exclusively by Keycloak. analytics_db for audit logs (written by Admin Service only).

### Development Workflow & Conventions

- ✅ PASS: Monorepo structure with source/services/admin-service. All endpoints prefixed with `/api/v1/`. Code formatting via rustfmt + clippy.

### Governance

- ✅ PASS: This implementation plan aligns with all constitutional provisions. No amendments required.

**CONCLUSION**: All constitution gates pass. Proceed to Phase 0 research.

## Project Structure

### Documentation (this feature)

```text
specs/003-admin-flow/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   ├── api-contracts.md
│   └── error-contracts.md
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
source/
├── services/
│   └── admin-service/           # Sprint 2 Admin Service (NEW)
│       ├── src/
│       │   ├── main.rs          # Actix-web entry point
│       │   ├── config.rs        # Environment variable configuration
│       │   ├── error.rs         # AuthError enum and ResponseError trait
│       │   ├── models/          # Domain models (partner, station, charger)
│       │   │   ├── mod.rs
│       │   │   ├── partner.rs
│       │   │   ├── station.rs
│       │   │   └── charger.rs
│       │   ├── routes/          # HTTP route handlers
│       │   │   ├── mod.rs
│       │   │   ├── partner.rs
│       │   │   ├── station.rs
│       │   │   └── charger.rs
│       │   ├── repositories/    # Database access layer
│       │   │   ├── mod.rs
│       │   │   └── inventory.rs
│       │   ├── services/        # Business logic orchestration
│       │   │   ├── mod.rs
│       │   │   └── admin_orchestrator.rs
│       │   ├── middleware/      # Custom middleware
│       │   │   ├── mod.rs
│       │   │   ├── idempotency.rs
│       │   │   └── audit.rs
│       │   ├── keycloak.rs      # Keycloak client (if needed for validation)
│       │   └── validation.rs    # Input validation
│       ├── tests/
│       │   ├── integration/
│       │   │   ├── partner_test.rs
│       │   │   ├── station_test.rs
│       │   │   └── charger_test.rs
│       │   └── unit/
│       │       ├── models_test.rs
│       │       ├── repositories_test.rs
│       │       └── services_test.rs
│       ├── Cargo.toml
│       ├── Cargo.lock
│       └── Dockerfile
├── crates/
│   ├── db-models/              # Shared database models (shared across services)
│   │   ├── src/
│   │   │   ├── partner.rs       # Partner DTOs
│   │   │   ├── station.rs       # Station DTOs
│   │   │   └── charger.rs       # Charger DTOs
│   │   ├── Cargo.toml
│   │   └── Cargo.lock
│   └── validation/             # Shared validation rules
│       ├── src/
│       │   ├── mod.rs
│       │   ├── partner.rs       # Partner validation rules
│       │   ├── station.rs       # Station validation rules
│       │   └── charger.rs       # Charger validation rules
│       ├── Cargo.toml
│       └── Cargo.lock
└── infra/
    ├── docker-compose.yml       # Updated with admin-service service
    └── postgres/
        └── init/
            └── 0001_init_schemas.sql  # Already exists from MVP-1
```

**Structure Decision**:
- **Option 2 (Web application)** was selected. The Admin Service is an Actix-web microservice with separate frontend apps (dashboard, web driver, mobile driver) as stated in the constitution.
- All source code lives under `source/services/admin-service/` with clear separation: `src/` for business logic, `tests/` for tests, `Cargo.toml` for dependencies.
- Shared domain models and validation rules are extracted into `crates/db-models` and `crates/validation` to avoid code duplication and enforce consistency across services (per constitution requirement for compile-time type-checked queries).
- Infrastructure definitions (docker-compose, PostgreSQL schema) remain in `source/infra/`.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| (None) | N/A | All constitution gates pass. |

