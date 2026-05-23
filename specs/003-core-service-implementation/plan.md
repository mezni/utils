# Implementation Plan: Core Service Implementation

**Branch**: `003-core-service-implementation` | **Date**: 2026-05-23 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/003-core-service-implementation/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Implement the core-service with basic CRUD operations for CoreEntity, JWT-based authentication integration, PostgreSQL persistence, health/metrics endpoints, and OpenAPI documentation. The service will be accessible via NGINX gateway at `/api/core/v1/` with URL path versioning and optimistic concurrency control.

## Technical Context

**Language/Version**: Rust 1.75+ with Actix Web 4.x

**Primary Dependencies**: Actix Web 4.x, SQLx 0.7, jsonwebtoken 8.x, validator 0.16, serde 1.x, utoipa 3.x, tokio-test 0.4, reqwest 0.11, lapin 2.x, config 0.13, thiserror 1.x, tracing 0.1

**Storage**: PostgreSQL with PostGIS extension, connection pooling (10 min, 20 max), SQLx migrations with compile-time verification

**Testing**: Rust built-in testing, testcontainers-rs for integration tests, reqwest for API testing

**Target Platform**: Ubuntu 22.04 LTS with Docker multi-stage builds for minimal secure images

**Project Type**: web-service (microservice)

**Performance Goals**: 100 concurrent requests, 100ms health check response 99% of time

**Constraints**: Must integrate with auth-service via JWT (dual validation), PostgreSQL connection pooling with graceful failure handling, optimistic concurrency control with version numbers

**Scale/Scope**: Core service with CRUD operations, part of 4-service microservices architecture

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Principle I: Clean Modular Architecture ✅
- Core-service responsibilities align with constitution: companies, stations, chargers, favorites, reviews, moderation, outbox, and audit event publishing
- REST for synchronous calls, RabbitMQ for asynchronous events
- PostgreSQL as system of record

### Principle II: Domain Model Integrity ✅
- Infrastructure hierarchy: Company → Station → Charger will be enforced
- Entity identifiers will use typed-prefix + nanoid format (CMP-<nanoid>, STA-<nanoid>, CHR-<nanoid>)
- "Company" concept replaces legacy "network" concept

### Principle III: Event Integrity via Outbox ✅
- Core-service is the sole event producer
- All domain events will be written to PostgreSQL outbox table in same transaction
- Relay worker will publish outbox rows to RabbitMQ

### Principle IV: Soft-Delete Discipline ✅
- Soft-delete applies to infrastructure entities: companies, stations, chargers
- Each table will carry `deleted_at TIMESTAMPTZ` column
- Read queries will include `WHERE deleted_at IS NULL` unless explicit admin/audit path

### Principle V: Security & Identity ✅
- JWT validation at gateway AND independently at core-service
- OAuth PKCE flow for interactive clients
- Secrets via environment variables only

### Principle VI: Observability ✅
- Structured JSON logs with correlation ID
- `/health` endpoint (liveness + readiness)
- `/metrics` endpoint exposing Prometheus-compatible metrics

### Principle VII: Quality & Testing Discipline ✅
- Unit tests for domain logic
- Integration tests for cross-component behavior (DB, queue, HTTP)
- Transaction tests for business-mutation + outbox writes
- Outbox tests for relay-worker delivery
- Audit-log tests
- Soft-delete tests

### GATE STATUS: ✅ PASSED - No constitutional violations detected

### Post-Design Validation

After completing the design phase (research, data model, contracts, and quickstart), we re-validate against the constitution:

#### Principle I: Clean Modular Architecture ✅
- Core-service responsibilities clearly defined and aligned with constitution
- Microservices architecture maintained with clear separation of concerns
- REST and RabbitMQ communication patterns established
- PostgreSQL as system of record maintained

#### Principle II: Domain Model Integrity ✅
- Infrastructure hierarchy (Company → Station → Charger) enforced in data model
- Typed-prefix + nanoid identifiers implemented
- "Company" concept replaces "network" concept
- Proper foreign key constraints ensure data integrity

#### Principle III: Event Integrity via Outbox ✅
- Outbox table designed with proper event structure
- Event publishing contracts defined
- RabbitMQ configuration specified
- At-least-once delivery with idempotency requirements

#### Principle IV: Soft-Delete Discipline ✅
- Soft-delete implemented for infrastructure entities (companies, stations, chargers)
- `deleted_at` fields included in data model
- Cascade soft-delete behavior defined
- Non-infrastructure entities (favorites, reviews, audit logs) use hard-delete

#### Principle V: Security & Identity ✅
- JWT validation at both gateway and service level
- Role-based access control defined in API contracts
- OAuth PKCE flow maintained
- Secrets via environment variables

#### Principle VI: Observability ✅
- Health check endpoints defined
- Metrics endpoints for Prometheus
- Structured logging with correlation IDs
- Audit log events defined

#### Principle VII: Quality & Testing Discipline ✅
- Testing categories defined in quickstart
- API contracts include comprehensive error handling
- OpenAPI documentation for all endpoints
- Concurrency control with optimistic locking

### FINAL GATE STATUS: ✅ PASSED - Constitution fully satisfied

## Project Structure

### Documentation (this feature)

```text
specs/003-core-service-implementation/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
services/
└── core-service/
    ├── src/
    │   ├── main.rs            # Application entry point
    │   ├── lib.rs             # Library root
    │   ├── models/            # Domain models (Company, Station, Charger, etc.)
    │   │   ├── mod.rs
    │   │   ├── company.rs
    │   │   ├── station.rs
    │   │   └── charger.rs
    │   ├── repositories/      # Data access layer
    │   │   ├── mod.rs
    │   │   ├── company_repository.rs
    │   │   ├── station_repository.rs
    │   │   └── charger_repository.rs
    │   ├── services/          # Business logic
    │   │   ├── mod.rs
    │   │   ├── company_service.rs
    │   │   ├── station_service.rs
    │   │   └── charger_service.rs
    │   ├── handlers/          # API endpoint handlers (controllers)
    │   │   ├── mod.rs
    │   │   ├── company_handler.rs
    │   │   ├── station_handler.rs
    │   │   └── charger_handler.rs
    │   ├── dto/               # Data transfer objects
    │   │   ├── mod.rs
    │   │   ├── company_dto.rs
    │   │   ├── station_dto.rs
    │   │   └── charger_dto.rs
    │   ├── events/            # Event publishing
    │   │   ├── mod.rs
    │   │   ├── event.rs
    │   │   └── publisher.rs
    │   ├── middleware/        # JWT validation, error handling
    │   │   ├── mod.rs
    │   │   ├── auth.rs
    │   │   └── error.rs
    │   └── utils/             # Utilities and helpers
    │       ├── mod.rs
    │       ├── database.rs
    │       └── validation.rs
    ├── tests/
    │   ├── unit/              # Unit tests
    │   │   ├── models/
    │   │   ├── services/
    │   │   └── handlers/
    │   ├── integration/       # Integration tests
    │   │   ├── api_tests.rs
    │   │   └── database_tests.rs
    │   └── e2e/               # End-to-end tests
    │       └── api_e2e_tests.rs
    ├── migrations/            # SQLx migrations
    ├── Cargo.toml             # Rust dependencies
    ├── Dockerfile             # Multi-stage Docker build
    └── .dockerignore          # Docker build exclusions
```

**Structure Decision**: Following the microservices architecture from the constitution, core-service will be implemented as a standalone Rust + Actix Web service with clear separation of concerns following Clean Architecture principles. Rust's module system naturally supports this structure with strong compile-time guarantees.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| None | N/A | N/A |
