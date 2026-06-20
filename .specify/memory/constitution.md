<!--
Sync Impact Report
==================
Version Change: Template → 1.0.0
Modified Principles: N/A (initial constitution)
Added Sections:
  - Core Principles (5 principles)
  - Technology Constraints
  - Execution Standards
  - Governance
Removed Sections: None
Templates Requiring Updates: ✅ All aligned
  - plan-template.md: Constitution checks aligned
  - spec-template.md: Scope requirements aligned
  - tasks-template.md: Task categorization aligned
  - All command files: References validated
Follow-up TODOs: None
-->

# BorneMap Constitution

## Core Principles

### I. OpenAPI-First Architecture

All API development MUST follow an OpenAPI-first design methodology where `api/openapi/*.yaml` files are the authoritative source of truth. No backend or frontend code may be generated before the corresponding OpenAPI specification is complete and validated.

**Rules:**
- OpenAPI definition precedes implementation by definition
- Schema second, implementation third approach enforced
- No endpoints exist without OpenAPI timestamp
- All response bodies validated against OpenAPI schemas
- No dynamic SQL generation before contract validation

**Rationale:** OpenAPI-first ensures contract-driven development, eliminates integration drift, and provides a single source of truth for all API consumers.

### II. Deterministic Sprint Lifecycle

Every sprint MUST follow a 6-phase deterministic lifecycle with zero deviation:

1. **Sprint Ingestion** - Validate against architecture.txt, constitution.txt, guardrails.txt
2. **Contract Design** - OpenAPI-first gate before any code
3. **System Design** - DB schema, service responsibilities, API↔DB mapping
4. **Implementation** - Backend (Rust/Actix), Frontend (React/Expo)
5. **Code Review Gate** - 6-category audit before tests
6. **Testing Enforcement** - Unit, integration, E2E all must pass

**Rules:**
- No speculative features allowed
- No cross-sprint logic
- No extra services or schemas without explicit approval
- Sprint completion requires zero CRITICAL/HIGH bugs

**Rationale:** Deterministic execution eliminates scope creep, ensures quality gates are enforced, and maintains architectural integrity across all sprints.

### III. Contract-Driven Development

Nothing may be implemented before OpenAPI contracts are complete. This is a non-negotiable blocking condition for all sprint execution.

**Rules:**
- API-first gate: Contract design must complete before code generation
- Schema generation follows contract definitions
- No manual ID assignment (must use nanoid utilities)
- No hardcoded IDs in database migrations
- All responses validated against OpenAPI schemas

**Rationale:** Contract-driven development prevents implementation drift, ensures all stakeholders agree on API behavior before coding begins, and facilitates automatic client generation.

### IV. Identity System Enforcement

All entity identifiers MUST follow the canonical format: `<ENTITY_PREFIX>-nanoid(12)`

**Entity Prefixes:**
- partners: PRT
- stations: STN
- chargers: CHG

**Rules:**
- IDs generated via shared nanoid(12) utility only
- Manual ID assignment is forbidden
- IDs are opaque (no business logic parsing of prefix)
- Database enforces format: `CHECK (id ~ '^(PRT|STN|CHG)-[A-Za-z0-9_-]{12}$')`
- No hardcoded entity IDs in codebase
- Frontend must validate ID format on client side

**Rationale:** Deterministic ID generation prevents collisions, enables distributed-safe operations, and enforces strict data integrity across the system.

### V. Architecture Isolation

System architecture MUST maintain rigid service boundaries with zero cross-contamination.

**Service Topology (Immutable):**
- Auth Service (:3000) - exclusive users schema owner
- Driver Service (:3001) - read-only inventory views, Redis cache
- Admin Service (:3002) - exclusive inventory schema write access

**Frontend Applications (Fixed):**
- mobile-driver - Expo SDK 54
- web-driver - React + Leaflet
- dashboard - React + shadcn/ui

**Database Isolation:**
- platform_db (gis, inventory, users schemas)
- keycloak_db - isolated, no app access
- analytics_db - write-only event logs

**Rules:**
- No cross-service schema access
- No external integrations in validation phase
- No event brokers (Kafka, RabbitMQ, NATS)
- No distributed tracing or service mesh
- Docker Compose is only orchestration system allowed

**Rationale:** Isolation ensures maintainability, simplifies testing, prevents architectural drift, and allows independent scaling of services.

## Technology Constraints

### Backend
- **Language:** Rust (Actix-web only)
- **Database Access:** SQLx compile-time queries only (no raw SQL strings)
- **Shared Code:** Cargo workspace crates only (no frontend dependencies)
- **Testing:** Unit tests mandatory, integration tests for service boundaries

### Frontend
- **Mobile:** Expo SDK 54 (locked), React Native
- **Web:** React 18+, TypeScript strict mode
- **UI:** shadcn/ui (dashboard), Leaflet (web-driver)
- **Shared Logic:** TypeScript packages (no service imports)
- **API Access:** OpenAPI-generated client only (no direct fetch/axios)

### Database
- **Platform:** PostgreSQL 16 + PostGIS
- **Schemas:** gis, inventory, users (strict ownership)
- **Spatial:** GEOGRAPHY(Point, 4326) for stations only
- **Indexing:** GIST required for spatial data
- **Migrations:** SQLx macro-based (no manual migration files)

### Cache
- **Redis:** Driver Service exclusive responsibility
- **Usage:** Spatial tile snapshots, read-through caching
- **Cache Invalidation:** Admin Service ONLY (synchronous events)

### Identity
- **Provider:** Keycloak (single realm: bornemap)
- **Clients:** mobile-driver-app, web-driver-app, admin-dashboard
- **Authorization:** Role-based (driver, partner, admin)
- **Tokens:** JWT via JWKS (no runtime Keycloak calls)

## Execution Standards

### Code Review Gates (6-Category Audit)

Every sprint MUST pass through mandatory code review:

1. **Architecture Violations**
   - Service boundary leaks
   - Schema misuse
   - Unauthorized dependencies

2. **OpenAPI Drift**
   - Mismatch between contract and implementation
   - Missing endpoints
   - Incorrect DTO mapping

3. **Database Safety**
   - Raw SQL usage
   - Missing SQLx compile-time enforcement
   - Schema violations

4. **Identity Violations**
   - Invalid nanoid(12) format
   - Incorrect prefix usage
   - Hardcoded IDs

5. **Frontend Violations**
   - Direct fetch/axios usage
   - Bypassing OpenAPI client
   - State duplication of backend logic

6. **Security Violations**
   - Missing input validation
   - Unsafe trust boundaries
   - Cross-service data leakage

### Testing Enforcement

A sprint is NOT complete unless all tests pass:

- **Unit Tests:** Domain logic correctness, service-level validation
- **Integration Tests:** DB↔service correctness, API contract compliance
- **E2E Tests:** Full user journey, frontend → backend → DB flow

**Test Failure Rule:** ANY test failure = sprint automatically INVALID, bug cycle restarts, no progression allowed.

### Session Discipline

Before any implementation:
- Read GUARDRAILS.md
- Read SYSTEM_STATE.md
- Read OpenAPI spec
- Identify correct service boundary

After any implementation:
- Update SYSTEM_STATE.md
- Update roadmap_status.md
- Update sprint_backlog.md
- Never leave unresolved TODOs

## Governance

### Amendment Procedure

Constitution changes follow semantic versioning:
- **MAJOR:** Backward incompatible governance/principle removals or redefinitions
- **MINOR:** New principle/section added or materially expanded guidance
- **PATCH:** Clarifications, wording fixes, non-semantic refinements

**Process:**
1. Proposed change documented in ADR (Architecture Decision Record)
2. All stakeholders review and approve
3. Version increment according to impact
4. Template and dependent artifacts updated
5. Constitution file updated with sync impact report

### Compliance Review

- All PRs must verify compliance with all principles
- Architecture violations = HARD BUILD FAILURE
- Code review gates must be passed before merge
- Deterministic lifecycle enforced by SpecKit CI validator

### Runtime Guidance

All runtime development guidance is maintained in:
- `GUARDRAILS.md` - Execution standards & rules
- `architecture.txt` - Architecture flowchart (absolute highest authority)
- `SYSTEM_STATE.md` - Current state tracking

Constitution supersedes all other practices; any deviation requires explicit amendment and approval.

**Version**: 1.0.0 | **Ratified**: 2026-06-19 | **Last Amended**: 2026-06-19
