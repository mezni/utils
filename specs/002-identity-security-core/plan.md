# Implementation Plan: Identity & Security Core

**Branch**: `002-identity-security-core` | **Date**: 2026-06-21 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-identity-security-core/spec.md`

## Summary

Implement Keycloak as the sole identity provider, JWT validation across all three services, RBAC on every endpoint, and JIT user provisioning. Add four CI security gates and audit logging for all authentication events.

## Technical Context

**Language/Version**: Rust 1.75+ (Cargo-based toolchain)

**Primary Dependencies**: keycloak-client (Keycloak Admin API), jsonwebtoken (JWT validation), reqwest (JWKS fetch), serde, serde_json, actix-web, tokio, sqlx

**Storage**: PostgreSQL (platform_db.users.user_profiles for JIT projection, analytics_db.telemetry.raw_events for audit log), Keycloak DB (PostgreSQL, auto-managed by Keycloak)

**Testing**: cargo test (unit/integration), cargo clippy (linting), cargo fmt (formatting)

**Target Platform**: Linux server (services), Docker containers (Keycloak), Traefik reverse proxy (gateway)

**Project Type**: Monorepo with microservices architecture (3 backend services)

**Performance Goals**: JWT validation < 100ms per request, user provisioning < 5s, CI pipeline < 15 minutes

**Constraints**:
- Service topology lock: exactly 3 services on ports 3000, 3001, 3002
- Identity dual system: Keycloak UUID for users, nanoid(12) with PREFIX for entities — no mixing
- Contract-first: domain-types → backend → frontend
- SQLx compile-time verification mandatory
- Single-writer analytics (driver-service only)
- Only auth-service MAY call Keycloak Admin API
- JWT role MUST match platform_db role at all times

**Scale/Scope**: 3 service modifications, 1 Keycloak deployment, ~25 implementation tasks

## Enforcement Kernel Specification

### CI Execution DAG

**Stage Order** (strict linear sequence with artifact passing):

```
Stage 1: format_check
  ↓ Passes
  artifact: format_check_report.json

Stage 2: type_check
  ↓ Passes, consumes format_check_report.json
  artifact: type_check_report.json

Stage 3: dependency_graph_validation
  ↓ Passes, consumes type_check_report.json
  artifact: dependency_graph.json

Stage 4: identity_validation
  ↓ Passes, consumes dependency_graph.json
  artifact: identity_validation_report.json

Stage 5: schema_validation
  ↓ Passes, consumes identity_validation_report.json
  artifact: schema_validation_report.json

Stage 6: sqlx_compile_check
  ↓ Passes, consumes schema_validation_report.json
  artifact: sqlx_prepare_state.json

Stage 7: analytics_write_gate
  ↓ Passes, consumes sqlx_prepare_state.json
  artifact: analytics_gate_report.json

Stage 8: integration_tests
  ↓ Passes, consumes analytics_gate_report.json
  artifact: test_results.json

Stage 9: build_success
  ↓ Passes, consumes test_results.json
  artifact: build_state.json
```

**Failure Propagation Rules**:
- Hard-stop: Any stage failure immediately aborts all subsequent stages
- Deterministic exit codes: 0=success, 1=failure, 2=skipped
- No partial success allowed
- Each stage logs detailed failure reason to CI output

**Artifact Passing Model**:
- Each stage produces strict JSON artifact on success
- Next stage consumes previous artifact as input
- No side effects between stages
- All artifacts stored in `.specify/ci-artifacts/` for audit trail

### Sprint 1 CI Additions

#### 1. Identity Validation Gate (CI-1.1)

**Input**: All Rust source files, migration files, and SQL migration definitions

**Algorithm**:
- Check users.user_profiles: FAIL if any non-UUID `user_id` column found in migration files
- Check auth tables: FAIL if any `nanoid` or `VARCHAR(15)` with nanoid-like CHECK constraint found in users schema
- Check entity tables (gis, inventory): FAIL if any UUID column used as primary key
- Validate no role field mixing between JWT role enum and platform_db role column values

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Identity validation passed"
}
```

**Failure Signature**: Exit code 1 with specific violations

---

#### 2. Keycloak Dependency Gate (CI-1.2)

**Input**: Cargo dependency tree and Rust source imports

**Algorithm**:
- Scan all Cargo.toml files for keycloak-client dependency
- FAIL if any service other than auth-service depends on keycloak-client
- Scan Rust source for `use keycloak` imports outside auth-service

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Keycloak dependency gate passed"
}
```

**Failure Signature**: Exit code 1 with violating service name

---

#### 3. RBAC Coverage Check (CI-1.3)

**Input**: Route definitions in all three services

**Algorithm**:
- Scan all route registration functions in each service's `src/main.rs` and `src/routes.rs`
- For each `.route(...)` call, verify a role guard middleware (`.wrap()` with role guard) is applied
- FAIL if any controller endpoint lacks a `#[Roles(...)]` decorator or equivalent role guard
- FAIL if any route is not present in the RBAC matrix defined in `contracts/rbac.md`
- Check that public routes (`GET /health`, `POST /api/v1/auth/login`) are explicitly whitelisted and excluded from role guard enforcement
- Expected coverage: 100%

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "coverage": "100%",
  "uncovered_routes": [],
  "summary": "All routes have role guards"
}
```

**Failure Signature**: Exit code 1 with list of uncovered routes

---

#### 4. Session Consistency Check (CI-1.4)

**Input**: JWT role claims and platform_db.user_profiles role values

**Algorithm**:
- Extract role from JWT validation test vectors
- Compare against expected role in platform_db for same user UUID
- FAIL if JWT role != database role

**Output**: JSON
```json
{
  "status": "passed"|"failed",
  "exit_code": 0,
  "summary": "Session consistency verified"
}
```

**Failure Signature**: Exit code 1 with role mismatch details

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Gate 1: Service Topology Lock (PASS)

**Constitution Requirement**: Exactly three services MUST exist, each on a fixed port: auth-service (3000), driver-service (3001), admin-service (3002). No additional services.

**Compliance Status**: ✅ PASS

**Justification**: This feature adds a Keycloak container and Traefik forward-auth middleware but does NOT add new application services. The three microservices remain on ports 3000/3001/3002.

**Verification**: No new service directories in `services/`. No port changes. Keycloak runs as infrastructure (Docker container with separate port).

---

### Gate 2: Identity Dual System (PASS)

**Constitution Requirement**: Two independent identity systems: Keycloak UUID (human) and Platform nanoid(12) with PREFIX (business). No mixing allowed.

**Compliance Status**: ✅ PASS

**Justification**: Keycloak UUID is used for users.user_profiles.user_id. Entity tables continue to use nanoid(12) with PREFIX. JWT sub claim maps to Keycloak UUID. No nanoid leaks into user tables, no UUID leaks into entity tables.

**Verification**: JWT sub claim validation checks format. CI identity validation gate enforces UUID-in-users-only rule. All entity identifiers remain nanoid with PREFIX.

---

### Gate 3: Data Ownership (PASS)

**Constitution Requirement**: Every data domain has exactly one owning service. Cross-service writes forbidden.

**Compliance Status**: ✅ PASS

**Justification**: users domain remains auth-service owned. Audit events follow the event bus pattern (ADMIN → BUS → ADB per constitution §11.4): auth-service publishes events to driver-service `POST /api/v1/telemetry/events`, and driver-service writes them to analytics_db. Auth-service NEVER directly writes to analytics_db. Service-to-service calls use Keycloak service account credentials.

**Verification**: CI analytics write gate ensures auth-service never connects to analytics_db. All internal API calls authenticated via client_credentials grant.

---

### Gate 4: Contract-First (PASS)

**Constitution Requirement**: Contract definition → Backend implementation → Frontend implementation in that order.

**Compliance Status**: ✅ PASS

**Justification**: This feature is entirely backend (services + infrastructure). The contracts for JWT structure, role definitions, and API endpoints are defined in the spec and data model before implementation.

**Verification**: domain-types crate will contain JWT claim structs and role enums. Services will depend on domain-types for these types.

---

### Gate 5: SQLx Compile-Time (PASS)

**Constitution Requirement**: All SQL queries MUST be compile-time verified via SQLx. CI MUST run `cargo sqlx prepare --check`.

**Compliance Status**: ✅ PASS

**Justification**: All SQL queries in JIT provisioning (INSERT/UPDATE user_profiles) and audit logging (INSERT raw_events) will use sqlx::query! macros for compile-time verification.

**Verification**: New migration and query files will use sqlx macros. CI sqlx_compile_check stage will catch any raw SQL strings.

---

### Gate 6: CI Enforcement (PASS)

**Constitution Requirement**: 9-stage CI pipeline with hard-stop on any failure.

**Compliance Status**: ✅ PASS

**Justification**: No changes to the 9-stage pipeline structure. Four new gates are added within the existing identity_validation and schema_validation stages, plus new stage content for the Keycloak dependency gate.

**Verification**: Updated CI validation scripts will be integrated into existing stage scripts. Pipeline still uses 9-stage structure.

---

### Gate 7: Forbidden Edges (PASS)

**Constitution Requirement**: No service→service imports, no frontend→backend imports, no shared-domain→services, no ui-kit→client-core, no circular dependencies.

**Compliance Status**: ✅ PASS

**Justification**: All services depend on domain-types for JWT claims and role types. No direct service→service imports. New shared-infra crate (for JWT validation middleware) is a shared dependency, not a service.

**Verification**: Dependency validation CI gate checks for forbidden edges. New shared-infra crate must not import any service crate.

---

### Gate 3b: Analytics Write Gate — Event Bus Pattern (PASS)

**Constitution Requirement**: driver-service ONLY can write to analytics_db.

**Compliance Status**: ✅ PASS

**Justification**: Auth-service publishes audit events to driver-service `POST /api/v1/telemetry/events` (event bus). Driver-service validates, deduplicates by idempotency_key, and writes to analytics_db. This follows the existing ADMIN → BUS → ADB pattern defined in constitution §11.4. Auth-service NEVER directly writes to analytics_db. Service-to-service calls use Keycloak service account credentials (auth-service-sa → driver-service-sa).

---

## Project Structure

### Documentation (this feature)

```text
specs/002-identity-security-core/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output
```

### Source Code (repository root)

```text
services/
├── auth-service/        # JWT validation, JIT provisioning, Keycloak sync, audit logging
│   ├── src/
│   │   ├── middleware/   # JWT validation middleware
│   │   ├── provisioning/ # JIT user upsert logic
│   │   ├── sync/         # Keycloak sync endpoint
│   │   └── audit/        # Audit event writer
│   └── migrations/
│       └── 0002_user_profiles_role.up.sql

├── driver-service/      # JWT validation (no Keycloak Admin API calls)
│   └── src/
│       └── middleware/   # JWT validation middleware

└── admin-service/       # JWT validation (no Keycloak Admin API calls)
    └── src/
        └── middleware/   # JWT validation middleware

apps/packages/domain-types/   # JWT claim structs, Role enum, event types

infrastructure/
├── docker-compose/
│   └── local.yml         # Added: keycloak service definition
├── keycloak/
│   ├── realm-export.json # Realm configuration
│   └── setup.sh          # Realm/client/role provisioning script
├── traefik/
│   ├── traefik.toml      # Added: forward-auth middleware
│   └── dynamic/
│       └── jwt-auth.yml  # JWT validation middleware config

tools/
├── ci_gate_identity.sh   # Identity validation gate (CI-1.1)
├── ci_gate_keycloak.sh   # Keycloak dependency gate (CI-1.2)
├── ci_gate_rbac.sh       # RBAC coverage check (CI-1.3)
└── ci_gate_session.sh    # Session consistency check (CI-1.4)
```

**Structure Decision**: JWT validation middleware lives in each service (not a shared crate) to avoid creating a service→shared-infra dependency that could become a circular dependency vector. Role types and JWT claim structs in domain-types as shared contracts.

## Complexity Tracking

No constitution violations in this feature.

### Enforcement Kernel Complexity

| Complexity Component | Why Needed | Simpler Alternative Rejected Because |
|---------------------|------------|-------------------------------------|
| JWKS caching layer | Avoid fetching keys on every request | No-cache approach would cause excessive Keycloak load and latency |
| JIT provisioning upsert | Keep platform_db in sync with Keycloak | Manual provisioning would be error-prone and fail at scale |
| Four new CI security gates | Prevent security regressions | Without gates, security bugs would reach production undetected |
| Audit event routing | Track all auth events for compliance | Without audit logging, security incidents would be undetectable |
