<!--
  Sync Impact Report — Constitution Update
  Version change: 0.0.0 (template) → 1.0.0 (initial populated)
  Modified principles: All — first population from BorneMap Constitution v1.15.2
  Added sections: All principles and sections populated from project constitution
  Removed sections: None
  Templates requiring updates:
    - spec-template.md: ✅ Updated (see consistency check below)
    - plan-template.md: ✅ Updated
    - tasks-template.md: ✅ Updated
    - checklist-template.md: ⚠ No changes needed
  Follow-up TODOs: None
-->

# BorneMap Constitution

## Core Principles

### I. Service Topology Lock (NON-NEGOTIABLE)

Exactly three services exist: `auth-service` (3000), `driver-service` (3001),
`admin-service` (3002). No additional services may be introduced. No service
splitting or duplication allowed. New services require a Constitution upgrade.

Rationale: Prevents uncontrolled system expansion and maintains deterministic
service topology.

### II. Clean Architecture (NON-NEGOTIABLE)

Every backend service MUST follow Clean Architecture with four layers:
`domain/` (pure logic, no frameworks/DB/HTTP), `application/` (use cases,
orchestration, DTO mapping), `infrastructure/` (SQLx, Redis, Keycloak,
external APIs), `presentation/` (HTTP, request validation, response mapping).

Rationale: Enforces separation of concerns and testability at every layer.

### III. Identity Dual-System (NON-NEGOTIABLE)

Two independent identity systems MUST NEVER overlap. Human identity uses
Keycloak UUID (`user_uuid`). Business entities use `PREFIX-nanoid(12)`
format (e.g., `STA-abc123def456`). Users MUST NOT use nanoid. Entities
MUST NOT use UUID. No mixing allowed.

Rationale: Prevents identity collision between human users and platform
entities across the entire system.

### IV. Data Ownership & Isolation

Each data domain is strictly owned by a single service: `users` schema →
`auth-service`, `gis` schema → `driver-service`, `inventory` schema →
`admin-service`. Cross-service database writes are FORBIDDEN. No service
trusts another service's runtime state.

Rationale: Guarantees data integrity, clear failure boundaries, and
independent service evolution.

### V. Security Engineering (NON-NEGOTIABLE)

Assume all inputs are hostile. Enforce strict validation, authentication
checks, authorization checks, least privilege, and schema validation at
every boundary. Never trust client input, cached state, or inter-service
communication. Trusted sources: Keycloak JWT, SQLx compile-time checks,
domain-types contracts, schema validation.

Rationale: Defense-in-depth for a geospatial platform handling operator
and driver data.

## Enforcement & CI

### CI Gates (HARD FAIL Conditions)

- Analytics database write violation → HARD FAIL
- Identity rule violation → HARD FAIL
- Service topology change → HARD FAIL
- SQLx `cargo sqlx prepare --check` failure → HARD FAIL
- Schema mismatch → HARD FAIL
- Dependency graph violation → HARD FAIL
- Migration ownership violation → HARD FAIL

### Testing Requirements

Every feature MUST include unit tests, integration tests, and contract
tests. Must cover: success paths, failure paths, authorization failures,
and boundary conditions. No feature is complete without tests.

### Migration Governance

Migrations are forward-only, no destructive rollback, SQLx compatible,
CI-validated. Ownership: `users` schema → auth-service, `gis` + analytics_db
→ driver-service, `inventory` schema → admin-service.

## Development Workflow

### Execution Model

Sprint-based with Speckit workflow: Spec → Plan → Tasks → Approval →
Implementation → Validation → Delivery Artifacts. Each sprint produces:
SYSTEM_STATE.md, roadmap_status.md, sprint_state.json, sprint_review.md,
validation_report.md, follow_up.md.

### API Design

REST only, versioned endpoints (`/api/v1/...`), consistent DTOs, structured
error responses. No duplicate ownership of endpoints. Operational endpoints
allowed everywhere: `/health`, `/ready`, `/live`, `/metrics`.

### Dependency Graph

Frontend: `ui-kit → domain-types → client-core`. Backend:
`services → shared-domain → shared-infra`. Service→service imports
FORBIDDEN. Frontend→backend imports FORBIDDEN. Circular dependencies
FORBIDDEN. Violation = HARD FAILURE.

## Governance

This Constitution is the System of Record and supersedes all other
practices and LLM output. Amendments require:
1. Documentation of the proposed change
2. Approval and version bump
3. Migration plan for affected systems
4. Consistency validation across all dependent artifacts
5. Compliance review

Versioning: MAJOR for backward-incompatible governance/principle changes;
MINOR for new principles/sections; PATCH for clarifications.

**Version**: 1.0.0 | **Ratified**: 2026-06-24 | **Last Amended**: 2026-06-24
