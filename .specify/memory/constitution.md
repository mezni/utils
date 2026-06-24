<!--
  Sync Impact Report — Constitution Update
  ==========================================
  Version change: (template) → 1.15.2
  Modified principles: All 5 principles populated from template placeholders
  Added sections:
    - I. Project Identity & Mission
    - II. Service Topology Invariance
    - III. Data Ownership & Identity Separation
    - IV. Clean Architecture & Dependency Governance
    - V. Security & Testing Discipline
    - Architecture & Infrastructure Rules
    - Development Workflow & Quality Gates
  Removed sections: None (template was empty)
  Templates requiring updates:
    - .specify/templates/plan-template.md → ⚠ pending (Constitution Check section has placeholder)
    - .specify/templates/spec-template.md → ✅ no changes needed
    - .specify/templates/tasks-template.md → ✅ no changes needed
    - .opencode/commands/ → ⚠ verify no outdated references
  Follow-up TODOs: None. All placeholders filled.
-->

# BorneMap Constitution

## Core Principles

### I. Project Identity & Mission

BorneMap is an EV charging station discovery and management platform for the
Tunisian market. All system decisions MUST follow deterministic execution with
strict architectural enforcement and zero uncontrolled system expansion.
Constitution v1.15.2 governs all system decisions as the single source of truth
for architecture, data ownership, and enforcement rules.

### II. Service Topology Invariance

Exactly **three services only**:
- `auth-service` (port 3000) — authentication + user profiles
- `driver-service` (port 3001) — GIS + telemetry + analytics write
- `admin-service` (port 3002) — inventory + analytics read

No additional services may be introduced. No service splitting or duplication
allowed. The following require a Constitution upgrade: new services, new
databases, new event pipelines, new shared modules, new ownership domains.
Violation = ARCHITECTURE DRIFT.

### III. Data Ownership & Identity Separation

Two independent identity systems MUST NEVER overlap:
- **Keycloak UUID** (sub) — for human identity only
- **PREFIX-nanoid(12)** — for business objects only (STA-, CHG-, OPR-, EVT-)

Rules:
- Users MUST NOT use nanoid
- Entities MUST NOT use UUID
- All data domains are strictly owned by a single service
- Cross-service database writes are forbidden
- Ownership transfer requires Constitution upgrade
- Valid: `STA-abc123def456`, `CHG-k9x2lm8q1v7z`
- Invalid: `STA_nanoid(12)`, `STAabc123def456`, UUID in entity system

### IV. Clean Architecture & Dependency Governance

**Backend (Rust):**
- Layers: `domain/` → `application/` → `infrastructure/` → `presentation/`
- Domain: pure logic only, no DB, no HTTP, no frameworks
- Application: use-case orchestration only, DTO mapping allowed
- Infrastructure: SQLx only, Redis, external integrations
- Presentation: HTTP handling only, validation, response mapping

**Frontend (TypeScript):**
- Package dependency chain: `ui-kit` → `domain-types` → `client-core`
- `ui-kit`: UI only (components, layouts, tokens, accessibility primitives)
- `domain-types`: contract layer (DTOs, API contracts, event schemas, entity IDs)
- `client-core`: transport layer (API clients, React Query wrappers, session)

**Forbidden:**
- service → service imports
- frontend → backend imports
- circular dependencies
- business logic in ui-kit or client-core

Violation = HARD FAILURE.

### V. Security & Testing Discipline

Assume all inputs are hostile. Always enforce:
- authentication, authorization, schema validation
- least privilege, strict trust boundaries

Never trust:
- client input, cached state, inter-service communication

Every feature MUST include:
- unit tests, integration tests, contract tests
- success paths, failure paths, authorization failures, boundary conditions

SQLx Enforcement: ALL queries MUST be compile-time verified. CI MUST run
`cargo sqlx prepare --check`. Failure = HARD STOP.

Frontend Security:
- never trust client-side data
- validate all API responses
- assume all inputs are hostile
- never expose internal system structure
- never bypass backend authorization logic

## Architecture & Infrastructure Rules

### Database Architecture

**platform_db** (3 schemas):
| Schema | Owner | Tables |
|--------|-------|--------|
| `users` | auth-service | user_profiles (Keycloak sub linkage) |
| `gis` | driver-service | osm_charging_stations_temp, osm_charging_stations |
| `inventory` | admin-service | inventory-service API boundary |

**keycloak_db**: Identity provider storage only. No application logic allowed.

**analytics_db**:
| Service | Access |
|---------|--------|
| driver-service | READ/WRITE |
| admin-service | READ ONLY |
| auth-service | NO ACCESS |
| Frontend | NEVER access |

### API Ownership

- **auth-service**: authentication APIs, user profile APIs
- **driver-service**: GIS APIs, telemetry ingestion, nearby search APIs
- **admin-service**: inventory APIs, analytics dashboards

Business endpoints MUST NOT be duplicated.
Operational endpoints allowed everywhere: `/health`, `/ready`, `/live`, `/metrics`

### Migration Governance

- auth-service → `users` schema
- driver-service → `gis` + `analytics_db`
- admin-service → `inventory` schema
- forward-only migrations, no destructive rollback
- SQLx compatibility required, CI validation required

## Development Workflow & Quality Gates

### Spec Kit SDD Cycle

ALL work MUST follow: `specify → plan → tasks → implement → validate`

### Sprint Output Requirements

Every sprint MUST produce:
- SYSTEM_STATE.md
- roadmap_status.md
- sprint_state.json
- sprint_review.md
- validation_report.md
- follow_up.md

### CI Hard Fail Conditions

- analytics_db write violation
- identity violation (UUID vs nanoid cross-contamination)
- service topology change
- SQLx failure
- schema mismatch
- dependency violation
- migration violation

### Known Inherited Bugs

| ID | Issue | Rule |
|-----|-------|------|
| KNOWN-001 | Test stations leaking | filter `is_test = FALSE` |
| KNOWN-002 | Missing `deleted_at` | required field |
| KNOWN-003 | Duplicate nearby endpoint | driver-service owns |
| KNOWN-004 | CI grep brittle | regex-safe enforcement |

## Governance

Constitution supersedes all other practices. Amendments require documentation,
approval, migration plan, and Constitution version bump.

**Governance Hierarchy:**
1. SDEC v3.0 (highest authority)
2. BorneMap Constitution v1.15.2
3. Architecture docs
4. Sprint artifacts
5. LLM output (lowest authority)

Versioning policy:
- MAJOR: Backward incompatible governance/principle removals or redefinitions
- MINOR: New principle/section added or materially expanded guidance
- PATCH: Clarifications, wording, typo fixes, non-semantic refinements

**Version**: 1.15.2 | **Ratified**: 2026-06-24 | **Last Amended**: 2026-06-24
