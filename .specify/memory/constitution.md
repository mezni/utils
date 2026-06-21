<!--
  Sync Impact Report
  ====================
  Version change: (template) → v1.0.0
  Previous file was an unpopulated template — all sections populated from
  docs/constitution/constitution.md v1.15.2.

  Modified principles (template → populated):
    [PRINCIPLE_1_NAME]      → I. Service Topology Lock
    [PRINCIPLE_2_NAME]      → II. Identity Dual System
    [PRINCIPLE_3_NAME]      → III. Data Ownership
    [PRINCIPLE_4_NAME]      → IV. Contract-First
    [PRINCIPLE_5_NAME]      → V. SQLx Compile-Time
    [SECTION_2_NAME]        → System Architecture & Topology
    [SECTION_3_NAME]        → CI Enforcement & Pipeline
    [GOVERNANCE_RULES]      → Populated with full governance rules

  Added sections (relative to template):
    - All 5 principles populated with content from 21-section constitution
    - Section 2 (System Architecture & Topology) — combines sections 3–7, 12–13
    - Section 3 (CI Enforcement & Pipeline) — combines sections 14, 15, 18
    - Governance section — combines sections 17, 20, 21

  Removed sections: N/A (template had only placeholders)

  Templates requiring updates:
    - .specify/templates/plan-template.md        — ✅ Constitution Check gate already references constitution
    - .specify/templates/spec-template.md        — ✅ No changes needed (scope neutral)
    - .specify/templates/tasks-template.md       — ✅ No changes needed (structure neutral)

  Follow-up TODOs: None — all placeholders resolved.
-->

# BorneMap Constitution

> System of Record — EV charging station discovery and management platform for the
> Tunisian market.

## Core Principles

### I. Service Topology Lock

Exactly three services MUST exist, each on a fixed port:

- `auth-service` (3000) — Authentication + user profiles
- `driver-service` (3001) — GIS + telemetry + analytics write
- `admin-service` (3002) — Inventory + analytics read

No additional services may be introduced. No service splitting, duplication,
renaming, or merging is permitted. Any topology change requires a Constitution
upgrade. **Violation = ARCHITECTURE DRIFT.**

### II. Identity Dual System

Two independent identity systems MUST NEVER overlap:

| System       | Type              | Purpose          |
|--------------|-------------------|------------------|
| Keycloak     | UUID (sub)        | Human identity   |
| Platform     | PREFIX-nanoid(12) | Business objects |

- Users MUST use UUID only (Keycloak `sub`).
- Entities MUST use `PREFIX-nanoid(12)` only (prefixes: `STA`, `CHG`, `OPR`, `EVT`).
- Cross-format mixing is forbidden.

Identity fields in analytics MUST separate human and business identifiers:
`user_uuid` (UUID) and `operator_id` (OPR-xxx) are valid; ambiguous `actor_id`
fields are invalid.

### III. Data Ownership

Every data domain has exactly one owning service. Cross-service writes are
forbidden.

| Schema/Database    | Owner          | Permissions                         |
|--------------------|----------------|--------------------------------------|
| platform_db.users  | auth-service   | READ/WRITE (exclusive)              |
| platform_db.gis    | driver-service | READ/WRITE (exclusive)              |
| platform_db.inventory | admin-service | READ/WRITE (exclusive)            |
| analytics_db       | driver-service | READ/WRITE; admin READ ONLY         |

Frontend applications have NO direct database access. Ownership transfer
requires a Constitution upgrade.

### IV. Contract-First

All changes follow a strict order:

1. **Contract definition** — `domain-types` updated (DTOs, API contracts, event
   schemas).
2. **Backend implementation** — services implement against published contracts.
3. **Frontend implementation** — UI consumes backend APIs.

Any other sequence is an **INVALID CHANGE FLOW**. `domain-types` contains ONLY
type definitions — no runtime logic, no networking, no UI concerns.

### V. SQLx Compile-Time

ALL SQL queries MUST be compile-time verified via SQLx. CI MUST run
`cargo sqlx prepare --check`. No runtime SQL string construction, no dynamic
query generation, no ORMs. **Failure = HARD STOP.**

## System Architecture & Topology

### Service Responsibilities

| Service        | Owned APIs                                                     |
|----------------|---------------------------------------------------------------|
| auth-service   | Authentication, user profile APIs                             |
| driver-service | GIS APIs, telemetry ingestion (`POST /api/v1/telemetry/events`), nearby search |
| admin-service  | Inventory CRUD, analytics dashboards                          |

Operational endpoints (`/health`, `/ready`, `/live`, `/metrics`) are allowed on
all services.

### Database Architecture

Only three databases exist:

- **platform_db** — Application data (schemas: `users`, `gis`, `inventory`)
- **analytics_db** — Telemetry and analytics events
- **keycloak_db** — Identity provider storage (no application logic)

### Frontend Package Structure

```
apps/packages/
  ui-kit/         — UI ONLY (components, layouts, tokens, accessibility)
  domain-types/   — Contracts ONLY (DTOs, event schemas, entity IDs)
  client-core/    — Transport ONLY (API clients, auth, mappers)
```

Dependency chain: `ui-kit → domain-types → client-core`

### Backend Package Structure

```
backend/shared/
  shared-domain/  — Pure domain primitives ONLY (entity IDs, DTOs, event contracts)
  shared-infra/   — Infra only (JWT parsing, DB pools, logging)
```

Dependency chain: `services → shared-domain → shared-infra`

### Forbidden Edges

- `service → service` imports
- `frontend → backend` imports
- `shared-domain → services`
- `ui-kit → client-core`
- Circular dependencies anywhere

**Violation = HARD FAILURE.**

### Trust Boundary

No service trusts another service's runtime state. Trusted sources: Keycloak
JWT, SQLx compile-time validation, published contracts, schema validation.
Untrusted: external payloads, cached state, service assumptions, client input.

## CI Enforcement & Pipeline

### CI Pipeline DAG (Strict Order)

1. `format_check`
2. `type_check`
3. `dependency_graph_validation`
4. `identity_validation`
5. `schema_validation`
6. `sqlx_compile_check`
7. `analytics_write_gate`
8. `integration_tests`
9. `build_success`

**Any failure = HARD STOP.** No partial success allowed.

### Analytics Write Gate

ONLY `driver-service` may WRITE to `analytics_db`. Enforced via DB roles,
CI grep/static analysis, and runtime middleware. `admin-service` has READ ONLY
access. `auth-service` has NO ACCESS.

### Event System

All events flow through a single ingestion endpoint at driver-service:
`POST /api/v1/telemetry/events`. Events MUST include `schema_version`,
`idempotency_key`, and be replay-safe. driver-service is the authoritative
owner of event schemas and MUST deduplicate all events.

### Migration Isolation

| Service        | Allowed Schema           |
|----------------|--------------------------|
| auth-service   | `users`                  |
| driver-service | `gis` + `analytics_db`   |
| admin-service  | `inventory`              |

Rules: forward-only migrations, no destructive rollback, SQLx compatibility
required, CI validation required.

### HARD FAIL Conditions

- analytics_db write violation
- Identity format violation (UUID in entities, nanoid in users)
- Service topology change
- SQLx failure
- Schema mismatch / drift
- Dependency violation (cycles, cross-layer imports)
- Migration violation

## Governance

### Authority Hierarchy

1. **SDEC v3.0** — Runtime execution enforcement (highest authority)
2. **BorneMap Constitution** — Architecture definition (this document)
3. **Architecture docs** — Detailed design references
4. **Sprint artifacts** — State, review, backlog documents
5. **LLM output** — Lowest authority; must not override higher layers

### Amendment Procedure

1. Propose change with rationale and migration plan.
2. Determine version bump:
   - **MAJOR**: Backward-incompatible principle removal/redefinition.
   - **MINOR**: New principle or materially expanded guidance.
   - **PATCH**: Clarifications, wording, typo fixes.
3. Ratify with documented approval.
4. Propagate changes to dependent artifacts (plans, specs, CI scripts).
5. Update `LAST_AMENDED_DATE`.

### Compliance Review

- Every PR/review MUST verify constitution compliance.
- CI automatically enforces structural, identity, ownership, and dependency
  rules.
- Complexity additions must be justified in the plan template's Complexity
  Tracking section.
- Known inherited bugs (KNOWN-001 through KNOWN-004) must be tracked until
  resolved.

### Sprint Output Requirements

Every sprint MUST produce:
- `SYSTEM_STATE.md` — Current system inventory and status
- `roadmap_status.md` — Sprint pipeline and milestones
- `sprint_state.json` — Machine-readable sprint state
- `sprint_review.md` — Sprint review and decisions
- `validation_report.md` — Compliance audit results
- `follow_up.md` — Action items and open questions

---

**Version**: 1.0.0 | **Ratified**: 2026-06-21 | **Last Amended**: 2026-06-21
