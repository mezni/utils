<!--
SYNC IMPACT REPORT
==================
Version change: 1.0.0 → 2.0.0
Bump rationale: MAJOR — backward-incompatible redefinition of architecture
and principles. The platform has fundamentally shifted from a multi-service
deployment (4 Rust microservices, Keycloak, RabbitMQ, MongoDB) to a
modular monorepo with a single Rust backend binary. Principles I–VII have
been removed and replaced with new principles I–V reflecting the revised
architecture. ID prefix system changed (CMP-/STA-/CHR- → USR-/PRT-/STN-
/CHG-/CNT-/REV-). Governance model simplified.

Modified principles (removed → replaced):
  - I. Clean Modular Architecture → I. Modular Monorepo Architecture
  - II. Domain Model Integrity → II. Semantic Identity & Data Isolation
  - III. Event Integrity via Outbox (NON-NEGOTIABLE) → REMOVED
  - IV. Soft-Delete Discipline → Merged into Principle II
  - V. Security & Identity → Merged into Principle II
  - VI. Observability → REMOVED (deferred to future phase)
  - VII. Quality & Testing Discipline → REMOVED (deferred to future phase)

Added principles:
  - III. Administrative UX Discipline
  - IV. Mobile & Discovery Constraints
  - V. Deterministic Implementation

Added sections:
  - Platform Constraints & Technology Stack (replaces multi-service stack)
  - Governance (amendment, versioning, compliance)

Removed sections:
  - ADR Registry (pre-existing ADR-001..005) — ADRs from the prior
    architecture are no longer binding. New ADRs must be filed under the
    revised constitution if architectural boundaries are affected.
  - Non-Goals (narrowed scope under new architecture)

Templates requiring updates:
  - ✅ .specify/templates/plan-template.md — Constitution Check section
    resolves dynamically against this file; no structural edit required.
  - ✅ .specify/templates/spec-template.md — No mandated section change.
  - ✅ .specify/templates/tasks-template.md — No structural change required.
  - ⚠ docs/constitution.md — Standalone public constitution doc exists;
    should be updated to align with this ratified version.
  - ⚠ .specify/templates/commands/ — Directory does not exist; no
    references to update.

Follow-up TODOs:
  - Update docs/constitution.md to reflect v2.0.0 principles.
  - ADR-001..005 from v1.0.0 are void under this constitution; file new
    ADRs as architectural decisions arise.
  - Observability and testing discipline principles were deferred; consider
    re-introducing them as MINOR amendments when Phase 2 matures.
-->

# BorneMap Ecosystem Constitution

## 1. Project Overview

BorneMap is a high-performance, multi-tenant geospatial ecosystem designed
for the Tunisian market. The platform focuses on rapid EV charging station
discovery and management, utilizing a modular monorepo pattern to ensure
development speed, system integrity, and strict data isolation.

## Core Principles

### I. Modular Monorepo Architecture

All services exist within the `sources/` directory as a single workspace.
The backend is a Rust compiled binary using Actix-web for async HTTP,
SQLx for compile-time verified SQL, and Tokio as the multi-threaded
runtime. The database is PostgreSQL 16+ with PostGIS; all coordinates
use `GEOGRAPHY(Point, 4326)` in longitude-first notation. All API
endpoints are mounted strictly behind `/api/v1/*`. Spatial queries
(`ST_DWithin`) MUST resolve in ≤200ms under concurrent production
workloads. Domain layers under `/backend/src/domain/` MUST be structured
for clean extraction into standalone microservices when scale demands it.

**Rationale**: A monorepo maximizes development velocity for a small team
while preserving modular boundaries that allow surgical service extraction
without rewrites.

### II. Semantic Identity & Data Isolation

No UUIDs. All primary and foreign keys MUST use the format
`[PREFIX]-[12-char-lowercase-alphanumeric-nanoid]`. The prefix registry
is fixed: `USR-` (users), `PRT-` (partner operators), `STN-` (stations),
`CHG-` (chargers), `CNT-` (connector types), `REV-` (reviews).

Multi-tenancy is enforced at the database extraction tier. Partner
Dashboard API requests MUST inject the verified `owner_id` context
(mapped to a `USR-` token) into all queries automatically. No
partner-scoped endpoint MAY omit the `owner_id` constraint.

Sandbox isolation: records marked `is_test = true` are strictly excluded
from production mobile discovery and analytics reporting via
repository-level filtering (`AND ($4 = TRUE OR s.is_test = FALSE)`).
The `include_test` parameter defaults to `false` on all public-facing
endpoints.

Soft delete applies to `users`, `partner_profiles`, and `stations`.
These tables MUST carry a `deleted_at TIMESTAMPTZ` column and every read
query MUST include `WHERE deleted_at IS NULL` unless the caller is an
explicit admin/audit path.

**Rationale**: Human-readable prefixed identifiers make ownership and
audit trails unambiguous. Repository-level multi-tenancy and test-flag
filtering prevent data leaks at the SQL boundary where they are
enforceable and testable.

### III. Administrative UX Discipline

The Admin Portal is partitioned into dedicated routing zones: Overview,
Users, Data, Analytics, Security, and Settings. Design tokens are driven
by a centralized `tailwind.config.ts`; hardcoded hex codes are banned
in view files.

Defensive UI rules are non-negotiable:
- `<ScrollableTable />` is required for any data matrix containing
  relational keys, enforcing a minimum content width of 800px to
  prevent horizontal layout breakage.
- Destructive actions require a confirmation modal where the operator
  must manually type the full resource ID (e.g., `STN-4f7d2a8b9c02`)
  before the execution button unlocks. Simple click-to-delete is
  forbidden.

When the Sandbox Workspace Selector is active, a persistent
`border-t-4 border-sky-500` visual indicator MUST illuminate to
separate test views from production data.

**Rationale**: Administrative errors on production data are
irreversible without strong interlocks. Centralized design tokens
prevent visual drift across the three client applications.

### IV. Mobile & Discovery Constraints

The mobile driver app MUST use a managed Expo Go workflow. Ejection
(`expo eject` / `expo prebuild`) is prohibited. Dependencies MUST be
locked to exact versions to prevent native runtime drift.

Discovery invariants:
- Nearby search default radius: 20km (`radius=20000.0`).
- Pagination hard-cap: 50 records per request (`LIMIT 50`).
- Test records (`is_test = true`) are completely hidden from
  production mobile instances.

The map canvas takes the entire viewport. Details grids, action sheets,
and filters MUST layer as top-level overlays; no side-panel layout that
clips the map viewport is permitted.

**Rationale**: Managed Expo eliminates native build complexity. Strict
pagination and radius caps keep map rendering performant on mobile
devices. Test-record isolation protects driver-facing data integrity.

### V. Deterministic Implementation

All domain layers (`/backend/src/domain/`) MUST be built to be easily
split into standalone microservices. Cross-domain dependencies are
expressed through clean interfaces, not shared mutable state.

Sandbox environments use a shared seed script
(`20260525000001_seed_sandbox.up.sql`) that populates 5 partner
profiles, 100 test stations, and 300 chargers with compliant semantic
identifiers. All seed records carry `is_test = true`. The seed data
is deterministic and repeatable across environments.

Admin web sessions MUST display a `border-t-4 border-sky-500` visual
indicator when testing sandbox data.

**Rationale**: Deterministic seeding enables reliable end-to-end
testing and developer onboarding. Modular domain boundaries future-proof
the architecture for service extraction without requiring it prematurely.

## Platform Constraints & Technology Stack

**Approved technology stack** — substitutions require an ADR:

- Backend: Rust + Actix-web, SQLx, Tokio runtime
- Database: PostgreSQL 16+ with PostGIS
- Frontend (Web): React + Vite, Tailwind CSS, Leaflet
- Mobile: Expo Go (managed workflow), React Native
- Deployment: `docker-compose.dev.yml` with `postgis/postgis:16-3.4-alpine`
  and a Rust backend container

**Non-Goals** (out of scope unless this constitution is amended):

- OCPP / charging-session control
- Billing or payment processing
- Energy management or smart-charging optimization
- Direct hardware/charger communication
- Real-time charger availability polling (deferred post-MVP)

**Repository structure** — top-level layout is fixed:

```
bornemap-monorepo/
├── Cargo.toml
├── docker-compose.dev.yml
└── sources/
    ├── backend/
    └── frontend/
        ├── packages/ui/
        └── apps/
            ├── admin-portal/
            ├── partner-dashboard/
            └── mobile-driver/
```

## Governance

**Amendment policy.** This constitution supersedes all prior practices.
Amendments MUST:

1. Land as a PR editing this file (and any dependent templates) together
   with an ADR that justifies the change.
2. Bump `Version` using semantic versioning:
   - **MAJOR**: principle removed, redefined incompatibly, or governance
     model changed.
   - **MINOR**: principle or section added, or guidance materially
     expanded.
   - **PATCH**: clarifications, wording, or non-semantic fixes.
3. Update `Last Amended` to the merge date (ISO `YYYY-MM-DD`).
4. Propagate to `.specify/templates/plan-template.md`,
   `spec-template.md`, and `tasks-template.md` where their gates or
   sections depend on the change.

**Code review.** Every PR MUST verify compliance with Principles I–V.
Reviewers MUST cite the principle number when blocking a change.

**Compliance review.** A quarterly review MUST confirm that running
services, schemas, and CI gates still match this document; deltas become
ADRs or amendments.

**ADR governance.** Any architectural change that affects a
constitutional boundary (repository structure, identity scheme,
multi-tenancy enforcement, UI guardrails, mobile constraints, approved
stack) MUST be documented as an ADR under `docs/adr/` before
implementation begins. PRs that violate this rule MUST be blocked at
review.

**Prior ADRs voided.** ADR-001 through ADR-005 from the v1.0.0
constitution (multi-service architecture) are no longer binding. New
ADRs must be filed under this constitution when architectural
boundaries are affected.

**Version**: 2.0.0 | **Ratified**: 2026-05-25 | **Last Amended**: 2026-05-25
