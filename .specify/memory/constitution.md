<!--
  Sync Impact Report
  ==================
  Version change: (template) → 1.0.0
  Modified principles: N/A (first fill)
  Added sections:
    - Principle I: Data-First, Contract-Driven
    - Principle II: Strict Service Boundaries
    - Principle III: Authorization & Tenant Isolation (NON-NEGOTIABLE)
    - Principle IV: REST-Only, Contract-Driven APIs
    - Principle V: Event-Driven, Eventually Consistent
    - Section: Technology & Infrastructure Constraints
    - Section: Development & Operational Workflow
    - Governance rules
  Removed sections: N/A
  Templates requiring updates:
    - .specify/templates/plan-template.md — ✅ No changes needed (generic Constitution Check section)
    - .specify/templates/spec-template.md — ✅ No changes needed
    - .specify/templates/tasks-template.md — ✅ No changes needed
    - .specify/templates/commands/*.md — No command files exist
  Follow-up TODOs: None
-->

# Bornemap Constitution

## Core Principles

### I. Data-First, Contract-Driven

Business data is the authoritative source of truth — not UI state, not events, and
not derived projections. Every domain has a designated source of truth:
Identity (Keycloak), Users (platform_db), Stations (inventory schema),
GIS (derived layer), Analytics (analytics_db), Events (Clickstream).
Cross-domain leakage is forbidden: analytics MUST NOT affect business logic,
GIS MUST NOT be authoritative, identity MUST NOT be duplicated in the DB,
and authorization MUST NEVER be inferred from the frontend.

### II. Strict Service Boundaries

The system follows a pragmatic monolith-of-services model — a small set of
independent services that are tightly governed. Each domain occupies its own
bounded context: Identity (Keycloak), Business Data (platform_db),
Spatial Data (GIS layer), Analytics (analytics_db), Events (RabbitMQ).
No overlap between domains is permitted. Services are independent but governed
by shared contracts and explicit interface boundaries.

### III. Authorization & Tenant Isolation (NON-NEGOTIABLE)

Authorization MUST be enforced at the backend service layer AND at the
repository/data-access layer. Frontend enforcement is for UX only and is
never considered secure. Partner isolation is critical: partners MUST only
access their own stations, chargers, availability, and reports. Tenant IDs
MUST NEVER be accepted from client input — partner_id is always derived from
`users.partner_membership`. Only three roles exist: `registered_driver`,
`partner`, `admin`. No additional roles are allowed.

### IV. REST-Only, Contract-Driven APIs

All service-to-service and service-to-client communication uses pure REST APIs.
No GraphQL, no RPC-over-HTTP, no mixed paradigms are permitted. All responses
follow a standard envelope: `{ "success": true, "data": {}, "meta": {} }` for
success and `{ "success": false, "error": { "code": "...", "message": "..." } }`
for errors. URL-based versioning only (`/api/v1/`). Breaking changes require a
new version path. All endpoints MUST validate request schemas, response schemas,
auth requirements, and pagination structure.

### V. Event-Driven, Eventually Consistent

RabbitMQ is the event backbone for behavioral analytics (clickstream) and
GIS synchronization (outbox pattern). Events represent user behavior and
system-visible state transitions only — no UI implementation details.
Delivery is at-least-once; consumers MUST deduplicate via `event_id`.
Events are immutable once emitted — never updated, never deleted.
GIS is derived data, never authoritative, updated asynchronously via the
outbox → queue → worker pipeline with idempotent, replay-safe processing.

## Technology & Infrastructure Constraints

**Backend**: All services MUST be implemented in Rust (Driver Service,
Admin Service, Clickstream Service, GIS Worker, Analytics Writer).

**Frontend**: Web apps use React + Vite; mobile uses React Native Expo.
No Next.js. All apps share design tokens, API client, and auth client.

**Identity**: Keycloak is the single identity provider — OAuth2 / OpenID Connect,
JWT-based validation only. No custom auth in the platform DB.

**Database**: Three PostgreSQL databases — `keycloak_db` (identity only),
`platform_db` (business + GIS with PostGIS enabled), `analytics_db`
(event analytics with monthly partitioning).

**Mapping**: Leaflet is the core mapping engine for all web apps.

**Messaging**: RabbitMQ as the event backbone with topic exchanges.

**Deployment**: Docker Compose on bare metal / VM. Traefik as ingress — only
Traefik is exposed to the internet. No Kubernetes. No service mesh.

**Configuration**: Environment variables only. No dynamic config service,
no remote feature flags in MVP. Secrets are NEVER committed to Git.
All services MUST crash on missing required env vars.

**API**: REST-only with JSON envelopes. URL versioning (`/api/v1/`).
ID strategy: ULID with prefixes (`USR-`, `PRT-`, `STN-`, `CHG-`, `REV-`, `EVT-`).

## Development & Operational Workflow

**Architecture-first**: Design boundaries MUST be frozen before implementation
begins. The constitution, architecture, domain model, and event taxonomy are
locked during Phase 0 and serve as non-negotiable contracts.

**Soft delete**: Stations, partners, and reviews MUST use soft delete only
(`deleted_at IS NULL`). No hard deletes in production.

**Migrations**: Run BEFORE service startup. NEVER auto-executed at runtime.

**Startup order**: PostgreSQL → RabbitMQ → Keycloak → Traefik → Backend
services → Workers → Frontend apps.

**Deployment**: Artifact-based (prebuilt Docker images with SHA digests).
Release manifest MUST be verified before deployment — partial mismatch = failure.

**Pre-deployment smoke tests**: auth login, station fetch, GIS sync check,
event ingestion check.

**Rollback**: MUST be preplanned with image versioning. Three levels:
service rollback, full stack rollback, DB restore from backup.

**Backups**: Required before every production release (platform_db, analytics_db).

**Testing**: All PRs require authorization correctness tests, API contract tests,
GIS consistency validation, and event integrity checks.

## Governance

This constitution is the highest authority for all system design, architecture,
and implementation decisions. It supersedes all other documentation, ADRs, and
practices.

- **Compliance**: All PRs, reviews, and architectural decisions MUST verify
  compliance with the rules defined in this constitution.
- **Amendments**: Changes require documented rationale, stakeholder approval,
  a migration plan, and a version bump following semantic versioning:
  - MAJOR: Backward-incompatible governance changes, principle removals,
    or redefinitions.
  - MINOR: New principles or materially expanded guidance.
  - PATCH: Clarifications, wording refinements, typo fixes.
- **Review**: Compliance MUST be reviewed quarterly. Complexity MUST be
  justified — simplicity (YAGNI) is the default position. Any violation
  must be documented with explicit rationale and an approved exception.
- **Guidance**: For runtime development guidance, refer to architecture
  documents in `docs/` and the implementation plan in `.specify/templates/`.

**Version**: 1.0.0 | **Ratified**: 2026-06-01 | **Last Amended**: 2026-06-01
