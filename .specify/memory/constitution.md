<!--
SYNC IMPACT REPORT
==================
Version change: (template, unversioned) → 1.0.0
Rationale: Initial ratification. The previous file was an unfilled template
           (all [PLACEHOLDER] tokens). First concrete adoption ⇒ MAJOR 1.0.0.

Principles defined (7):
  I.   Data-First Source of Truth
  II.  Strict Domain & Service Separation
  III. Ownership-Enforced Authorization (Partner Isolation)
  IV.  Contract-Driven REST APIs
  V.   Event-Driven & Derived State (At-Least-Once, Idempotent)
  VI.  Soft Delete & Auditability
  VII. Verification Discipline (Testing & Observability)

Added sections:
  - Core Principles (7 principles)
  - Technology & Infrastructure Constraints
  - Development Workflow & Quality Gates
  - Governance

Removed sections: none (template placeholders replaced)

Templates requiring updates:
  ✅ .specify/templates/plan-template.md      — Constitution Check gate aligns (generic, no change required)
  ✅ .specify/templates/spec-template.md      — no constitution-specific mandatory sections added
  ✅ .specify/templates/tasks-template.md     — review confirmed compatible (testing/observability tasks supported)
  ✅ .specify/templates/checklist-template.md — generic, no change required

Follow-up TODOs:
  - RATIFICATION_DATE set to 2026-06-02 (today) as no prior adoption date exists.
    If an earlier formal adoption date is known, amend via PATCH.
-->

# Bornemap Constitution

Bornemap is a REST-based, map-centric EV charging station platform. Identity is
externalized to Keycloak, business data is transactional in PostgreSQL, spatial
state (GIS) is derived asynchronously, and behavior is captured via a clickstream
analytics pipeline. The system runs on a minimal bare-metal Docker Compose
infrastructure fronted by Traefik. This Constitution is the highest authority for
system structure, data ownership, identity, authorization, eventing, GIS behavior,
and deployment. It overrides all implementation documents.

## Core Principles

### I. Data-First Source of Truth

The transactional business data (`platform_db`) is authoritative. UI, events, GIS,
and analytics are projections of it, never sources of truth. Every domain has
exactly one declared owner: Identity → Keycloak; Users/Stations/Chargers/Reviews →
`platform_db`; GIS geometry state → GIS layer (derived); Events/Analytics →
`analytics_db`. Analytics MUST NOT influence business logic, and authorization MUST
NEVER be inferred from the frontend.

**Rationale**: A single authoritative store prevents drift, contradictory state,
and the corruption risks of multi-master data.

### II. Strict Domain & Service Separation

The system is a small set of independently governed services (driver-service,
admin-service, clickstream-service, gis-worker, analytics-writer), not microservice
fragmentation. Domains (Identity, Business Data, GIS, Analytics, Events) MUST NOT
overlap. No cross-schema writes occur without crossing a service boundary, and no
cross-service env or runtime config coupling is permitted. No service mesh, no event
sourcing core, no CQRS beyond the GIS/analytics derivation split.

**Rationale**: Clear boundaries keep a solo/small team able to reason about, deploy,
and roll back each component without distributed-systems overengineering.

### III. Ownership-Enforced Authorization (Partner Isolation)

Authorization MUST be enforced at the backend and the repository/data-access layer;
the frontend provides UX gating only and is never trusted for security. Only three
roles exist: `registered_driver`, `partner`, `admin`. Partner scope (`partner_id`)
is derived solely from `users.partner_membership` and MUST NEVER be accepted from a
client. A partner MUST NEVER read, modify, or infer another partner's data. A user
belongs to at most one partner.

**Rationale**: Tenant isolation enforced in the data layer is the only defense that
cannot be bypassed by a compromised or buggy client.

### IV. Contract-Driven REST APIs

APIs are pure REST under `/api/v1/{domain}/{resource}` with URL-based versioning
(breaking changes → `/v2`); no GraphQL, no RPC, no header versioning, no BFF
complexity. All responses use the standard envelope: success `{ "success": true,
"data": {}, "meta": {} }` and error `{ "success": false, "error": { "code": "",
"message": "" } }`. List endpoints MUST paginate. Canonical error codes and contract
shapes are shared across all four frontends.

**Rationale**: A single predictable contract serves web, mobile, and dashboards
identically, enabling shared clients, caching, and stable evolution.

### V. Event-Driven & Derived State (At-Least-Once, Idempotent)

RabbitMQ is the event backbone. Station/charger mutations propagate via the outbox
pattern → queue → worker; clickstream events flow frontend → clickstream-service →
queue → analytics-writer. GIS is derived, asynchronous, and eventually consistent —
it MUST NEVER block business operations and MUST NEVER be authoritative. All
consumers assume at-least-once delivery and MUST deduplicate (GIS by entity, events
by `event_id`); all processing MUST be idempotent and replay-safe. Analytics events
are immutable, append-only, carry no PII, and are never used for authorization or
business decisions. Required mutations (station/review/availability changes) MUST
emit events.

**Rationale**: Decoupled, replay-safe async processing keeps the core transactional
path fast while allowing spatial and analytical enrichment to lag safely.

### VI. Soft Delete & Auditability

`station`, `partner`, and `review` MUST use soft delete only (`deleted_at`); hard
deletes are forbidden in production. All mutable entities carry audit fields
(`created_at`, `updated_at`, `created_by`, `updated_by`, `deleted_at`) and use the
ULID+prefix ID strategy (USR-, PRT-, STN-, CHG-, REV-, EVT-). A station is visible
only when `is_live = true AND deleted_at IS NULL AND status = 'active' AND
is_public = true`. Admins have global scope but MUST still respect soft-delete,
audit, and integrity rules.

**Rationale**: Recoverability and audit trails are non-negotiable for a
multi-tenant operational platform; visibility rules prevent leaking unpublished
inventory.

### VII. Verification Discipline (Testing & Observability)

A feature is not complete without tests covering authorization correctness, partner
isolation, data correctness, GIS idempotency/correctness, event integrity, API
contracts, and soft-delete behavior. Releases are gated: all unit, integration, and
contract tests pass, critical E2E flows pass, security checks pass, and no
performance regression is detected. Every service MUST emit structured JSON logs
with request correlation IDs, baseline metrics, and error-tracing metadata, and MUST
expose a `/health` endpoint. No PII appears in logs.

**Rationale**: In a distributed, event-driven system, correctness and isolation
guarantees are only real if continuously verified and observable in production.

## Technology & Infrastructure Constraints

- **Backend**: Rust for all services. **Frontend**: React + Vite (web), React Native
  Expo (mobile); no Next.js. **Mapping**: Leaflet. **Repo**: single monorepo
  (`services/`, `crates/`, `apps/`, `packages/`, `infra/`, `docs/`); kebab-case
  naming per `docs/WORKSPACE_CONVENTIONS.md`.
- **Data**: three PostgreSQL databases — `keycloak_db` (identity only), `platform_db`
  (business + GIS, PostGIS enabled), `analytics_db` (partitioned events). Cross-DB
  joins are impossible by design. GIST indexes required on geometry; all foreign keys
  and common query filters (`status`, `partner_id`, `is_live`, `station_id`) indexed.
- **Configuration**: environment variables only; fail-fast on invalid/missing config;
  host-managed secrets; only `.env.example` committed; no secrets in Git; no dynamic
  remote config in MVP. Each service owns its own env with no cross-service coupling.
- **Deployment**: Docker Compose on bare metal/VM; no Kubernetes, no service mesh, no
  registry dependency (artifacts loaded via release manifest with SHA verification).
  Only Traefik (and the Keycloak auth endpoint) is publicly exposed; everything else
  is internal-only. Migrations run before service startup, never auto-executed at
  runtime.
- **Performance baseline**: single-region; designed for < 100 events/sec, moderate
  concurrency; station search ≤ 200ms p95; GIS queries MUST use spatial indexes; no
  full-table scans on list paths.
- **UX**: map-first, progressive authentication (no login wall before browsing),
  cross-app consistency, RTL (Arabic) + LTR (French) support, WCAG 2.1 AA.

## Development Workflow & Quality Gates

- **Conventional commits** (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`);
  commit per logical task group; never commit build artifacts or `node_modules`.
- **CI** runs lint, unit, integration, contract, and security tests with isolated DB
  and RabbitMQ containers and a test Keycloak realm; frontend CI adds type checks,
  build validation, and accessibility checks.
- **Pre-release gates** (all mandatory): unit + integration + contract pass; critical
  E2E flows pass; no performance regression; security checks pass.
- **Post-deploy smoke tests**: auth login, station fetch, GIS sync sanity,
  clickstream ingestion, analytics pipeline verification.
- **Sprint discipline**: dependency-gated ordering — identity before data access,
  data before GIS, GIS before driver UX, UX before analytics. No service list or
  domain boundary changes without a Constitution amendment.

## Governance

This Constitution supersedes all other practices and implementation documents
(`docs/*.md`). When a conflict arises, this document wins.

- **Amendments** require a documented change, review approval, and a migration plan
  for any affected data, contracts, or services. The Sync Impact Report at the top of
  this file MUST be updated on every amendment.
- **Versioning policy** (semantic):
  - **MAJOR**: backward-incompatible governance/principle removals or redefinitions.
  - **MINOR**: a new principle/section or materially expanded mandatory guidance.
  - **PATCH**: clarifications, wording, and non-semantic refinements.
- **Compliance review**: all PRs and reviews MUST verify adherence to these
  principles. Any deviation MUST be justified in the plan's Complexity Tracking with
  the simpler alternative explicitly rejected; unjustified violations block merge.
- **Runtime guidance**: contributors follow `docs/WORKSPACE_CONVENTIONS.md` and the
  authoritative specs in `docs/` (architecture, api, db, events, deployment, config,
  tests, uxui), all of which remain subordinate to this Constitution.

**Version**: 1.0.0 | **Ratified**: 2026-06-02 | **Last Amended**: 2026-06-02
