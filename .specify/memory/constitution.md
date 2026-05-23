<!--
SYNC IMPACT REPORT
==================
Version change: (unratified template) → 1.0.0
Bump rationale: Initial ratification. Previous file contained only unfilled
template placeholders; this is the first populated, governing version.

Modified principles:
  - [PRINCIPLE_1_NAME] → I. Clean Modular Architecture
  - [PRINCIPLE_2_NAME] → II. Domain Model Integrity
  - [PRINCIPLE_3_NAME] → III. Event Integrity via Outbox (NON-NEGOTIABLE)
  - [PRINCIPLE_4_NAME] → IV. Soft-Delete Discipline
  - [PRINCIPLE_5_NAME] → V. Security & Identity
  Added principles (beyond template's 5):
  - VI. Observability
  - VII. Quality & Testing Discipline

Added sections:
  - Platform Constraints & Technology Stack (replaces [SECTION_2_NAME])
  - Development Workflow & Governance (replaces [SECTION_3_NAME])
  - Non-Goals (explicit exclusions)
  - ADR Registry (pre-existing ADR-001..005)

Removed sections: none (template was empty placeholders only).

Templates requiring updates:
  - ✅ .specify/templates/plan-template.md — Constitution Check section now
    has concrete gates derived from this file; existing placeholder language
    ("Gates determined based on constitution file") still resolves correctly
    against this constitution. No structural edit required at ratification
    time; reviewers MUST validate each plan against principles I–VII.
  - ✅ .specify/templates/spec-template.md — No mandated section change
    introduced by this constitution beyond what the template already covers
    (User Scenarios, Requirements, Success Criteria, Assumptions).
  - ✅ .specify/templates/tasks-template.md — Task categories already cover
    setup, foundational, per-story, polish. Principle VII (testing
    discipline) and Principle VI (observability) MUST be reflected as
    concrete tasks per feature; no template structural change required.
  - ⚠ .specify/templates/commands/*.md — Directory does not exist in this
    repository; no command-template references to update.
  - ⚠ README.md / docs/quickstart.md — Not present in repo root at
    ratification time. When created, they MUST reference this constitution.

Follow-up TODOs:
  - None. All placeholder tokens resolved.
-->

# BorneMap Constitution

BorneMap is a geospatial EV charging discovery platform built for Tunisia. Its
mission is to give EV drivers a fast, accurate, and visually rich way to
discover and evaluate charging stations — while giving companies, operators,
and administrators the tools to manage infrastructure data and monitor
platform health.

## Core Principles

### I. Clean Modular Architecture

The platform MUST be built as a set of independently deployable services
following Clean Architecture boundaries. Service responsibilities are fixed:

- `auth-service`: Keycloak integration, JWT validation, OAuth handling.
- `core-service`: companies, stations, chargers, favorites, reviews,
  moderation, outbox, and audit event publishing. Sole event producer.
- `geo-service`: nearby search, bounding-box queries, routing, ETA
  calculation. All backend services implemented in Rust (ADR-002).
- `analytics-service`: event consumption, analytics aggregation, audit log
  persistence.

Inter-service communication MUST use REST for synchronous calls and RabbitMQ
for asynchronous events. PostgreSQL + PostGIS is the system of record;
MongoDB is reserved for analytics and audit logs only. Direct cross-service
database access is forbidden.

**Rationale**: Bounded contexts and a single source of truth prevent
coupling drift, enable independent scaling of all services in Rust,
and keep audit/analytics workloads off the transactional path.

### II. Domain Model Integrity

The infrastructure hierarchy is fixed and MUST be enforced at the schema and
API layers:

```
Company (CMP-<nanoid>)
└── Station (STA-<nanoid>)
    └── Charger (CHR-<nanoid>)
```

Rules:

- Companies are created by Admin only.
- A station MUST be owned by exactly one company OR by a private individual.
- A charger MUST belong to exactly one station.
- "Company" replaces the legacy "network" concept (ADR-001); the term
  `network` MUST NOT be reintroduced as a top-level grouping.
- Entity identifiers MUST use the typed-prefix + nanoid format shown above.

**Rationale**: A single, prefixed identifier scheme and a strict hierarchy
make ownership, cascading operations, and audit trails unambiguous.

### III. Event Integrity via Outbox (NON-NEGOTIABLE)

`core-service` is the sole event producer. All domain events MUST be written
to the PostgreSQL `outbox` table inside the same database transaction as the
business mutation that produced them. A relay worker publishes outbox rows
to RabbitMQ. Consumers MUST treat delivery as at-least-once and MUST
implement idempotency (e.g., dedupe key on event id).

No service MAY publish domain events to RabbitMQ outside the outbox
pipeline. Direct `channel.publish` calls from business logic are forbidden.

**Rationale**: The outbox pattern (ADR-004) is the only way to guarantee
that what consumers see matches what the database committed; bypassing it
silently corrupts analytics, audit logs, and downstream state.

### IV. Soft-Delete Discipline

Soft delete applies exclusively to infrastructure entities: `companies`,
`stations`, `chargers`. These tables MUST carry a `deleted_at TIMESTAMPTZ`
column and every read query MUST include `WHERE deleted_at IS NULL` unless
the caller is an explicit admin/audit path that opts in.

Cascading semantics:

- Deleting a company MUST soft-delete its stations and their chargers.
- Deleting a station MUST soft-delete its chargers.

Non-infrastructure entities (favorites, reviews, moderation records, outbox
rows, audit logs) MUST NOT use soft delete; they follow their own retention
or hard-delete policies (ADR-005).

**Rationale**: Reversibility and audit reconstruction on infrastructure
data; avoiding soft-delete sprawl on transient or append-only data.

### V. Security & Identity

Keycloak is the sole identity provider (ADR-003). The following are
non-negotiable:

- Keycloak MUST NOT be publicly exposed; access is via the NGINX gateway.
- JWTs MUST be validated at the gateway AND independently at each service.
- The OAuth PKCE flow MUST be used for all interactive clients.
- TLS termination MUST occur at NGINX (Let's Encrypt).
- Rate limiting MUST be applied at the gateway for all public endpoints.
- Secrets MUST be supplied via environment variables only; no secret value
  may be committed to the repository.

**Rationale**: A single, externally-managed IdP plus defense-in-depth JWT
validation is the only sustainable posture for a multi-service deployment.

### VI. Observability

Every service MUST expose:

- Structured JSON logs with a correlation ID propagated from the gateway
  through all downstream calls and event consumers.
- A `/health` endpoint (liveness + readiness signaling).
- A `/metrics` endpoint exposing Prometheus-compatible metrics.

Logs MUST NOT contain raw secrets, JWTs, or PII beyond what is strictly
required for support.

**Rationale**: Without correlation IDs and uniform health/metrics surfaces,
incident response across four services becomes guesswork.

### VII. Quality & Testing Discipline

The following test categories are mandatory and MUST exist before a feature
is considered done:

- Unit tests for domain logic.
- Integration tests for cross-component behavior (DB, queue, HTTP).
- Transaction tests proving atomicity of business-mutation + outbox writes.
- Outbox tests proving relay-worker delivery semantics.
- Audit-log tests proving the audit event was persisted with the expected
  shape.
- Soft-delete tests proving `deleted_at IS NULL` filtering and cascade.
- Spatial correctness tests for any geo-service query (nearby,
  bounding-box, routing, ETA).

Definition of Done (all MUST hold):

1. All applicable tests above pass in CI.
2. OpenAPI specs are updated for any REST surface change.
3. Security review confirms Principle V is upheld.
4. Logging/metrics/health for the changed path comply with Principle VI.
5. An ADR is filed if a constitutional boundary is affected.

**Rationale**: The hard-to-test categories (outbox, audit, soft-delete,
spatial) are exactly the ones whose silent regressions cause data loss or
fraud; making them mandatory closes that gap.

## Platform Constraints & Technology Stack

**Non-Goals (out of scope in all phases unless this constitution is amended):**

- OCPP / charging-session control.
- Billing or payment processing.
- Energy management or smart-charging optimization.
- Direct hardware/charger communication.
- Real-time charger availability polling (deferred post-MVP).

**Approved technology stack** — substitutions require an ADR:

- Backend: Rust + Actix-Web (all services), PostgreSQL + PostGIS, MongoDB,
  RabbitMQ, Keycloak.
- Frontend: React + Vite, Tailwind CSS, shadcn/ui, React Query, React
  Router, Framer Motion, Leaflet.

**Deployment** — on-premises only at ratification:

- Orchestrated by a single `docker-compose.yml`.
- NGINX as the gateway with Let's Encrypt TLS.
- Required containers: `nginx`, `keycloak`, `auth-service`, `core-service`,
  `geo-service`, `analytics-service`, `postgres`, `mongodb`, `rabbitmq`.

**Repository structure** — top-level layout is fixed:

```
bornemap/
├── docs/
├── services/
├── frontend/
├── infra/
├── docker-compose.yml
├── Makefile
└── CONTRIBUTING.md
```

## Development Workflow & Governance

**ADR Governance Rule.** Any architectural change that affects a
constitutional boundary (service responsibilities, data ownership, identity
provider, event pipeline, soft-delete scope, deployment topology, approved
stack) MUST be documented as an ADR under `docs/adr/` before implementation
begins. PRs that violate this rule MUST be blocked at review.

**ADR Registry (pre-existing, binding at ratification):**

- ADR-001: Drop `networks`; use companies as the top-level grouping.
- ADR-002: All backend services implemented in Rust.
- ADR-003: Keycloak as sole identity provider.
- ADR-004: Outbox pattern for reliable event publishing.
- ADR-005: Soft delete on infrastructure entities only.

**Code review.** Every PR MUST verify compliance with Principles I–VII and
the Definition of Done in Principle VII. Reviewers MUST cite the principle
number when blocking a change.

**Amendment & versioning policy.** This constitution supersedes ad-hoc
practices. Amendments MUST:

1. Land as a PR editing this file (and any dependent templates) together
   with the ADR that justifies the change.
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

**Compliance review.** A quarterly review MUST confirm that running
services, schemas, and CI gates still match this document; deltas become
ADRs or amendments.

**Version**: 1.0.0 | **Ratified**: 2026-05-22 | **Last Amended**: 2026-05-22
