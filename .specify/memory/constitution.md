<!--
  Sync Impact Report:
  Version change: n/a (template) → 1.0.0
  Modified principles: All 5 principles newly established from template
  Added sections: Core Principles (5), Service Isolation & Security Standards, Context Governance & Documentation, Governance
  Removed sections: None
  Templates requiring updates: ✅ none required (all templates are generic and principle-agnostic)
  Follow-up TODOs: None
-->

# BorneMap Constitution

## Core Principles

### I. Validation Before Optimization

BorneMap's strategic operational rule is **"validation before
optimization"**. System scaling, high-availability clusters, and
infrastructure optimizations MUST only be executed following explicitly
validated market demand. Premature scaling or infrastructure investment
without demonstrated user adoption is strictly prohibited.

**Rationale**: Prevents resource waste on unvalidated assumptions in a
niche market (Tunisian EV charging discovery). Forces product-market
fit validation before operational cost escalation.

### II. Technical Stack Governance

The following technology stack is locked for the MVP and initial
rollouts. Deviation requires a formal constitution amendment:

- **Backend Services**: Rust via Actix-web (modular workspace divided
  into discrete worker domains)
- **Database Layer**: PostgreSQL with PostGIS extension for spatial
  computations
- **In-Memory Cache**: Redis for session management, state distribution,
  and token lookups
- **Message Broker**: RabbitMQ for asynchronous event-driven decoupling
  and background pipeline tasks
- **Mobile Client**: React Native via the Expo Go framework
- **Admin Portal**: React (Vite / Next.js layout engine)

**Rationale**: A locked stack eliminates architecture churn during MVP
  delivery and ensures all contributors target the same runtime
  environment.

### III. Architecture & Service Taxonomy

- The main business router gateway MUST be strictly designated as
  **`api-service`**. Generic references to `api/` are deprecated to
  ensure strict architectural decoupling from `auth-service` and any
  future async consumer daemons.
- Services MUST communicate downstream via AMQP queues managed by
  RabbitMQ, or cryptographically verify identity using stateless bearer
  tokens.
- Service isolation is mandatory; no shared mutable state across service
  boundaries.

**Rationale**: Clear naming prevents ambiguous routing and reinforces
  the boundary between the public API gateway and internal service
  mesh.

### IV. Data Architecture Standards

- **Identifiers**: All database entity IDs MUST adhere to the
  `XXX-nanouuid` pattern (e.g., `stn-e3b0c442`, `prv-k9x2m47a`,
  `chg-7b2a19f4`). Standard UUIDv4 arrays or sequential auto-incrementing
  integers are strictly prohibited from direct public API exposure.
- **Spatial Consistency**: Location telemetry MUST use PostGIS geometry
  coordinates (`SRID 4326`).
- **Default Map View**: Client map views MUST natively anchor on the
  geographic center bounds of Tunis, Tunisia.

**Rationale**: Custom ID prefixes enable at-a-glance entity type
  identification. PostGIS SRID 4326 is the industry standard for WGS 84
  GPS coordinates and ensures interoperability with mapping providers.

### V. Operational Workflows & Deployment

- **Early Integration**: Automated CI/CD pipelines, container
  orchestration primitives, and operational code layouts MUST be
  integrated at the earliest stage of development.
- **Orchestration Blueprint**: Local engineering loops and staging
  deployments MUST use Docker Compose to guarantee environment parity
  across developer machines and infrastructure targets. Compose files
  reside under `/deployments`.
- **Context Governance**: Development tracking specifications, structural
  schema dictionaries, and onboarding maps MUST be kept in sync under
  `/spec` and `/docs` to facilitate clean, context-aware parsing by AI
  coding assistants.

**Rationale**: Early CI/CD prevents integration surprises. Docker
  Compose guarantees identical environments from laptop to staging.

## Service Isolation & Security Standards

- Inter-service communication MUST use either AMQP (RabbitMQ) or
  stateless bearer tokens. Direct database sharing across services is
  prohibited.
- All public-facing endpoints MUST authenticate via the `auth-service`
  before reaching `api-service` business logic.
- Internal entity IDs (XXX-nanouuid) MUST NOT be exposed in external API
  responses unless explicitly required by the client contract.
- Secrets and credentials MUST never be committed to version control.
  Environment-specific configuration is loaded via environment variables
  or a secured vault.

## Context Governance & Documentation

- Feature specifications reside under `/spec/[feature-name]/` following
  the template at `.specify/templates/spec-template.md`.
- Architectural decisions MUST be documented under `/docs/adr/` with
  clear context, decision, and consequences.
- The constitution at `.specify/memory/constitution.md` is the
  authoritative source of project principles. All plans, specs, and
  tasks MUST reference and comply with it.
- AI coding assistants MUST be provided with constitution context before
  generating plans or implementation code.

## Governance

The constitution supersedes all other project practices and conventions.
Amendments require:

1. **Documentation**: A new Architecture Decision Record (ADR) under
   `/docs/adr/` describing the change, rationale, and migration plan.
2. **Approval**: Explicit team consensus before the amendment is applied.
3. **Versioning**: The constitution version follows semantic versioning:
   - MAJOR: Backward-incompatible governance or principle removals.
   - MINOR: New principles or materially expanded guidance.
   - PATCH: Clarifications, wording fixes, non-semantic refinements.
4. **Compliance Review**: Every implementation plan MUST include a
   "Constitution Check" gate that validates compliance before Phase 0
   research begins.

**Version**: 1.0.0 | **Ratified**: 2026-05-27 | **Last Amended**: 2026-05-27
