<!--
  Sync Impact Report — v0.0.0 → v1.0.0

  Version change: 0.0.0 (unfilled template) → 1.0.0 (initial population)

  Modified principles:
    - [PROJECT_NAME] → "BorneMap"
    - [PRINCIPLE_1_NAME] → "I. Pragmatic Architecture"
    - [PRINCIPLE_2_NAME] → "II. Clear Ownership Boundaries"
    - [PRINCIPLE_3_NAME] → "III. Operational Simplicity"
    - [PRINCIPLE_4_NAME] → "IV. Evolution over Complexity"
    - [PRINCIPLE_5_NAME] → "V. Data Separation in PostgreSQL"

  Added sections:
    - Full Core Principles (5 principles populated)
    - Section 2: System Architecture
    - Section 3: Engineering & Quality Standards
    - Governance rules

  Removed sections: N/A (first fill)

  Templates requiring updates:
    - docs/constitution.md   ✅ already exists (v5.1, source of truth)
    - docs/plan.md           ⚠ pending — if plan template is used, must reference constitution sections
    - docs/tasks.md          ✅ already reflects constitution architecture
    - docs/epic00.md         ✅ aligns fully
    - .specify/templates/plan-template.md    ✅ no changes needed (generic)
    - .specify/templates/spec-template.md     ✅ no changes needed (generic)
    - .specify/templates/tasks-template.md    ✅ no changes needed (generic)

  Follow-up TODOs: none — all placeholders resolved from existing project docs.
-->

# BorneMap Constitution

## Core Principles

### I. Pragmatic Architecture

Minimize deployable services. Avoid premature microservices
fragmentation. The system MUST ship with the fewest runtime services
that can express the domain boundaries cleanly.

### II. Clear Ownership Boundaries

Every component MUST have a single source of truth and explicit
responsibility. No shared mutable state across service boundaries.
Cross-service DB access is FORBIDDEN.

### III. Operational Simplicity

Bare metal deployment. Docker Compose. Traefik as reverse proxy.
Manual deployment from GitHub Container Registry (GHCR).
NO staging environment — only local (Docker Compose) and production.

### IV. Evolution over Complexity

Optimize for fast iteration. Introduce complexity (new services,
new schemas, new infrastructure) ONLY when justified by real load
or domain needs. Complexity MUST be documented and approved.

### V. Data Separation in PostgreSQL

Even within one database instance, data MUST live in four isolated
schemas: `inventory` (transactional), `users` (identity-linked),
`gis` (geospatial enrichment), `analytics` (event + aggregation).
No new schemas without explicit architectural approval.

## System Architecture

### Services

The system consists of exactly five runtime services plus Keycloak:

- **Keycloak** — Identity provider (authentication, tokens, sessions,
  OAuth, role assignment). Does NOT own profiles, favorites, reviews,
  or partner business logic.
- **Admin Service** — System of record for inventory. Sole writer of
  the `inventory` schema. Handles partner/station/charger CRUD,
  moderation, reporting, GIS sync triggers.
- **Driver Service** — Discovery + user actions. Public station
  discovery, nearby search, favorites, reviews, user profile.
  MUST separate public vs authenticated endpoints explicitly.
- **Clickstream Service** — Event ingestion only. Validates and
  publishes to RabbitMQ. MUST NOT handle business data.
- **GIS Sync Worker** — Reads `inventory.station`, computes GIS
  enrichments, updates `gis` schema artifacts. NEVER source of truth.
- **Traefik** — Single public entrypoint. Only Traefik exposes public
  ports; all other services remain internal.

### Communication Model

ALLOWED:
- REST (frontend ↔ services)
- RabbitMQ (async events)
- DB access only within owning service

FORBIDDEN:
- Cross-service DB access
- `inventory` writes outside Admin Service
- `gis` writes outside GIS Sync Worker

### Identity & RBAC

Exactly three roles: `registered_driver`, `partner`, `admin`.
No additional roles without explicit architectural approval.

Enforcement layers: (1) Keycloak — authentication + claims,
(2) Service layer — authorization, (3) DB constraints — final
enforcement.

Partner isolation is CRITICAL: all partner queries MUST enforce
`partner_id` filter at repository level. No exception at API layer.
Violation = architectural defect.

### Identifier Standard

All entities use NanoID with prefixes: `USR-`, `PRT-`, `STN-`,
`CHG-`, `REV-`. Must appear consistently in DB, APIs, logs, events,
and UI.

### Architectural Invariant

`inventory.station` is the single source of truth for physical
infrastructure. All other systems (GIS, analytics) are derived
projections. GIS NEVER writes back to inventory.

## Engineering & Quality Standards

### Clean Architecture (Rust)

Four layers: `domain` (MUST NOT depend on frameworks),
`application`, `infrastructure` (adapts external systems),
`interfaces` (HTTP/worker entrypoints).

### API Standards

REST only. JSON only. Versioned endpoints `/v1/`. Cursor-based
pagination only. Standard error format with `error_code`, `message`,
and `trace_id`.

### Testing

Mandatory: unit tests, integration tests, auth tests, DB query tests,
smoke tests.

### Security

- Traefik is the only public entrypoint
- JWT required for protected routes
- Partner isolation mandatory
- No secrets in repo
- Public endpoints explicitly declared

### CI/CD

CI is mandatory. No auto-deployment — only artifact generation.
Pipeline: lint → test → build → contract validation → Docker build →
GHCR publish. Artifacts tagged `ghcr.io/<service>:<git-sha>`.
Deterministic builds required.

### Observability

Structured JSON logs (service_name, request_id, trace_id, user_id,
event_type). Metrics: request latency (p95/p99), error rates,
GIS sync lag, clickstream ingestion lag, DB query latency.
`trace_id` MUST propagate across services.

### Definition of Done

A feature is complete only if: tests included, auth rules validated,
migrations included, CI passes, no secrets exposed, documentation
updated.

## Governance

The constitution supersedes all other practices. Amendments require:

1. A documented proposal describing the change and its rationale.
2. Approval from the project architect or lead.
3. A migration plan if the change affects in-flight work.

Versioning follows semantic versioning:
- MAJOR: backward-incompatible governance/principle removals or
  redefinitions.
- MINOR: new principle/section added or materially expanded guidance.
- PATCH: clarifications, wording, typo fixes, non-semantic refinements.

All PRs and reviews MUST verify compliance with this constitution.
Complexity additions MUST be justified with reference to specific
sections. Use `AGENTS.md` for runtime development guidance.

**Version**: 1.0.0 | **Ratified**: 2026-05-31 | **Last Amended**: 2026-05-31
