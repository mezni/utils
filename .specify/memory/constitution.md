<!--
Sync Impact Report
Version change: 0.0.0 (template) → 1.0.0
Modified principles: N/A (all new — first population)
Added sections:
  - Principle 1: Simplicity First
  - Principle 2: Single Source of Truth
  - Principle 3: Clear Separation of Concerns
  - Principle 4: Manual Operations Allowed
  - Principle 5: Identity & Security (NON-NEGOTIABLE)
  - User Model & Access Rules
  - Non-Negotiable Rules & Engineering Discipline
  - Governance section
Removed sections: N/A
Templates requiring updates:
  - .specify/templates/plan-template.md — ✅ updated (no changes needed, generic gates)
  - .specify/templates/spec-template.md — ✅ updated (no changes needed)
  - .specify/templates/tasks-template.md — ✅ updated (no changes needed)
  - .specify/templates/checklist-template.md — ✅ updated (no changes needed)
Follow-up TODOs: None
-->

# Bornemap Constitution

## Core Principles

### 1. Simplicity First

Keep architecture minimal and easy to operate. Start simple, avoid
over-engineering. YAGNI principles apply. Prefer straightforward solutions
over complex frameworks or abstractions.

### 2. Single Source of Truth

Each domain has one authoritative system. Identity is managed exclusively by
Keycloak (login, token issuance, session management). Business data lives in
the platform database (`platform_db`). Analytics lives in the analytics
database (`analytics_db`). No direct DB access from frontend. Secrets are
stored only on host environment files.

### 3. Clear Separation of Concerns

Separate authentication, business data, and analytics into distinct domains
with dedicated services. Each backend service MUST follow Clean Architecture:
domain layer (pure logic), application layer (use cases), infrastructure layer
(DB, external systems), interface layer (HTTP, workers). Single monorepo with
Rust backend workspace and shared domain packages (auth, config, errors, IDs,
observability).

### 4. Manual Operations Allowed

The system prioritizes manual deployment simplicity over full automation.
Deployment is fully manual — no automated production deploy from CI. GitHub
Actions is used only for build, test, lint, and Docker image build. Runtime
uses bare metal with Docker Compose and Traefik as the public entrypoint.

### 5. Identity & Security (NON-NEGOTIABLE)

Keycloak is the only identity provider. Keycloak handles login, token
issuance, and session management — it does NOT handle business data
(favorites, reviews, stations, partner data). Only Traefik exposes public
ports. All APIs MUST validate JWT (except public endpoints). Partner access
MUST always be scoped to their own organization. Partner users are strictly
scoped to one organization.

## User Model & Access Rules

### User Types

- **Public Driver** — anonymous user; can browse stations, search, filter,
  view station details, view reviews and ratings. Cannot authenticate actions,
  create favorites, or create reviews.
- **Registered Driver** — authenticated user; all public capabilities plus
  manage favorites, manage own reviews, manage profile.
- **Partner** — authenticated business user; manage own stations only, manage
  own chargers only, update availability. Partners are strictly scoped to
  their own organization.
- **Admin** — global system administrator; full platform access, manage users
  and partners, manage stations and chargers, moderate content.

### Roles (Strict Set)

`registered_driver`, `partner`, `admin` — no additional roles allowed.

Partner users are strictly scoped to one organization. Partner scope is
mandatory and enforced at every API layer.

## Non-Negotiable Rules & Engineering Discipline

1. Stations MUST live in `inventory.station`.
2. GIS is NEVER the source of truth — it is a derived projection layer only.
   GIS schema contains derived spatial data, updated asynchronously via GIS
   Sync Worker. GIS failures MUST NOT block station updates.
3. Analytics lives only in `analytics_db`. Events are immutable and never
   affect system state.
4. Keycloak is the ONLY identity provider.
5. Partner scope is MANDATORY and enforced at every layer.
6. Routing and navigation are OUT OF SCOPE for MVP.
7. Public access MUST always be available without login.
8. Station lifecycle is controlled by the business layer; soft delete is
   required (no hard deletes in MVP). Inactive stations MUST NOT appear in
   public discovery.
9. GIS updates are asynchronous and event-driven; failures do not block
   station updates. GIS Sync Worker polls an outbox table for changes.

### Engineering Conventions

- Single monorepo (`ev-platform/`) with Rust backend workspace.
- Shared domain packages for auth, config, errors, IDs, observability.
- Each backend service MUST follow Clean Architecture (domain → application →
  infrastructure → interface layers).
- Frontend apps share a `packages/ui` component library with design tokens.
- Frontend apps: Driver Web App (React + Vite), Driver Mobile App (React
  Native Expo), Partner Dashboard (React + Vite), Admin Dashboard (React +
  Vite).

## Governance

This constitution supersedes all other project practices and documentation.
Amendments require written documentation, team approval, and a migration plan.
All PRs and reviews MUST verify compliance with constitution principles.
Complexity introduced without constitutional justification MUST be flagged and
resolved. The constitution is versioned using semantic versioning (MAJOR for
principle redefinitions, MINOR for additions, PATCH for clarifications).

**Version**: 1.0.0 | **Ratified**: 2026-06-05 | **Last Amended**: 2026-06-05
