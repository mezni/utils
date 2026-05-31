<!--
  Sync Impact Report
  ==================
  Version change: N/A (initial draft) → 1.0.0
  Modified principles: N/A (all new)
  Added sections: Core Principles (5 principles), Architecture & System Model,
                  CI/CD & Deployment, Governance
  Removed sections: N/A (first constitution)
  Templates requiring updates:
    ✅ .specify/templates/plan-template.md — No changes needed (generic template)
    ✅ .specify/templates/spec-template.md — No changes needed (generic template)
    ✅ .specify/templates/tasks-template.md — No changes needed (generic template)
    ✅ .specify/templates/checklist-template.md — No changes needed (generic template)
  Follow-up TODOs:
    - TODO(RATIFICATION_DATE): Original adoption date not provided by user
-->

# BorneMap Constitution

## Core Principles

### I. Source of Truth (NON-NEGOTIABLE)

`inventory.station` MUST be the canonical source of all station data. GIS
MUST be treated as a derived projection layer — never written to directly by
business logic. Analytics data MUST remain separate from business state in
MongoDB. Only three PostgreSQL schemas are permitted: `inventory`, `users`,
and `gis`. No additional application schemas MAY exist.

### II. Minimal Service

The system MUST avoid microservice fragmentation. Only the following backend
services are permitted: Driver Service, Admin Service, Clickstream Service,
GIS Sync Worker, and Keycloak (external identity provider). A new backend
service MAY only be introduced to solve a scaling constraint or a clear
domain boundary issue that cannot be resolved within the existing services.

### III. Separation of Concerns

Domain ownership MUST follow these boundaries:

- **Business data**: PostgreSQL (`inventory` and `users` schemas)
- **Spatial enrichment**: GIS schema (derived projections only)
- **Analytics**: MongoDB
- **Identity**: Keycloak
- **Messaging (analytics)**: RabbitMQ

No component MAY cross these boundaries. Authorization MUST be enforced in
backend services, not in frontend UI code.

### IV. Event Discipline

Events MUST be limited and intentional. Only three event families exist:

1. **GIS sync events** (station.created, station.updated, station.archived) —
   sourced from the PostgreSQL outbox for GIS projection updates.
2. **Clickstream events** (page.viewed, station.opened, search.performed,
   filter.applied, favorite_station.added, review.submitted) — published
   to RabbitMQ and forwarded to MongoDB pipeline.
3. **Optional business audit events** — used only for audit and reporting
   consistency.

No event explosion architecture is permitted. The outbox pattern MUST be
the source of truth for GIS synchronization events.

### V. Operational Simplicity

Deployment MUST target a single production environment via Docker Compose.
CI/CD MUST use GitHub Actions building to GHCR with SSH-based deployment.
No Kubernetes, no service mesh, no multi-region deployment, and no staging
environment exist. Rollback MUST be deterministic via image tag reversal.

## Architecture & System Model

### Runtime Model

Docker Compose is the deployment unit. Traefik is the API gateway. Services
communicate over internal Docker networks (edge, service, data).

### PostgreSQL Data Model

- **inventory** schema owns: partner, station, charger, station_availability
- **users** schema owns: user_account, user_profile, partner_membership,
  favorite_station, station_review
- **gis** schema owns: OSM imports, roads, boundaries, derived spatial
  projections

### Event Flow

```
Admin Service writes station → outbox event in same transaction →
commit succeeds → GIS Worker processes event → GIS schema updated
```

GIS sync MUST be idempotent per `station_id` + `sync_version`.

### Access Control

| Role | Scope |
|------|-------|
| Public Driver (anonymous) | Public station discovery only |
| Registered Driver | Own favorites, reviews, profile |
| Partner | Own stations, chargers, availability |
| Admin | Global access |

## CI/CD & Deployment

### Pipeline

```
commit → CI → lint/test → build Docker images →
merge main → push to GHCR → SSH deploy → docker compose restart
```

### CI Responsibilities

- Lint
- Test
- Build Docker images

### CD Responsibilities

- Push images to GHCR
- Deploy via SSH
- Restart Docker Compose stack
- Rollback via image tag

## Governance

### Amendment Procedure

1. Proposed amendments MUST be documented with rationale and version impact.
2. Changes MUST be reviewed against all 5 Core Principles for compliance.
3. Amendments MUST specify whether they are MAJOR, MINOR, or PATCH per the
   versioning policy.
4. Ratified amendments update LAST_AMENDED_DATE.

### Versioning Policy

- **MAJOR**: Backward-incompatible governance or principle removals/redefinitions
- **MINOR**: New principle/section added or materially expanded guidance
- **PATCH**: Clarifications, wording, typo fixes, non-semantic refinements

### Architecture Stability Rules

- No schema expansion without approval (only 3 schemas permitted)
- No new backend service without justification (must solve scaling or domain
  boundary issue)
- No direct GIS writes from business logic (GIS is always derived)
- No frontend authorization assumptions (backend is authoritative)

### Compliance Review

All specifications, plans, and task lists MUST reference this constitution
for principle compliance. Complexity MUST be justified against the Minimal
Service and Operational Simplicity principles.

**Version**: 1.0.0 | **Ratified**: TODO(RATIFICATION_DATE) | **Last Amended**: 2026-05-30
