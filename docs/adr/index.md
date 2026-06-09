# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the BorneMap platform.

## Index

| ID | Title | Status |
|---|---|---|
| [ADR-001](adr-001-postgresql-single-database.md) | PostgreSQL as single database | Accepted |
| [ADR-002](adr-002-schema-separation.md) | Schema separation | Accepted |
| [ADR-003](adr-003-prefixed-nanoids.md) | Prefixed NanoIDs over UUIDs (from MVP-2) | Accepted |
| [ADR-004](adr-004-direct-analytics-insert.md) | Direct analytics insert over message broker | Accepted |
| [ADR-005](adr-005-rust-actix-web.md) | Rust + Actix-web for backend (from MVP-2) | Accepted |
| [ADR-006](adr-006-bare-metal-docker-compose.md) | Bare metal + Docker Compose over Kubernetes | Accepted |
| [ADR-007](adr-007-keycloak-auth.md) | Keycloak for authentication (from MVP-3) | Accepted |
| [ADR-008](adr-008-postgresql-trigger-gis-sync.md) | PostgreSQL trigger for GIS sync (from MVP-4) | Accepted |
| [ADR-009](adr-009-monorepo-source-root.md) | Monorepo with source/ root | Accepted |
| [ADR-010](adr-010-traefik-edge-router.md) | Traefik as edge router (from MVP-6) | Accepted |
| [ADR-011](adr-011-react-vite-web.md) | React + Vite for web applications | Accepted |
| [ADR-012](adr-012-react-native-expo-54.md) | React Native + Expo SDK 54 | Accepted |
| [ADR-013](adr-013-single-dashboard-app.md) | Single Dashboard App for partner and admin | Accepted |
| [ADR-014](adr-014-leaflet-openstreetmap.md) | Leaflet + OpenStreetMap | Accepted |
| [ADR-015](adr-015-local-image-builds.md) | Local image builds — no registry | Accepted |
| [ADR-016](adr-016-json-server-mvp-1.md) | json-server for MVP-1 mock API | Accepted |
| [ADR-017](adr-017-multiple-mvp-cycle.md) | Multiple MVP cycle delivery strategy | Accepted |
| [ADR-018](adr-018-dashboard-before-driver.md) | Dashboard built before driver apps in every MVP | Accepted |
| [ADR-019](adr-019-partner-type-field.md) | Partner type field (business / personal) | Accepted |
| [ADR-020](adr-020-partner-operational-flags.md) | Partner operational flags (is_verified, is_live, is_active) | Accepted |
| [ADR-021](adr-021-audit-trail-inventory.md) | Audit trail on all inventory tables | Accepted |

## Template

```markdown
# ADR-NNN: Title

**Status:** Proposed | Accepted | Deprecated | Superseded
**Date:** YYYY-MM-DD

## Context

What is the issue that we're seeing that is motivating this decision or change?

## Decision

What is the change that we're proposing and/or doing?

## Consequences

Why is this a good or bad decision? What becomes easier or harder?
```
