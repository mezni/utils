# Architecture Overview

**Phase**: 1 — Foundation
**Related Tasks**: TASK-01 through TASK-58
**Related ADRs**: ADR-001 through ADR-015
**Last Updated**: 2026-06-07

---

## Architecture Scope (Phase 1)

| Component | Status | Sprint |
|---|---|---|
| Cargo workspace | 🔴 Planned | 1.1 |
| pnpm workspace | 🔴 Planned | 1.1 |
| ev-core (shared crate) | 🔴 Planned | 1.1 |
| ev-db (shared crate) | 🔴 Planned | 1.1 |
| CI/CD (6 workflows) | 🔴 Planned | 1.1 |
| Docker Compose baseline | 🔴 Planned | 1.1 |
| PostgreSQL + PostGIS | 🔴 Planned | 1.2 |
| inventory schema | 🔴 Planned | 1.2 |
| gis schema | 🔴 Planned | 1.2 |
| Driver Service | 🔴 Planned | 1.3 |
| Admin Service | 🔴 Planned | 1.4 |
| Driver Web App | 🔴 Planned | 1.5 |
| Driver Mobile App | 🔴 Planned | 1.5 |
| Dashboard App | 🔴 Planned | 1.5 |

**Out of Scope**:
- Keycloak / authentication (Phase 2)
- Clickstream Service (Phase 5)
- ev-auth shared crate (Phase 2)
- GIS sync trigger (Phase 6)
- Real-time availability

---

## System Philosophy

The platform follows a **pragmatic monolith** approach: multiple services owned by a single team, deployed on a single host, sharing a single database with schema-level separation. This avoids premature microservice complexity while maintaining clear domain boundaries.

### Key Decisions

| Decision | ADR | Rationale |
|---|---|---|
| Single database, schema separation | ADR-001, ADR-002 | One Postgres instance, domain isolation via schemas |
| NanoIDs over UUIDs | ADR-003 | Human-friendly prefixed identifiers (PRT-..., STN-...) |
| Direct analytics insert (no queue) | ADR-004 | Current scale doesn't warrant RabbitMQ |
| Rust + Actix-web | ADR-005 | Performance, type safety, single binary deployment |
| Bare metal + Docker Compose | ADR-006 | One-person operations; Kubernetes is overkill |
| Keycloak for auth | ADR-007 | Industry standard, mature, self-hosted |
| PostgreSQL trigger for GIS sync | ADR-008 | Atomic with business transaction, no worker needed |
| Monorepo (Cargo + pnpm) | ADR-009 | Single source of truth, shared dependency management |
| Traefik edge router | ADR-010 | Automatic TLS, simple routing, Docker-native |
| React + Vite for web | ADR-011 | Fast dev experience, Tailwind CSS integration |
| React Native + Expo SDK 54 | ADR-012 | Cross-platform mobile with managed workflow |
| Single Dashboard App | ADR-013 | Partner and Admin share same codebase, role-based views |
| Leaflet + OpenStreetMap | ADR-014 | Free tiles, no API key, sufficient for Tunisia |
| Local image builds | ADR-015 | No registry dependency, build on host |

---

## Communication Patterns

- **Service-to-Database**: Direct SQL via sqlx with bind parameters. No ORM.
- **Public-to-Service**: HTTP via Traefik reverse proxy. All endpoints under /api/v1 prefix.
- **Frontend-to-Backend**: REST/JSON via typed API client packages.
- **Inter-service**: None in Phase 1. Services do not call each other.
- **Asynchronous**: None in Phase 1. Future analytics events use direct HTTP POST.

---

## Deployment Model

```
Internet
    │
    ▼
  Traefik (ports 80/443)
    │
    ├── /api/v1/* → driver-service:8080  (public discovery)
    ├── /api/v1/* → admin-service:8081   (admin/partner CRUD)
    └── /api/v1/* → clickstream-service:8082 (analytics - Phase 5)
```

All Rust services expose internal ports only. Traefik is the sole public entrypoint.
