# Architecture Documentation

System design, service responsibilities, data architecture, and deployment procedures.

## Contents

- **[Overview](overview.md)** — High-level system design including system diagram, core principles, services overview, frontend applications, data architecture, request flows, technology stack, deployment topology, runtime guarantees, security posture, resilience & recovery, and observability.

- **[Services](services.md)** — Backend service responsibilities, boundaries, and ownership. Covers Keycloak (authentication), Driver Service (discovery), Admin Service (management), Clickstream Service (analytics), and Traefik (routing). (To be created)

- **[Data Architecture](data.md)** — Database design, schemas, cross-schema rules, and synchronization mechanisms. Covers four schemas (inventory, users, gis, analytics), identifier scheme (prefixed NanoIDs), GIS synchronization via trigger, backup & recovery, and performance optimization.

- **[GIS Synchronization](gis.md)** — PostgreSQL trigger-based spatial data enrichment. (To be created)

- **[Deployment](deployment.md)** — Docker Compose setup, health checks, and runtime procedures. (To be created)

## Key Design Principles

1. **Pragmatic Architecture** — Exactly five services (Keycloak, Driver, Admin, Clickstream, Traefik)
2. **Single Source of Truth** — inventory.station owns all station data
3. **Simple Operations** — One person can operate the platform
4. **Schema Separation** — Four isolated schemas with documented cross-schema rules
5. **Public Access First** — Authentication never required for browsing

## Critical Non-Negotiable Constraints

- **Only Traefik exposes public ports** (80 and 443)
- **Keycloak owns all authentication** — no service implements login
- **inventory.station is authoritative** for station data
- **gis schema is derived** — never the source of truth
- **No service writes to gis directly** — trigger function only
- **Cross-schema access strictly controlled** — see Data Architecture
- **All services use Docker** with health checks and migrations

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React + Vite, React Native + Expo |
| Backend | Rust + Actix-web |
| Database | PostgreSQL 16 + PostGIS |
| Auth | Keycloak 24 |
| Router | Traefik v3 |
| Orchestration | Docker Compose |

## Services Quick Reference

| Service | Type | Ownership | Reads | Writes |
|---------|------|-----------|-------|--------|
| Driver Service | Rust/Actix | Discovery, profiles | inventory, gis, users | users |
| Admin Service | Rust/Actix | Management, reporting | inventory, users, analytics | inventory |
| Clickstream Service | Rust/Actix | Analytics ingestion | none (validates only) | analytics |
| Keycloak | Auth Server | Authentication, JWT | none (auth only) | JWT claims |
| Traefik | Edge Router | Routing, TLS | none | routing rules |

## Request Flow Patterns

**Public Discovery:**
```
Client → Traefik → Driver Service → inventory + gis → JSON
```

**Authenticated Action:**
```
Client (with JWT) → Traefik → Service (auth middleware) → schema → JSON
```

**Partner Management:**
```
Partner (with JWT) → Traefik → Admin Service (scope middleware) → inventory → JSON
(partner_id enforced from JWT)
```

**Analytics Event:**
```
Client → Traefik → Clickstream → analytics.raw_events → Success
```

## Data Architecture at a Glance

```
inventory (Admin writes)
  ├─ partner, station, charger, availability
  └─ Source of truth

users (Driver writes)
  ├─ user_account, user_profile, favorites, reviews
  └─ User data and relationships

gis (Trigger writes)
  ├─ roads, boundaries, amenities, station_locations
  └─ Derived enrichment (synced from inventory.station)

analytics (Clickstream writes)
  ├─ raw_events, event_aggregates
  └─ Event data isolated
```

## Deployment

- **Environment:** Bare metal + Docker Compose
- **Images:** Built on host (no image registry)
- **Secrets:** Host-managed, never committed
- **Health Checks:** All services declare health endpoints
- **Migrations:** Services run migrations on startup
- **Rollback:** Manual following deployment runbook

## Questions?

1. **System design?** → Start with [Overview](overview.md)
2. **How services are organized?** → Read [Services](services.md) (when available)
3. **Database design details?** → See [Data Architecture](data.md)
4. **Why we made these choices?** → Check [ADRs](../adr/)
5. **Deploying to production?** → See [Deployment](deployment.md)

---

**Last Updated:** 2026-06-05
