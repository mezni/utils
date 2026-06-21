# BorneMap Architecture

**Status**: Canonical
**Version**: 1.0

---

## System Overview

BorneMap is an EV charging station discovery and management platform for the Tunisian market. The architecture enforces strict service ownership, deterministic identity separation, and a single-writer analytics pipeline.

---

## Architecture Diagram

```
                Keycloak (Identity Authority)
                          |
                    Traefik Gateway
                (auth + rate limiting + routing)
                          |
        +-----------------+------------------+
        |                 |                  |
 auth-service     driver-service      admin-service
 (identity proj)   (GIS + telemetry)   (inventory)
        |                 |                  |
 platform_db        PostGIS + Redis     inventory schema
        |                 |
     users          analytics_db (WRITE ONLY via ingestion API)
```

---

## Service Topology

| Service        | Port | Language | Responsibility                         |
|----------------|------|----------|----------------------------------------|
| auth-service   | 3000 | Rust     | JWT validation, user projection, sync  |
| driver-service | 3001 | Rust     | GIS engine, telemetry, analytics write |
| admin-service  | 3002 | Rust     | Inventory CRUD, analytics read         |

---

## Database Topology

| Database      | Purpose                       | Owner           |
|---------------|-------------------------------|-----------------|
| platform_db   | Application data (3 schemas)  | Mixed (see below)|
| analytics_db  | Telemetry & analytics events  | driver-service  |
| keycloak_db   | Identity provider storage     | Keycloak only   |

### platform_db Schemas

| Schema    | Tables                           | Owner          |
|-----------|----------------------------------|----------------|
| users     | users, user_profiles             | auth-service   |
| gis       | osm_charging_stations_temp, curated | driver-service |
| inventory | partners, stations, chargers     | admin-service  |

---

## Identity Architecture

**Dual identity model**:
- **Human**: Keycloak UUID (`sub`) — used in auth-service, user_profiles
- **Business**: `PREFIX-nanoid(12)` — STA, CHG, OPR, EVT

Never mix identity types.

---

## Frontend Architecture

```
apps/packages/
  ui-kit/        -- UI only (components, tokens, layouts)
  domain-types/  -- Contracts only (DTOs, schemas, types)
  client-core/   -- Transport only (API clients, auth, mappers)
```

Dependency chain: `ui-kit → domain-types → client-core`

---

## Backend Architecture

```
backend/
  auth-service/     -- Port 3000, users schema owner
  driver-service/   -- Port 3001, gis + analytics owner
  admin-service/    -- Port 3002, inventory owner
  shared/
    shared-domain/  -- Pure types only (no infra)
    shared-infra/   -- Infra only (JWT, DB, logging)
```

Dependency chain: `services → shared-domain → shared-infra`

---

## Event Pipeline

```
Frontend → Traefik → driver-service (/telemetry/events)
           → validation → enrichment → deduplication
           → analytics_db (write)
```

**Only driver-service writes analytics_db.**

---

## CI Pipeline (Strict Order)

1. `format_check`
2. `type_check`
3. `dependency_graph_validation`
4. `identity_validation`
5. `schema_validation`
6. `sqlx_compile_check`
7. `analytics_write_gate`
8. `integration_tests`
9. `build_success`

Any failure = HARD STOP.

---

## Key Architecture Rules

1. **3 services exactly** — no more, no fewer
2. **Single writer per schema** — strict ownership
3. **UUID for users, nanoid for entities** — never mixed
4. **SQLx compile-time** — no runtime SQL
5. **No circular dependencies** — enforced in CI
6. **Contract-first** — domain-types before implementation
7. **Analytics single-writer** — driver-service only

---

## Identity Prefixes

| Prefix | Entity   |
|--------|----------|
| STA    | Station  |
| CHG    | Charger  |
| OPR    | Operator |
| EVT    | Event    |

---

## Technology Stack

| Layer        | Technology                          |
|--------------|-------------------------------------|
| Services     | Rust (Actix-web / Axum)             |
| Database     | PostgreSQL + PostGIS                |
| Cache        | Redis                               |
| Identity     | Keycloak (OIDC)                     |
| Gateway      | Traefik                             |
| Mobile       | Expo SDK 54                         |
| Web          | React + Leaflet                     |
| Admin UI     | React + shadcn/ui                   |
| CI           | GitHub Actions + custom ci_guard.sh |

---

**Version**: 1.0 | **Status**: Canonical | **Last Updated**: Sprint 0
