# Architecture Overview

## System Context

BorneMap is an EV charging station discovery and management platform for the Tunisian market. Four microservices serve three client applications backed by PostgreSQL + PostGIS, Keycloak identity, and Redis caching.

---

## Client Applications

| App | Stack | Audience |
|-----|-------|----------|
| **Driver Mobile** — `apps/mobile-driver` | Expo SDK 54, React Native, `react-native-maps` | Public drivers + registered drivers |
| **Driver Web** — `apps/web-driver` | React, Leaflet | Public drivers + registered drivers |
| **Dashboard** — `apps/dashboard` | React, shadcn/ui, Tailwind CSS, React Router v6, Framer Motion, React Query | Partners + Admins |

Shared packages (`packages/`): `shared-ui`, `shared-types`, `shared-hooks`, `api-client` — reused across all client apps.

---

## Services

| Service | Port | Responsibility |
|---------|------|----------------|
| **Auth Service** | `:3000` | Identity management — owns `users` schema, integrates with Keycloak (two realms: `bornemap-drivers`, `bornemap-staff`) |
| **Driver Service** | `:3001` | Driver-facing API — station operations, favorites, nearby queries (via GIS Service) |
| **Admin Service** | `:3002` | Admin/partner management — partner CRUD, station admin, analytics writes |
| **GIS Service** | `:3003` | Read-optimized spatial API — serves `/api/v1/nearby`, owns `gis` schema reads, Redis-backed cache |

### Service Communication

```
Driver Service ──> GIS Service  (spatial reads — /api/v1/nearby, /api/v1/stations/{id})
Admin Service  ──> GIS Service  (spatial reads — /api/v1/nearby, /api/v1/stations/{id})
Auth Service   ──> Keycloak     (identity CRUD via kcadm/admin API)
```

No synchronous service-to-service calls beyond the above. No event bus, no message queue.

All spatial reads route through GIS Service. Driver and Admin services do **not** query PostGIS directly for spatial data — only GIS Service reads `inventory.station.location` for nearby/station-detail queries.

---

## Data Stores

| Database | Type | Hosted In | Purpose |
|----------|------|-----------|---------|
| `platform_db` | PostgreSQL 15 + PostGIS 3.4 | Docker | Application data: 3 schemas (`gis`, `inventory`, `users`) |
| `keycloak_db` | PostgreSQL 16 | Docker | Keycloak identity/credential store |
| `analytics_db` | PostgreSQL 16 | Docker | Usage events and analytics (MVP-5) |
| **Redis** | Redis 7 | Docker | GIS query cache (GIS Service) |

---

## Request Flow

```
                          ┌──────────┐
                          │  Client   │
                          └────┬─────┘
                               │ HTTPS
                          ┌────▼─────┐
                          │  Traefik  │  TLS termination + path-based routing
                          └──┬───┬───┘
              ┌──────────────┤   ├──────────────┐
              │              │   │              │
         ┌────▼───┐   ┌─────▼───▼────┐   ┌─────▼────┐
         │  Auth  │   │Driver/Admin  │   │   GIS    │
         │:3000   │   │:3001 / :3002 │   │  :3003   │
         └───┬────┘   └──────┬───────┘   └─────┬────┘
             │               │                  │
        ┌────▼────┐    ┌────▼────────┐    ┌────▼────┐
        │Keycloak │    │ platform_db │    │  Redis  │
        │  users  │    │  inventory  │    │  cache  │
        └─────────┘    └─────────────┘    └─────────┘
```

### Data Write Pattern

- Driver Service and Admin Service write directly to `inventory` schema (station, charger, partner tables) — synchronous, no outbox
- Writes to `inventory.station` / `charger` trigger synchronous cache invalidation on GIS Service Redis
- Auth Service is sole writer to `users` schema
- Admin Service writes directly to `analytics_db`

### Data Read Pattern

- All spatial reads (`/api/v1/nearby`, map queries) route through GIS Service
- GIS Service checks Redis cache first, falls back to PostGIS query, populates cache on miss
- Non-spatial reads (station details by ID, partner details) served directly by Driver/Admin services from `platform_db`

---

## Infrastructure

| Component | MVP | Notes |
|-----------|-----|-------|
| Traefik | MVP-6 | TLS termination, path-based routing to services |
| Redis | MVP-2+ | GIS query cache, added alongside GIS Service |
| Keycloak | MVP-3 | Two realms, identity brokering |
| OSM Importer | MVP-2 | One-shot ETL: OSM → `gis` schema |

All components run in Docker Compose (`infra/docker-compose.yml`).

---

## Key Architectural Decisions

- **Read/write separation**: spatial reads go through GIS Service (read-optimized), writes go direct to `inventory` (no outbox during validation phase)
- **No event bus**: synchronous cache invalidation only — GIS Service exposes a cache-bust endpoint called by Driver/Admin services after writes
- **Single `platform_db`**: not separated by schema ownership boundaries; all services share one PostGIS instance with schema-level access control
- **GIS Service as an independent service**: not a library embedded in Driver Service — enables independent scaling, Redis caching, and read-optimized configuration
