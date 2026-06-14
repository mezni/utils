# BorneMap Architecture

## System Topology

[Client App] --> Traefik Gateway (:80)
 |
 |-- /api/v1/driver/* --> driver-service (:3001) [Read-Only GIS Proximity]
 |-- /api/v1/admin/*  --> admin-service  (:3002) [Read/Write Inventory]

## Clean Architecture (4-Tier)

### Layer Rules

| Layer | Imports | Responsibilities |
|-------|---------|-----------------|
| Domain | None (zero framework/HTTP/DB imports) | Core entities, native types, parsing, geographical bounds |
| UseCase | Domain | Interactors, business rules, Repository trait contracts |
| Adapter | UseCase | Actix-web controllers, DTOs, Utoipa serialization |
| Infrastructure | Domain | SQLx clients, PostGIS queries, repository implementations |

## Gateway Routing

Traefik performs path stripping via `stripPrefix` middleware so internal Actix routers
see only service-local paths (e.g., `/stations/nearby`), not the full `/api/v1/driver/` URL.

## Database Topology

Single `platform_db` cluster with two isolated schemas:
- **gis** — spatial discovery cache (GIST-indexed geography columns)
- **inventory** — core asset tracking (partners, stations)

Cross-schema sync: database triggers propagate inventory mutations to gis automatically.

## ID Strategy

Primary keys follow the pattern `{prefix}-{nanouuid}`:
- `stn-abc123xyz` — station
- `ptn-456defuvw` — partner
- `usr-mvp1-fallback` — MVP-1 auth fallback

## Security Boundaries

Coordinate validation enforces Tunisia bounding box before any database operation:
- Longitude: 7.0000 – 12.0000
- Latitude: 30.0000 – 38.0000

All database queries use SQLx parameter binding markers ($1, $2) — string concatenation
is strictly forbidden.
