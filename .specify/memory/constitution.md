# BorneMap MVP-1 Constitution

## Core Principles

### I. Database-First Architecture
Every feature starts at the database layer. Schema, indexes, and functions are defined before any service code. PostGIS is the sole geospatial authority — all geo-filtering lives in SQL, not application code.

### II. GIS Isolation
The `gis` schema is read-only cache layer. Source of truth is `inventory.stations`. Cross-schema replication is handled by PL/pgSQL triggers only — no application-level dual-write logic.

### III. Contract-First API
All endpoints MUST follow `/api/v1/*`. No undocumented endpoints. No ad-hoc endpoint creation. Request/response shapes are frozen before implementation.

### IV. Mock Identity (MVP-1)
auth-service is deferred. All records use `usr-mvp1-fallback` as `created_by`/`updated_by`. JWT validation is out of scope until Phase 2.

### V. No Business Logic in Rust
All geospatial filtering, aggregation, and transformation runs in PostGIS functions. Rust services are thin HTTP-to-DB bridges with direct query-to-response mapping.

### VI. Single Service (MVP-1)
Only `driver-service` is implemented. `admin-service` and `auth-service` are deferred. No inter-service communication exists.

### VII. No Microservice Sprawl
Allowed components: postgres, driver-service, mobile-app, web-driver, osm-importer, traefik (minimal). No Redis, Kafka, queues.

### VIII. OSM Import to gis Only
OSM import script writes directly to `gis.osm_stations` with `source='OSM_IMPORT'`. It does NOT create `inventory.*` records. Inventory population is handled separately via seed data or admin-service (Phase 3).

## Technology Stack

| Layer | Technology | Constraint |
|-------|-----------|------------|
| Database | PostgreSQL 17 + PostGIS | No ORM, no extensions beyond PostGIS |
| Backend | Rust + Actix-Web 4 + SQLx 0.7 | No other web frameworks |
| Mobile | Expo SDK 54 + react-native-maps | No heavy state frameworks |
| Web | React + Vite + Leaflet | Shared types with mobile |
| Routing | Traefik v3 | Minimal config only |
| Import | bash + curl + psql | No ETL tools |

## Service Boundaries (MVP-1)

| Service | Port | Responsibility |
|---------|------|----------------|
| driver-service | 3001 | Geospatial discovery (read-only) |
| auth-service | 3000 | Deferred to Phase 2 |
| admin-service | 3002 | Deferred to Phase 3 |

## Database Ownership

| Schema | Owner | Access |
|--------|-------|--------|
| inventory | admin-service (deferred) | driver-service read |
| gis | driver-service (read-only) | Direct reads only |
| configuration | admin-service (deferred) | References only |
| users | auth-service (deferred) | Not used in MVP-1 |

## Entity ID Standard

- Stations: `STA-` prefix
- Partners: `PRT-` prefix
- Chargers: `CHR-` prefix

## Quality Gates

Before any implementation:
- [ ] DB schema defined and reviewed
- [ ] GIS functions defined and reviewed
- [ ] API contracts documented
- [ ] Data flow mapped end-to-end

## Governance

This constitution supersedes all other practices for MVP-1. Amendments require documentation in an ADR. All PRs/reviews must verify compliance with these rules.

**Version**: 1.0.0 | **Ratified**: 2026-06-14 | **Last Amended**: 2026-06-14
