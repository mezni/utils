# Sprint 001 — Implementation Plan

## Summary
Geospatial EV charging backbone: PostGIS database, OSM ingestion, inventory domain model, sync engine, nearby query system, driver API, and web map application.

## Architecture

### Services
| Service | Role | Schema Ownership |
|---------|------|------------------|
| driver-api (:3001) | Nearby search, station detail (read-only) | gis schema (reads mv_stations_geo) |
| admin-api (:3002) | Partner/station/charger/connector CRUD | inventory schema |
| sync-engine | OSM ingestion + geospatial sync | inventory schema (writes) |
| ingestion | OSM Overpass API fetcher | — |
| web (:5173) | Driver map application | — |

### Tech Stack
- **Backend**: Rust 1.85+ (Axum, sqlx, georust)
- **Frontend**: Node.js 22+, React, Leaflet
- **Database**: PostgreSQL 16 + PostGIS 3.4+
- **Cache**: Redis 7+ (optional)
- **Infra**: Docker Compose

### Key Design Rules
- All geo queries hit `mv_stations_geo` — never base tables
- GiST index on `stations.location`
- Typed nanoid IDs: PAR-, STA-, CHR-, CON-, JOB-
- Idempotent upsert for all imports
- sync_jobs audit trail for every import

## Execution Order
1. Infrastructure: Docker Compose + PostGIS + DB init
2. Domain schema: Partner → Station → Charger → Connector
3. OSM ingestion: Fetcher → Parser → Sync pipeline
4. GIS layer: mv_stations_geo + find_nearby_stations
5. Driver API: /health → /nearby → /stations/:id
6. Web app: MapView → StationList → StationDetail
7. Polish: caching, logging, error handling, validation

See `specs/001-ev-charging-foundation/tasks.md` for full task breakdown.
