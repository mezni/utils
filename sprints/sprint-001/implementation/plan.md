# Sprint 001 — Implementation Plan

## Summary

Build EV charging platform incrementally:

1. **Infrastructure**: Docker Compose, PostgreSQL + PostGIS, service scaffolds, migrations
2. **OSM Ingestion**: Fetch from Overpass API, parse POIs, store in GIS staging table
3. **Inventory Schema**: Partners, Stations, Chargers, Connectors with typed nanoid IDs (PAR-, STA-, CHR-, CON-)
4. **Sync + Nearby**: Map staging → inventory, materialized view with power tier, find_nearby_stations function
5. **Driver API**: GET /health and GET /nearby endpoints with latency tracking
6. **Web App**: Leaflet map with station markers, distance, power tier badges, availability

## Architecture

### Services
| Service | Role | Schema Ownership |
|---------|------|------------------|
| driver-api (:3001) | nearby search API (read-only) | reads mv_stations_geo |
| admin-api (:3002) | station CRUD (out of scope for now) | inventory schema |
| sync-engine | OSM → inventory sync | writes inventory |
| ingestion | OSM fetcher | — |
| web (:5173) | map interface | — |

### Tech Stack
- **Backend**: Rust 1.85+ (Axum, sqlx, georust)
- **Frontend**: Node.js 22+, React, Leaflet
- **Database**: PostgreSQL 16 + PostGIS 3.4+
- **Cache**: Redis 7+ (optional)
- **Infra**: Docker Compose

### Key Design Rules
- All geo queries via `mv_stations_geo` materialized view — never base tables
- GiST index on `stations.location`
- Typed nanoid IDs: PAR-, STA-, CHR-, CON-, JOB-
- Idempotent upsert for all imports (ON CONFLICT DO UPDATE)
- sync_jobs audit trail for every import

## Execution Order

```
Phase 1 (Setup)
  ↓
Phase 2 (OSM → GIS) + Phase 3 (Inventory Schema) [parallel]
  ↓
Phase 4 (Sync + Nearby)
  ↓
Phase 5 (Driver API)
  ↓
Phase 6 (Web App)
  ↓
Polish (caching, logging, validation)
```

## Deliverables

1. **Infrastructure**: Docker Compose with PostgreSQL + PostGIS, all service scaffolds
2. **Ingestion**: OSM data in staging table, idempotent import script
3. **Inventory Schema**: Partners → Stations → Chargers → Connectors with cascade FKs
4. **Spatial Layer**: mv_stations_geo with power tier, find_nearby_stations function
5. **Driver API**: GET /health (200 with status, DB, timestamp), GET /nearby (sorted by distance)
6. **Web App**: Map view with markers, badges, distance indicators

See `specs/001-ev-charging-foundation/tasks.md` for detailed 48-task breakdown.

## Success Criteria

- Docker Compose starts and PostGIS is available in <30s
- OSM import with 50 stations completes with zero duplicates on re-run
- Inventory schema enforces FK cascade — deleting station removes all children
- find_nearby_stations returns <2s and nearest first with power tiers
- GET /health returns ok with DB status, GET /nearby returns <150ms
- Web app renders markers accurately on map with distance and power info
