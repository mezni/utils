# Sprint 001 — Scope

## In Scope

- PostgreSQL 16 + PostGIS database initialization with spatial extensions
- OSM data ingestion into GIS-compatible normalized form
- Inventory domain schema: Partners, Stations, Chargers, Connectors
- Typed nanoid ID system (PAR-, STA-, CHR-, CON-, JOB-)
- Status lookup tables, connector types, current types, data sources registry
- Sync engine with idempotent import processing and sync_jobs audit trail
- Materialized view `mv_stations_geo` with power tier classification and availability aggregation
- `find_nearby_stations` spatial function querying exclusively via materialized view
- Driver REST API: `GET /health`, `GET /nearby`, `GET /stations/:id`
- Driver web application with map view, station list, and station detail view
- Docker Compose setup with internal networking and deterministic initialization
- Redis as optional caching layer
- Cross-cutting: GiST spatial indexing, idempotency, observability (sync_jobs logging, latency tracking, error capture)
