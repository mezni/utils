# Sprint 001 — EV Charging Platform Foundation

**Sprint ID**: sprint-001
**Status**: Active
**Phase**: FOUNDATION

## Goal

Deliver a fully functional geospatial EV charging backbone including: database and Docker Compose infrastructure, OSM → GIS ingestion, inventory domain schema (Partners, Stations, Chargers, Connectors), sync engine with idempotent pipeline, spatial query function, driver REST API with health check and nearby endpoint, and a minimal driver web map application.

## Deliverables

1. **Infrastructure**: Docker Compose with PostgreSQL 16 + PostGIS, service scaffolds, migrations, init scripts
2. **OSM → GIS Ingestion**: OpenStreetMap fetcher, parser, staging table, idempotent import
3. **Inventory Schema**: Partners (PAR-), Stations (STA-), Chargers (CHR-), Connectors (CON-), typed nanoid IDs, FK cascade
4. **Sync System + Nearby**: Materialized view mv_stations_geo, find_nearby_stations function with power tier classification
5. **Driver Service**: GET /health and GET /nearby REST endpoints
6. **Driver Web App**: Map view with station markers, distance indicators, power tier badges

## 6 User Stories

### US1: Database + Docker Compose
Provision PostGIS database and Docker Compose orchestration layer.

### US2: Import OSM to GIS
Ingest OSM charging station POIs into GIS staging table.

### US3: Create Inventory Schema
Build domain model with Partner → Station → Charger → Connector hierarchy.

### US4: Sync System + Nearby Function
Map staging → inventory and provide spatial query function.

### US5: Driver Service API
Expose GET /health and GET /nearby endpoints.

### US6: Driver Web App
Render stations on a map with location markers and details.
