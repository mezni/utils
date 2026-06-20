# Sprint 001 — Backlog

## Sprint Goal
Deliver geospatial EV charging platform foundation: database, ingestion, inventory, sync engine, nearby search, driver API, and web map.

## Stories

| ID | Priority | Story | Effort | Status |
|---|---|---|---|---|
| S-001 | P1 | Driver finds nearby charging stations | L | Pending |
| S-002 | P2 | Partner manages station inventory | XL | Pending |
| S-003 | P3 | System operator imports geospatial data | M | Pending |
| S-004 | P4 | Driver views station details | M | Pending |

## Tasks by Story

### S-001 — Nearby station search (P1)
- [ ] Database schema — setup PostGIS + inventory tables
- [ ] Create `mv_stations_geo` materialized view with power tier + availability
- [ ] Implement `find_nearby_stations` spatial function
- [ ] Driver API endpoint: `GET /nearby`
- [ ] Driver web app — map view with station markers
- [ ] GiST index creation on spatial columns

### S-002 — Partner inventory management (P2)
- [ ] Partner entity schema + nanoid generation
- [ ] Station entity schema with geolocation
- [ ] Charger entity schema
- [ ] Connector entity schema
- [ ] Status lookup tables, connector types, current types
- [ ] API endpoint: create/read/update/delete stations
- [ ] Cascading delete enforcement
- [ ] Partner web interface (minimal)

### S-003 — Geospatial data ingestion (P3)
- [ ] OSM ingestion pipeline (raw staging table)
- [ ] Sync engine — map external POIs to stations
- [ ] Idempotency — upsert + deduplication by osm_id + spatial proximity
- [ ] `sync_jobs` audit trail
- [ ] Data sources registry

### S-004 — Station detail view (P4)
- [ ] Driver API endpoint: `GET /stations/:id`
- [ ] Driver web app — station detail page with charger/connector breakdown
- [ ] Availability indicator rendering

### Cross-cutting
- [ ] Docker Compose with PostGIS + Redis + services
- [ ] Health endpoint for all services
- [ ] Error handling and graceful empty states
- [ ] Sync latency tracking
- [ ] Documentation update
