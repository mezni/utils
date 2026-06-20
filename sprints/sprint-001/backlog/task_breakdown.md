# Sprint 001 — Task Breakdown

## Phase 1: Infrastructure & Database

| Task | ID | Owner | Est. | Depends On |
|---|---|---|---|---|
| Docker Compose setup (PostgreSQL 16 + PostGIS) | T-001 | — | 2h | — |
| Redis service in Docker Compose | T-002 | — | 30m | — |
| Database init scripts (postgis, hstore, pgcrypto) | T-003 | — | 1h | T-001 |
| Internal Docker network configuration | T-004 | — | 30m | T-001 |
| Service stubs (driver-api, sync-engine, ingestion) | T-005 | — | 1h | T-001 |

## Phase 2: Domain Schema (Inventory)

| Task | ID | Owner | Est. | Depends On |
|---|---|---|---|---|
| Partner table (PAR- nanoid, type, verification, metadata JSONB) | T-101 | — | 1h | T-003 |
| Station table (STA- nanoid, FK→partner, geography, status, hstore tags) | T-102 | — | 1.5h | T-101 |
| Charger table (CHR- nanoid, FK→station, vendor/model, firmware, status) | T-103 | — | 1h | T-102 |
| Connector table (CON- nanoid, FK→charger, type, current_type, power, availability) | T-104 | — | 1h | T-103 |
| Status/type lookup tables | T-105 | — | 1h | T-003 |
| Data sources registry | T-106 | — | 30m | T-003 |
| ID generation utility (typed nanoid) | T-107 | — | 1h | — |

## Phase 3: OSM Ingestion & Sync Engine

| Task | ID | Owner | Est. | Depends On |
|---|---|---|---|---|
| OSM raw staging table in gis schema | T-201 | — | 1h | T-003 |
| OSM import script (Overpass API → staging) | T-202 | — | 2h | T-201 |
| sync_osm_charging_stations() function (map POIs→stations) | T-203 | — | 3h | T-102, T-201 |
| Deduplication logic (osm_id + spatial proximity) | T-204 | — | 2h | T-203 |
| sync_jobs table and tracking | T-205 | — | 1h | T-003 |
| Idempotency verification (upsert + versioning) | T-206 | — | 1h | T-203 |

## Phase 4: GIS Query Layer

| Task | ID | Owner | Est. | Depends On |
|---|---|---|---|---|
| `mv_stations_geo` — metadata + geo + availability + power_tier | T-301 | — | 2h | T-102, T-103, T-104 |
| Power tier logic (ultra_fast≥150kW, fast≥50kW, medium≥22kW, slow<22kW) | T-302 | — | 30m | T-301 |
| `find_nearby_stations(lat, lon, radius, limit)` function | T-303 | — | 2h | T-301 |
| GiST index on stations.location | T-304 | — | 30m | T-102 |
| Query performance validation (<50ms urban radius) | T-305 | — | 1h | T-303 |

## Phase 5: Driver API Service

| Task | ID | Owner | Est. | Depends On |
|---|---|---|---|---|
| Service scaffold (Rust, Axum/Actix) | T-401 | — | 1h | T-005 |
| `GET /health` endpoint (DB connectivity, timestamp) | T-402 | — | 30m | T-401 |
| `GET /nearby` endpoint (lat, lon, radius → stations) | T-403 | — | 2h | T-303, T-401 |
| `GET /stations/:id` endpoint (full detail + chargers + connectors) | T-404 | — | 1.5h | T-102, T-103, T-104, T-401 |
| Redis caching layer (optional) | T-405 | — | 1h | T-002, T-403 |
| Latency tracking middleware | T-406 | — | 1h | T-401 |
| Error handling + logging | T-407 | — | 1h | T-401 |

## Phase 6: Driver Web Application

| Task | ID | Owner | Est. | Depends On |
|---|---|---|---|---|
| Web app scaffold (Node.js, React/Leaflet) | T-501 | — | 1h | — |
| Map view with user location tracking | T-502 | — | 2h | T-501 |
| Station markers from /nearby API | T-503 | — | 1.5h | T-403, T-502 |
| Distance indicators on markers | T-504 | — | 1h | T-503 |
| Station list panel (sorted by proximity, availability, power tier badges) | T-505 | — | 2h | T-503 |
| Station detail view (charger breakdown, connector types) | T-506 | — | 2h | T-404, T-501 |
| Empty/low-coverage region graceful handling | T-507 | — | 1h | T-503 |
| Error states and loading indicators | T-508 | — | 1h | T-501 |

## Phase 7: Cross-Cutting & Polish

| Task | ID | Owner | Est. | Depends On |
|---|---|---|---|---|
| Full integration test — Docker Compose up validates all services | T-601 | — | 1h | All phases |
| OSM ingestion → station extraction → nearby query | T-602 | — | 1h | T-202, T-303 |
| Performance validation (<150ms API, <50ms spatial query) | T-603 | — | 1h | T-403, T-303 |
| Documentation — architecture, API, setup instructions | T-604 | — | 2h | All phases |
| System state update | T-605 | — | 30m | All phases |

## Dependency Graph

```
T-001 ──→ T-003 ──→ T-105, T-106, T-201
                       │
              ┌────────┴────────┐
              │                 │
           T-101              T-201 ──→ T-202
              │                        │
           T-102 ◄─────────────────────┘
              │
           T-103 ──→ T-104
              │         │
              └────┬────┘
                   │
                T-301 ──→ T-302 → T-303 → T-304 → T-305
                   │
              T-401 ──→ T-402 → T-403 → T-404
                             │         │
                          T-405     T-501 → T-502 → T-503 → T-505 → T-506
                                              │         │
                                           T-504     T-507, T-508
```

## Execution Order (Recommended)

1. Phase 1: Infrastructure & Database
2. Phase 2: Domain Schema (parallel with Phase 3)
3. Phase 3: OSM Ingestion & Sync Engine (parallel with Phase 2)
4. Phase 4: GIS Query Layer
5. Phase 5: Driver API Service
6. Phase 6: Driver Web Application
7. Phase 7: Cross-Cutting & Polish
