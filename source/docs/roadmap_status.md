# Roadmap Status

**Last Updated:** June 2026

## MVP-1: Spatial Core Validation Pipeline
| Task | Sprint | Status | Notes |
|------|--------|--------|-------|
| Directory structure initialization | — | ✅ Complete | |
| Sprint 1.1 specification & planning | 1.1 | ✅ Complete | Spec, plan, tasks, analysis all passed |
| PostGIS 16 Docker profile | 1.1 | ✅ Complete | Running on port 5432 |
| `gis` & `inventory` schema DDL (`init.sql`) | 1.1 | ✅ Complete | 3 tables, indexes, seed data |
| Containerized `osm-importer` for Tunisia PBF | 1.1 | ✅ Complete | Tunisia OSM data loaded |
| Native `gis.get_nearby_stations` PostGIS function | 1.1 | ✅ Complete | Moved from inventory schema; all tests pass |
| Inventory→GIS sync outbox (sync_outbox + trigger + worker) | 1.1 | ✅ Complete | Transactional outbox; gis.process_sync_outbox() drains to gis.osm_stations |
| Sprint 1.2 specification & planning | 1.2 | ✅ Complete | Spec, plan, tasks, analysis all passed |
| Actix-web `driver-service` scaffold | 1.2 | ✅ Complete | `source/services/driver-service/` — Cargo.toml, config, logging, main.rs |
| SQLx connection pool + `/health` endpoint | 1.2 | ✅ Complete | db/pool.rs + api/health.rs — 500ms timeout, 200/503 |
| `/api/v1/nearby` endpoint | 1.2 | ✅ Complete | api/nearby.rs — input validation, PostGIS function call, JSON response |
| 4 integration tests | 1.2 | ✅ Complete | All passing against live DB |
| Traefik reverse proxy routing | 1.2 | ✅ Complete | dynamic.yml — PathPrefix(/api/v1/) → driver-service:3001 with health check |
| Sprint 1.3 specification & planning | 1.3 | ✅ Complete | Spec, plan, tasks, analysis, constitution check all passed — `specs/003-mobile-driver-app/` |
| Expo SDK 54 mobile driver app scaffold | 1.3 | 🔴 Pending | Map, markers, shimmer, offline cache, macro-zoom overlay |
| Sprint 1.4 specification & planning | 1.4 | ✅ Complete | Spec written at `specs/004-web-driver-client/spec.md` |
| Web driver client (React + Leaflet) scaffold | 1.4 | 🔴 Pending | Full-screen map, station markers, shimmer loading, viewport debouncing |
| Station detail drawer | 1.4 | 🔴 Pending | Premium sliding card with station info |
| Macro-zoom overlay | 1.4 | 🔴 Pending | Blocks interaction when zoomed out too far |

## MVP-2: Central Identity Integration
| Task | Status | Notes |
|------|--------|-------|
| Keycloak realm provisioning | 🔴 Pending | |
| Auth service skeleton | 🔴 Pending | |
| Driver registration flow | 🔴 Pending | |
| Admin invitation framework | 🔴 Pending | |

## MVP-3: Partner Curation Portals
| Task | Status | Notes |
|------|--------|-------|
| Admin service skeleton | 🔴 Pending | |
| Dashboard UI scaffold | 🔴 Pending | |
| Station/charger CRUD | 🔴 Pending | |
| Cache-busting integration | 🔴 Pending | |

## MVP-4: Real-time Metadata Extensibility
*(Not yet planned in detail)*

## MVP-5: Analytical Ingestion & Caching
*(Not yet planned in detail)*

## MVP-6: Production Hardening
*(Not yet planned in detail)*
