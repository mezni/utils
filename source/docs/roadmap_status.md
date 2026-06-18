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
| Actix-web `driver-service` scaffold | 1.2 | 🔴 Pending | |
| SQLx connection pool + `/health` endpoint | 1.2 | 🔴 Pending | |
| `/api/v1/nearby` endpoint | 1.2 | 🔴 Pending | |
| Traefik reverse proxy routing | 1.2 | 🔴 Pending | |
| Expo SDK 54 mobile app scaffold | 1.3 | 🔴 Pending | |
| React + Leaflet web client scaffold | 1.3 | 🔴 Pending | |
| Map markers, loading skeletons, out-of-bounds UX | 1.3 | 🔴 Pending | |

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
