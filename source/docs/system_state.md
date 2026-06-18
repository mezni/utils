# System State

**Last Updated:** June 2026

## Current Phase
**MVP-1 — Spatial Core Validation Pipeline (Sprint 1.3 — Planning Complete)**

## Infrastructure Status
| Component | Status | Notes |
|-----------|--------|-------|
| Directory Structure | ✅ Initialized | `source/` tree created per constitution §3 |
| Docker Compose | ✅ Updated | `source/infra/docker-compose.yml` — platform_db, driver-service, traefik services |
| PostgreSQL / PostGIS 16 | ✅ Running | `bornemap-db` container, healthy, port 5432 |
| Osm-importer container | ✅ Built | `bornemap/osm-importer` — Tunisia data loaded into gis schema |
| Traefik Gateway | ✅ Configured | `source/infra/traefik/dynamic.yml` — routes /api/v1/* to driver-service:3001 |
| Keycloak | 🔴 Not Provisioned | MVP-2 task |
| Redis | 🔴 Not Running | MVP-5 task |

## Service Status
| Service | Port | Status | Notes |
|---------|------|--------|-------|
| Auth Service | :3000 | 🔴 Not Started | MVP-2 task |
| Driver Service (Rust/Actix) | :3001 | ✅ Implemented | `source/services/driver-service/` — nearby + health endpoints |
| Admin Service | :3002 | 🔴 Not Started | MVP-3 task |

## Client Status
| Client | Tech Stack | Status | Notes |
|--------|-----------|--------|-------|
| Mobile Driver | Expo SDK 54 | 🟡 Planning Complete | Sprint 1.3 — spec, plan, tasks, analysis complete |
| Web Driver | React + Leaflet | 🔴 Not Scaffolded | Sprint 1.4 task — spec written |
| Dashboard | React + shadcn/ui | 🔴 Not Scaffolded | MVP-3 task |

## Database State
- `platform_db` (PostGIS) — ✅ Running (gis, inventory, public schemas; partner/station/charger tables; gis.osm_stations mirrored layer; sync_outbox + trigger; gis.get_nearby_stations; gis.process_sync_outbox worker; Tunisia OSM data loaded)
- `keycloak_db` — Not created
- `analytics_db` — Not created

## Shared Packages/Crates
| Package/Crate | Status | Notes |
|---------------|--------|-------|
| `packages/shared-types` | 🔴 Not Created | Sprint 1.3 |
| `packages/shared-hooks` | 🔴 Not Created | Sprint 1.3 |
| `packages/shared-ui` | 🔴 Not Created | Sprint 1.3 |
| `crates/db-models` | 🔴 Stubbed | Cargo workspace member created |
| `crates/validation` | 🔴 Stubbed | Cargo workspace member created |

## Identity
- Keycloak realm `bornemap` — Not provisioned

## Tracking Files
| File | Status |
|------|--------|
| `docs/constitution.md` | ✅ Created (v1.4) |
| `docs/system_state.md` | ✅ Updated (this file) |
| `docs/roadmap_status.md` | ✅ Updated |
| `docs/sprint_backlog.md` | ✅ Updated |
| `docs/bug_tracker.md` | ✅ Created |
| `.speckit/rules.md` | ✅ Created |
| `.specify/memory/constitution.md` | ✅ Created (v1.0.0) |

## Sprint 1.1 Artifacts
| Artifact | Path | Status |
|----------|------|--------|
| Spec | `specs/001-core-data-storage/spec.md` | ✅ Passed analysis |
| Plan | `specs/001-core-data-storage/plan.md` | ✅ Passed constitution check |
| Tasks | `specs/001-core-data-storage/tasks.md` | ✅ 22 tasks, 6 phases — all completed |
| Research | `specs/001-core-data-storage/research.md` | ✅ Decisions documented |
| Data Model | `specs/001-core-data-storage/data-model.md` | ✅ 3 entities defined |
| Contracts | `specs/001-core-data-storage/contracts/` | ✅ Function + compose contracts |
| Quickstart | `specs/001-core-data-storage/quickstart.md` | ✅ Setup instructions |
| Analysis | `specs/001-core-data-storage/checklists/requirements.md` | ✅ 16/16 items passing |

## Sprint 1.2 Artifacts
| Artifact | Path | Status |
|----------|------|--------|
| Spec | `specs/002-driver-service-api/spec.md` | ✅ Passed analysis |
| Plan | `specs/002-driver-service-api/plan.md` | ✅ Passed constitution check |
| Tasks | `specs/002-driver-service-api/tasks.md` | ✅ 19 tasks, 6 phases — all completed |
| Research | `specs/002-driver-service-api/research.md` | ✅ Decisions documented |
| Data Model | `specs/002-driver-service-api/data-model.md` | ✅ API response + config + state models |
| Contracts | `specs/002-driver-service-api/contracts/` | ✅ nearby-api.md, health-api.md |
| Quickstart | `specs/002-driver-service-api/quickstart.md` | ✅ Setup + test instructions |

## Sprint 1.3 Artifacts
| Artifact | Path | Status |
|----------|------|--------|
| Spec | `specs/003-mobile-driver-app/spec.md` | ✅ Passed analysis |
| Plan | `specs/003-mobile-driver-app/plan.md` | ✅ Passed constitution check |
| Tasks | `specs/003-mobile-driver-app/tasks.md` | ✅ 28 tasks, 8 phases — ready for implementation |
| Research | `specs/003-mobile-driver-app/research.md` | ✅ 7 decisions documented |
| Data Model | `specs/003-mobile-driver-app/data-model.md` | ✅ 5 entities + state transitions |
| Contracts | `specs/003-mobile-driver-app/contracts/nearby-api.md` | ✅ Mobile client behavior matrix |
| Quickstart | `specs/003-mobile-driver-app/quickstart.md` | ✅ Setup + 8 test scenarios |
| Analysis | `specs/003-mobile-driver-app/checklists/requirements.md` | ✅ 16/16 items passing |

## Sprint 1.4 Artifacts
| Artifact | Path | Status |
|----------|------|--------|
| Spec | `specs/004-web-driver-client/spec.md` | ✅ Draft — ready for planning |
| Checklist | `specs/004-web-driver-client/checklists/requirements.md` | ✅ 16/16 items passing |
