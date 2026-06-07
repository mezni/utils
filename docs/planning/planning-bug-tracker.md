# BorneMap — Planning & Bug Tracker

## Project Overview

**Platform**: EV station discovery and management for Tunisia
**Stack**: Rust + React + React Native + PostgreSQL/PostGIS + Keycloak
**Language**: English, French
**Repository**: github.com/mezni/BorneMap

---

## Active Phase: Phase 1 — Foundation

**Goal**: CI/CD pipeline running, database with GIS and inventory schemas, two Rust services with real endpoints, three frontend apps scaffolded with core screens. Everything wired together and verified.

---

## Sprint 1.1 — Monorepo and CI/CD

**Duration**: 2 weeks
**Status**: 🟢 Complete

### Tasks

| ID | Title | Status | Owner | Notes |
|---|---|---|---|---|
| TASK-01 | Initialize monorepo directory structure | ✅ Done | — | Full monorepo tree created |
| TASK-02 | Configure Cargo workspace root | ✅ Done | — | workspace Cargo.toml with shared dependencies |
| TASK-03 | Configure npm workspace | ✅ Done | — | root package.json, tsconfig.base.json, eslint/prettier |
| TASK-04 | Create ev-core shared crate | ✅ Done | — | NanoID generators, shared enums, 12 tests pass |
| TASK-05 | Create ev-db shared crate | ✅ Done | — | PgPool factory, pagination structs |
| TASK-06 | Create ci.yml — full workspace CI | ✅ Done | — | Rust lint+test + Frontend lint+build with caching |
| TASK-07 | Create ci-driver-service.yml | ✅ Done | — | Path-scoped, PostgreSQL container, cargo caching |
| TASK-08 | Create ci-admin-service.yml | ✅ Done | — | Path-scoped, PostgreSQL container, cargo caching |
| TASK-09 | Create ci-driver-web.yml | ✅ Done | — | Path-scoped, npm caching |
| TASK-10 | Create ci-driver-mobile.yml | ✅ Done | — | Path-scoped, npm caching, TypeScript check |
| TASK-11 | Create ci-dashboard.yml | ✅ Done | — | Path-scoped, npm caching |
| TASK-12 | Create environment file examples | ✅ Done | — | .env.example, driver-service.env.example, admin-service.env.example |
| TASK-13 | Create baseline Docker Compose | ✅ Done | — | docker-compose.yml (dev + pgadmin) and docker-compose.prod.yml |
| TASK-14 | Create .gitignore and .dockerignore | ✅ Done | — | Covers target/, node_modules/, .env, .specify/ |

### Sprint 1.1 Done Criteria

- [x] `cargo build --all` succeeds — verified (zero warnings)
- [x] `npm install` succeeds — verified (868 packages)
- [x] All CI workflows created and ready for test push
- [x] Docker Compose config validated; PostgreSQL starts cleanly
- [x] ev-core tests pass (`cargo test -p ev-core` — 12/12 pass)

---

## Sprint 1.2 — Database: GIS and Inventory Schemas

**Duration**: 2 weeks
**Status**: 🔴 Not Started

### Tasks

| ID | Title | Status | Owner | Notes |
|---|---|---|---|---|
| TASK-15 | Migration 0001 — Extensions (PostGIS, uuid-ossp, pgcrypto) | 🔴 Planned | — | |
| TASK-16 | Migration 0002 — Schemas (inventory, gis) | 🔴 Planned | — | |
| TASK-17 | Migration 0003 — Inventory tables (partner, station, charger, station_availability) | 🔴 Planned | — | |
| TASK-18 | Migration 0004 — Inventory indexes | 🔴 Planned | — | |
| TASK-19 | Migration 0005 — GIS tables (osm_nodes, osm_ways, roads, boundaries, amenity_points, station_locations) | 🔴 Planned | — | |
| TASK-20 | Migration 0006 — GIS GiST indexes | 🔴 Planned | — | |
| TASK-21 | Dev seeds — partners | 🔴 Planned | — | 3 partners |
| TASK-22 | Dev seeds — stations | 🔴 Planned | — | 15 stations across Tunisia |
| TASK-23 | Dev seeds — chargers | 🔴 Planned | — | 24 chargers |
| TASK-24 | Create migrate.sh runner | 🔴 Planned | — | |

### Sprint 1.2 Done Criteria

- [ ] All six migrations run from zero without errors on a fresh database
- [ ] Both schemas exist
- [ ] Seeds insert 3 partners, 15 stations, 24 chargers without errors
- [ ] All GiST indexes exist
- [ ] Spatial query test passes (ST_DWithin within 5km returns results)

---

## Sprint 1.3 — Driver Service

**Duration**: 2 weeks
**Status**: 🔴 Not Started

### Tasks

| ID | Title | Status | Owner | Notes |
|---|---|---|---|---|
| TASK-25 | Driver Service — Cargo.toml and structure | 🔴 Planned | — | |
| TASK-26 | Driver Service — Config, errors | 🔴 Planned | — | |
| TASK-27 | Driver Service — Health endpoint | 🔴 Planned | — | GET /api/v1/health |
| TASK-28 | Driver Service — Stations nearby endpoint | 🔴 Planned | — | GET /api/v1/stations/nearby |
| TASK-29 | Driver Service — Router configuration | 🔴 Planned | — | |
| TASK-30 | Driver Service — Main with migrations | 🔴 Planned | — | |
| TASK-31 | Driver Service — Dockerfile | 🔴 Planned | — | Multi-stage Rust build |
| TASK-32 | Driver Service — Integration tests | 🔴 Planned | — | nearby returns results, empty when far |

### Sprint 1.3 Done Criteria

- [ ] GET /api/v1/health returns `{"status":"ok","service":"driver-service","db":"ok"}`
- [ ] GET /api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=5 returns stations from seeds
- [ ] Both integration tests pass
- [ ] Service starts and connects to PostgreSQL via Docker Compose
- [ ] CI pipeline passes for driver-service

---

## Sprint 1.4 — Admin Service

**Duration**: 2 weeks
**Status**: 🔴 Not Started

### Tasks

| ID | Title | Status | Owner | Notes |
|---|---|---|---|---|
| TASK-33 | Admin Service — Cargo.toml and structure | 🔴 Planned | — | |
| TASK-34 | Admin Service — Config, errors | 🔴 Planned | — | |
| TASK-35 | Admin Service — Health endpoint | 🔴 Planned | — | GET /api/v1/health |
| TASK-36 | Admin Service — Partner CRUD (5 endpoints) | 🔴 Planned | — | POST/GET/GET/PUT/DELETE |
| TASK-37 | Admin Service — Station CRUD (5 endpoints) | 🔴 Planned | — | POST/GET/GET/PUT/DELETE |
| TASK-38 | Admin Service — Charger CRUD (5 endpoints) | 🔴 Planned | — | POST/GET/GET/PUT/DELETE |
| TASK-39 | Admin Service — Router configuration | 🔴 Planned | — | All 15 endpoints under /api/v1 |
| TASK-40 | Admin Service — Main with migrations | 🔴 Planned | — | |
| TASK-41 | Admin Service — Dockerfile | 🔴 Planned | — | Multi-stage Rust build |
| TASK-42 | Admin Service — Integration tests | 🔴 Planned | — | Partners, stations, chargers |

### Sprint 1.4 Done Criteria

- [ ] GET /api/v1/health returns 200
- [ ] All 15 CRUD endpoints return correct responses
- [ ] POST /api/v1/partners creates a partner with PRT-... ID
- [ ] POST /api/v1/stations creates a station for that partner
- [ ] POST /api/v1/chargers creates a charger for that station
- [ ] All integration tests pass
- [ ] CI pipeline passes for admin-service

---

## Sprint 1.5 — Frontend Apps Scaffold

**Duration**: 2 weeks
**Status**: 🔴 Not Started

### Tasks

| ID | Title | Status | Owner | Notes |
|---|---|---|---|---|
| TASK-43 | Driver Web — Project setup (Vite + React + Tailwind) | 🔴 Planned | — | |
| TASK-44 | Driver Web — MapPage with Leaflet | 🔴 Planned | — | Map + station markers from real API |
| TASK-45 | Driver Web — Proxy config for /api/v1 | 🔴 Planned | — | Vite proxy to driver-service |
| TASK-46 | Driver Mobile — Project setup (Expo SDK 54) | 🔴 Planned | — | |
| TASK-47 | Driver Mobile — MapScreen with react-native-maps | 🔴 Planned | — | MapView + markers from real API |
| TASK-48 | Driver Mobile — Location permission handling | 🔴 Planned | — | Graceful denial → default coords |
| TASK-49 | Dashboard — Project setup (Vite + React + Tailwind) | 🔴 Planned | — | |
| TASK-50 | Dashboard — AppShell with sidebar navigation | 🔴 Planned | — | 4 routes: Overview, Partners, Stations, Chargers |
| TASK-51 | Dashboard — Placeholder pages | 🔴 Planned | — | Overview (stat cards), Partners/Stations/Chargers (placeholder) |

### Sprint 1.5 Done Criteria

- [ ] Driver Web App starts at localhost:5173 with Leaflet map + station markers from Driver Service
- [ ] Clicking a marker shows station name, available charger count, distance
- [ ] Driver Mobile App starts on iOS simulator and Android emulator with MapView + markers
- [ ] Dashboard App starts at localhost:5174 with working left sidebar navigation
- [ ] Dashboard active nav item shows #EAF0E6 background and #007943 text
- [ ] All three frontend CI workflows pass

---

## Sprint 1.6 — Phase 1 Hardening

**Duration**: 1 week
**Status**: 🔴 Not Planned

### Tasks

| ID | Title | Status | Owner | Notes |
|---|---|---|---|---|
| TASK-52 | Backend fix sweep | 🔴 Planned | — | clippy, tests, health checks, indexes, FKs |
| TASK-53 | Frontend fix sweep | 🔴 Planned | — | Network calls, mobile layout, responsive, graceful errors |
| TASK-54 | CI verification | 🔴 Planned | — | Break commits, verify workflow triggers |
| TASK-55 | Write onboarding guide | 🔴 Planned | — | docs/guides/onboarding.md |
| TASK-56 | Write API docs for driver-service | 🔴 Planned | — | docs/api/v1/driver-service.md |
| TASK-57 | Write API docs for admin-service | 🔴 Planned | — | docs/api/v1/admin-service.md |
| TASK-58 | Write Phase 1 status report | 🔴 Planned | — | docs/project/phases/phase-01-status.md |

### Phase 1 Done Criteria

- [ ] `cargo build --all` succeeds with zero warnings
- [ ] `cargo test --all` passes — all integration tests green
- [ ] `npm build` succeeds for driver-web and dashboard
- [ ] `npm tsc --noEmit` passes for driver-mobile
- [ ] All six CI workflows pass on main branch
- [ ] Both services start in Docker Compose and pass health checks
- [ ] GET /api/v1/health returns ok with db:ok on both services
- [ ] GET /api/v1/stations/nearby returns real stations from seeds
- [ ] All 15 admin CRUD endpoints tested and working
- [ ] Driver Web shows map with station markers from real API
- [ ] Driver Mobile shows map with station markers from real API
- [ ] Dashboard shows left sidebar with four navigable routes
- [ ] Location permission denial handled gracefully on mobile
- [ ] Zero Class A bugs open
- [ ] docs/guides/onboarding.md complete and tested
- [ ] docs/api/v1/ documents for both services written

---

## Bugs

| ID | Title | Sprint | Status | Severity | Notes |
|---|---|---|---|---|---|
| — | — | — | — | — | — |

---

## Deferred Items

| Item | Rationale | Target |
|---|---|---|
| OCPP / charging sessions | Out of scope per constitution | Future phase |
| Payments / billing | Out of scope per constitution | Future phase |
| Routing / navigation | Out of scope per constitution | Future phase |
| Push notifications | Deferred per constitution | Future phase |
| Real-time availability (OCPP-driven) | Deferred per constitution | Future phase |
| Arabic / RTL support | Not in scope for current phase | Future phase |
| Keycloak / auth integration | Phase 2 | Sprint 2.x |
| Clickstream Service / analytics | Phase 2+ | Sprint 2.x |
| ev-auth shared crate | Phase 2 | Sprint 2.x |
| GIS sync trigger | Phase 2 | Sprint 2.x |

---

## Out of Scope

- OCPP protocol handling
- Payment processing
- Route planning / navigation
- Real-time charger availability via OCPP
- Push notifications
- Arabic / RTL support (English and French only)
- Kubernetes or container orchestration
- Image registry (images built on host)
- Automated deployment

---

## ADR Index

| ID | Title | Status |
|---|---|---|
| ADR-001 | PostgreSQL + PostGIS as single database | Accepted |
| ADR-002 | Schema separation over database separation | Accepted |
| ADR-003 | Prefixed NanoIDs over UUIDs | Accepted |
| ADR-004 | Direct analytics insert over RabbitMQ | Accepted |
| ADR-005 | Rust + Actix-web for backend services | Accepted |
| ADR-006 | Bare metal + Docker Compose over Kubernetes | Accepted |
| ADR-007 | Keycloak for authentication | Accepted |
| ADR-008 | PostgreSQL trigger for GIS synchronization | Accepted |
| ADR-009 | Monorepo with Cargo and npm workspaces | Accepted |
| ADR-010 | Traefik as edge router | Accepted |
| ADR-011 | React + Vite for web applications | Accepted |
| ADR-012 | React Native + Expo SDK 54 for mobile app | Accepted |
| ADR-013 | Single Dashboard App over separate Partner and Admin apps | Accepted |
| ADR-014 | Leaflet + OpenStreetMap for web map | Accepted |
| ADR-015 | Local image builds — no image registry | Accepted |

---

**Last Updated**: 2026-06-07
**Phase**: 1 — Foundation
**Active Sprint**: 1.1 (Not Started)
