# BorneMap — Roadmap

**Last Updated**: 2026-06-07

---

## Phase 1 — Foundation (Active)

**Status**: Sprint 1.1 — Not Started
**Goal**: CI/CD pipeline running, database with GIS and inventory schemas, two Rust services with real endpoints, three frontend apps scaffolded with core screens.

| Sprint | Focus | Duration | Status |
|---|---|---|---|
| 1.1 | Monorepo and CI/CD | 2 weeks | 🔴 Planned |
| 1.2 | Database: GIS and Inventory Schemas | 2 weeks | 🔴 Planned |
| 1.3 | Driver Service | 2 weeks | 🔴 Planned |
| 1.4 | Admin Service | 2 weeks | 🔴 Planned |
| 1.5 | Frontend Apps Scaffold | 2 weeks | 🔴 Planned |
| 1.6 | Phase 1 Hardening | 1 week | 🔴 Planned |

**Related Tasks**: TASK-01 through TASK-58

---

## Phase 2 — Authentication & Users (Future)

**Status**: 🔴 Not Started
**Goal**: Keycloak integration, user registration/login, driver profiles, favorites, reviews.

Key deliverables:
- Keycloak realm configuration and deployment
- ev-auth shared crate (JWT validation, roles, JWKS cache)
- Driver Service: authenticated endpoints (profile, favorites, reviews)
- Auth upgrade modal in driver apps
- packages/auth-client for frontend token management

---

## Phase 3 — Partner Dashboard (Future)

**Status**: 🔴 Not Started
**Goal**: Partner login, station management UI, availability updates, partner-scoped reports.

Key deliverables:
- Admin Service: partner scope enforcement middleware
- Dashboard: partner views for station/charger CRUD
- Dashboard: availability management UI
- Dashboard: partner reports

---

## Phase 4 — Admin Dashboard (Future)

**Status**: 🔴 Not Started
**Goal**: Admin global management, user moderation, platform reporting.

Key deliverables:
- Dashboard: admin views for all entities
- Review moderation UI
- Global reporting dashboards
- User management

---

## Phase 5 — Clickstream & Analytics (Future)

**Status**: 🔴 Not Started
**Goal**: Analytics event ingestion, event taxonomy, reporting.

Key deliverables:
- Clickstream Service
- analytics schema and event storage
- Event taxonomy enforcement
- Admin Service: analytics-based reporting

---

## Phase 6 — GIS & Map Enhancements (Future)

**Status**: 🔴 Not Started
**Goal**: OSM data import, GIS sync trigger, spatial enrichment.

Key deliverables:
- OSM import pipeline
- GIS sync trigger function
- Station location enrichment (road snap, region)
- gis.resync_all_stations() procedure
- Spatial query optimization

---

## Deferred / Out of Scope

| Feature | Rationale |
|---|---|
| OCPP / charging sessions | Requires hardware integration |
| Payments / billing | Separate payment provider integration |
| Routing / navigation | Navigation SDK integration |
| Push notifications | Requires push notification service |
| Arabic / RTL support | Simplified: French only for now |
| ev-geo crate (Rust geo library) | Deferred — SQL/PostGIS sufficient for Phase 1-6 |
| RabbitMQ / message queue | ADR-004 — direct insert sufficient |
| Kubernetes | ADR-006 — Docker Compose sufficient |

---

## Milestone Timeline (Estimated)

| Milestone | Target | Phase |
|---|---|---|
| Monorepo compiles, CI green | Sprint 1.1 | 1 |
| Database seeded, spatial queries work | Sprint 1.2 | 1 |
| Driver Service serves real data | Sprint 1.3 | 1 |
| Admin CRUD complete | Sprint 1.4 | 1 |
| All three frontends show data | Sprint 1.5 | 1 |
| Phase 1 hardened and documented | Sprint 1.6 | 1 |
| Users can register and login | Phase 2 | 2 |
| Partners can manage their stations | Phase 3 | 3 |
| Admins can manage platform | Phase 4 | 4 |
| Analytics pipeline live | Phase 5 | 5 |
| GIS enrichment operational | Phase 6 | 6 |
