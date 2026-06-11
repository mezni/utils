# MVP Roadmap

---

## Overview

| MVP | Focus | Timeline |
|---|---|---|
| MVP-1 | UX Discovery | Current |
| MVP-2 | Admin + Dashboard | Next |
| MVP-3 | Identity + RBAC | TBD |
| MVP-4 | Analytics | TBD |
| MVP-5 | Performance | TBD |
| MVP-6 | Production infra | TBD |

---

## Execution Strategy

**Delivery model:** MVP-driven incremental delivery, strict vertical slices (not layer-first), each MVP ends with stabilization sprint, no cross-MVP feature leakage.

**Core rule:** Each MVP must be production-shaped, even if scope-limited.

### Phase Structure

| Phase | Goal | Outcome |
|---|---|---|
| MVP-1 | Discovery UX | map-based station browsing |
| MVP-2 | Operations | admin + partner control |
| MVP-3 | Identity | Keycloak + RBAC |
| MVP-4 | Analytics | intelligence layer |
| MVP-5 | Performance | scaling + optimization |
| MVP-6 | Production | infra hardening |

### Global Execution Flow (per MVP)

```
Design → Backend → Frontend → Integration → Testing → Stabilization
```

---

## MVP-1: UX Discovery

**Goal:** Deliver high-quality map-based EV station discovery.

### Backend (Driver Service)
- Setup Rust Actix service
- Implement `GET /api/v1/stations`
- Implement `GET /api/v1/stations/nearby`
- Implement `GET /api/v1/stations/{id}`
- PostGIS integration (radius query)
- Seed stations data

### Database
- platform_db init
- inventory schema creation
- stations + chargers tables
- PostGIS extension enable
- Indexes for geospatial queries

### Clickstream Service
- `POST /api/v1/events`
- `POST /api/v1/events/batch`
- raw_events insert only
- payload validation

### Frontend (Driver Mobile App)
- Expo SDK 54 setup
- Map screen (react-native-maps)
- Station markers rendering
- Bottom sheet station detail
- Nearby search flow
- Optimistic UI interactions

### Integration
- Driver app → driver-service
- Driver app → clickstream-service
- Map radius query wired to backend

### Stabilization Sprint (MANDATORY)
- Fix map jitter issues
- Optimize PostGIS query latency
- Reduce API response payload
- UX polishing (loading states, skeletons)
- Event consistency validation

---

## MVP-2: Admin + Dashboard

**Goal:** Enable partner + admin management system.

### Backend (Admin Service)
- `POST/GET /api/v1/partners`
- `POST/PATCH/DELETE /api/v1/stations`
- `POST /api/v1/stations/{id}/chargers`
- Partner scoping enforcement
- CRUD logic for inventory

### Frontend (Dashboard)
- Partner management UI
- Station CRUD interface
- Charger management UI

### Database
- Enforce inventory constraints
- Add audit fields usage
- Partner lifecycle states

### Stabilization Sprint
- RBAC enforcement checks
- UI validation improvements
- API consistency fixes

---

## MVP-3: Identity + RBAC

**Goal:** Introduce Keycloak + dual realm system.

### Backend
- Integrate Keycloak JWT validation
- Implement auth-gateway layer
- `POST /api/v1/auth/login`
- `POST /api/v1/auth/social`
- `GET /api/v1/auth/me`

### Identity Model
- bm-drivers realm
- bm-control realm
- Role mapping: public_driver, registered_driver, partner, admin

### Frontend
- Login flow
- Session management
- Role-based UI rendering

### Stabilization Sprint
- Token expiration handling
- Refresh token logic
- RBAC correctness validation

---

## MVP-4: Analytics

**Goal:** Turn raw events into insight layer.

### Backend
- Enrich clickstream validation
- Event taxonomy enforcement
- Introduce aggregation jobs

### Database
- event_aggregates
- station_analytics
- Partition raw_events

### Frontend
- Basic analytics dashboard (admin)

### Stabilization Sprint
- Event consistency audits
- Performance tuning for ingestion
- Aggregation correctness

---

## MVP-5: Performance

**Goal:** Optimize system for scale.

### Tasks
- PostGIS query optimization
- Caching layer (stations nearby)
- Pagination enforcement everywhere
- DB indexing review
- Load testing

### Stabilization Sprint
- Latency benchmarking
- Memory profiling
- API response optimization

---

## MVP-6: Production Infrastructure

**Goal:** Production-grade deployment with Traefik.

### Infrastructure
- Traefik reverse proxy
- HTTPS (Let's Encrypt)
- Dockerized services
- Internal network isolation

### Services
- driver-service
- admin-service
- clickstream-service
- Keycloak (internal only)
- Postgres cluster

### Stabilization Sprint
- Rollback testing
- Failover validation
- Security audit
- Infra load test

---

## Cross-MVP Rules

### No Early Implementation
No feature may be implemented before its MVP stage.

### Vertical Slice Rule
Each MVP must include: backend, frontend, DB, integration, stabilization.

### Source Rule
All runtime code MUST live in /source.

### API Rule
All endpoints MUST follow /api/v1/*.

---

## Execution Order (Realistic)

1. platform_db schema + PostGIS
2. driver-service (MVP-1 APIs)
3. clickstream-service
4. mobile-driver app map UI
5. integration (UX working end-to-end)
6. stabilization sprint
7. admin-service (MVP-2)
8. dashboard UI
9. Keycloak integration (MVP-3)
10. analytics layer (MVP-4)
11. performance tuning (MVP-5)
12. production infra (MVP-6)

---

## One-Line Master Plan

```
MVP-1 UX → MVP-2 operations → MVP-3 identity → MVP-4 analytics → MVP-5 scale → MVP-6 production
```
