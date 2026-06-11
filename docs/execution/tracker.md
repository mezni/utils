# Execution Tracker

**Last Updated:** 2026-06-10
**Current Phase:** MVP-1 — Discovery Core

---

## Status Key

| Icon | Meaning |
|---|---|
| 🔴 | Pending |
| 🟡 | In Progress |
| 🟢 | Done |
| ⚠️ | Blocked |
| 🔵 | Stabilization |

---

## MVP-1: Discovery Core

### Sprint 0 — Infrastructure Bootstrap

| ID | Task | Status | Notes |
|---|---|---|---|
| INF-1 | Initialize monorepo structure (/source, /infra) | 🔴 | |
| INF-2 | Docker Compose setup (Postgres + PostGIS) | 🔴 | |
| INF-3 | PostGIS enablement + verify ST_DWithin | 🔴 | |
| INF-4 | Healthcheck endpoints (/health) | 🔴 | |

### Sprint 1 — Data Layer

| ID | Task | Status | Notes |
|---|---|---|---|
| DB-1 | Create inventory schema (partner, station, charger) | 🔴 | |
| DB-2 | Indexing strategy (lat/lng, partner_id, station_id) | 🔴 | |
| DB-3 | PostGIS integration (geometry, radius queries) | 🔴 | |
| DB-4 | Seed dataset (10 stations, 3 partners, 30 chargers) | 🔴 | |

### Sprint 2 — Driver Service

| ID | Task | Status | Notes |
|---|---|---|---|
| API-1 | Initialize Actix-web service (/api/v1 base) | 🔴 | |
| API-2 | GET /api/v1/stations | 🔴 | |
| API-3 | GET /api/v1/stations/{id} | 🔴 | |
| API-4 | GET /api/v1/stations/nearby (ST_DWithin) | 🔴 | |
| API-5 | Response normalization (DTO layer) | 🔴 | |

### Sprint 3 — Clickstream Service

| ID | Task | Status | Notes |
|---|---|---|---|
| EVT-1 | Initialize Actix-web service (/api/v1/events) | 🔴 | |
| EVT-2 | raw_events insert (JSONB) | 🔴 | |
| EVT-3 | Event taxonomy enforcement (MVP-1 only) | 🔴 | |
| EVT-4 | Async ingestion (fire-and-forget) | 🔴 | |

### Sprint 4 — Design System (Critical Path)

| ID | Task | Status | Notes |
|---|---|---|---|
| DS-1 | tokens.ts (colors, spacing, typography, shadows) | 🔴 | |
| DS-2 | Skeleton component (map + list) | 🔴 | |
| DS-3 | Empty state component | 🔴 | |
| DS-4 | Error state component (retry CTA) | 🔴 | |
| DS-5 | Button system (CTA + haptics) | 🔴 | |
| DS-6 | Bottom sheet base (Reanimated v3) | 🔴 | |

### Sprint 5 — Mobile Driver App

| ID | Task | Status | Notes |
|---|---|---|---|
| APP-1 | Expo SDK 54 setup | 🔴 | |
| APP-2 | Map screen (react-native-maps, markers) | 🔴 | |
| APP-3 | Nearby fetch integration (dynamic markers) | 🔴 | |
| APP-4 | Station bottom sheet (details on tap) | 🔴 | |
| APP-5 | Skeleton loading states | 🔴 | |
| APP-6 | Event tracking integration | 🔴 | |

### Sprint 6 — Integration

| ID | Task | Status | Notes |
|---|---|---|---|
| INT-1 | API wiring validation (app → services) | 🔴 | |
| INT-2 | Event correctness check | 🔴 | |
| INT-3 | End-to-end UX flow | 🔴 | |
| INT-4 | Payload optimization | 🔴 | |

### Sprint 7 — Stabilization

| ID | Task | Status | Notes |
|---|---|---|---|
| STAB-1 | PostGIS performance tuning (< 200ms) | 🔴 | |
| STAB-2 | API contract tests | 🔴 | |
| STAB-3 | UX audit (skeletons, empty/error states) | 🔴 | |
| STAB-4 | Map performance (jitter fix, 60fps) | 🔴 | |
| STAB-5 | Event pipeline validation | 🔴 | |

---

## MVP-2: Operations

| ID | Task | Status | Notes |
|---|---|---|---|
| MVP-2 | admin-service | 🔴 | |
| MVP-2 | dashboard UI | 🔴 | |

---

## MVP-3: Identity

| ID | Task | Status | Notes |
|---|---|---|---|
| MVP-3 | Keycloak integration | 🔴 | |
| MVP-3 | auth-gateway | 🔴 | |

---

## MVP-4: Analytics

| ID | Task | Status | Notes |
|---|---|---|---|
| MVP-4 | analytics layer | 🔴 | |

---

## MVP-5: Performance

| ID | Task | Status | Notes |
|---|---|---|---|
| MVP-5 | performance tuning | 🔴 | |

---

## MVP-6: Production

| ID | Task | Status | Notes |
|---|---|---|---|
| MVP-6 | production infra | 🔴 | |
