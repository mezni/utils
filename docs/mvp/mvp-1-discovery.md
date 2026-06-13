# BorneMap — FULL PROJECT EXECUTION PLAN

## Version: 1.0 (Reset Architecture)

---

## 🧠 0. EXECUTION MODEL

This project is executed as strict MVP increments:

- **MVP-1** → Discovery Core (Map + PostGIS)
- **MVP-2** → Admin + Partner Ops
- **MVP-3** → Identity (Keycloak + Auth-service)
- **MVP-4** → Analytics Intelligence
- **MVP-5** → Hardening + Performance
- **MVP-6** → Production Infra (Traefik full routing)

---

## 🧱 1. SYSTEM ARCHITECTURE (FINAL)

Reorganized structure:

```
bornemap/
├── source/
│   ├── front/
│   │   ├── apps/
│   │   │   ├── mobile-driver
│   │   │   ├── web-driver
│   │   │   └── dashboard
│   │   └── packages/
│   │       ├── @bm/types
│   │       ├── @bm/api-client
│   │       ├── @bm/utils
│   │       └── @bm/design-tokens
│   │
│   └── services/
│       ├── driver-service (MVP-1)
│       ├── admin-service (MVP-2)
│       └── auth-service (MVP-3)
│
├── infra/
│   ├── docker-compose.yml
│   ├── migrations/
│   ├── osm-import/
│   └── traefik/
│
├── docs/
└── scripts/
```

---

## 🌐 2. INFRASTRUCTURE PHASE (MVP FOUNDATION)

### 2.1 Docker Stack

- PostgreSQL + PostGIS (platform_db)
- PostgreSQL (analytics_db)
- Keycloak (later MVP-3)
- Traefik (MVP-6)

### 2.2 DB Design

**platform_db:**
- `inventory` (stations, chargers, partners)
- `gis` (read-only OSM-derived)
- `users` (MVP-3)

**analytics_db:**
- `raw_events` (append-only)

### 2.3 OSM IMPORT (CRITICAL STEP)

Geofabrik Tunisia extract
→ filter charging stations
→ transform script
→ PostGIS seed

**Result:**
- 50–300 real stations in Tunisia

---

## 🎯 3. MVP-1 — DISCOVERY CORE (CURRENT FOCUS)

### Goal

Fully working EV map discovery system using real PostGIS data

### 3.1 Backend (driver-service)

**Responsibilities:**
- PostGIS queries
- station APIs
- nearby search engine

**Endpoints:**
- `GET /api/v1/stations`
- `GET /api/v1/stations/{id}`
- `GET /api/v1/stations/nearby`

### 3.2 Data Layer

- PostGIS GEOGRAPHY indexing
- OSM seeded stations
- distance-based ordering

### 3.3 API Rules

- No auth
- No analytics
- No external calls

### 3.4 Done When

- [ ] Nearby query is accurate
- [ ] Real OSM data visible
- [ ] Latency < 200ms local

---

## 📱 4. FRONTEND (MVP-1)

### Structure

```
source/front/
```

**Apps:**
- `mobile-driver` (Expo 54)
- `web-driver` (React + Leaflet)

**Core System:**
- **Map layer:**
  - MapContainer abstraction
  - React Query for API
  - Zustand for UI state

**Features:**
- Map renders stations
- Live nearby updates
- Station bottom sheet

**Rule:**

❌ **NO fetch outside api-client**

---

## 🔌 5. API LAYER

### @bm/api-client

**Responsibilities:**
- All HTTP calls
- Typed responses
- Single source of truth

---

## 🧪 6. CI / VALIDATION SYSTEM

Enforces:
- No cross-MVP leakage
- No forbidden architecture
- API contract integrity
- Frontend rules (no direct fetch)

---

## 🔐 7. AUTH SYSTEM (MVP-3)

- `auth-service` introduced later
- Keycloak isolated
- No direct DB access

---

## 📊 8. ANALYTICS (MVP-4)

- `clickstream-service`
- Append-only DB
- No UI dependency in MVP-1

---

## 🌐 9. TRAEFIK (MVP-6)

Final production gateway:
- Routing
- TLS
- Domain separation

---

## 🧠 10. EXECUTION PHASE ORDER (CRITICAL)

1. **PHASE 0** → Infra + DB + PostGIS + OSM seed
2. **PHASE 1** → driver-service (Rust + PostGIS)
3. **PHASE 2** → API client
4. **PHASE 3** → frontend map system
5. **PHASE 4** → nearby search UX
6. **PHASE 5** → station details UX
7. **PHASE 6** → CI enforcement
8. **PHASE 7** → MVP freeze

---

## 🚫 11. NON-NEGOTIABLE RULES

- `source/front` = all frontend code
- `source/services` = all backend services
- PostGIS = source of truth for geospatial data
- OSM = only ingestion source
- No cross-MVP feature bleed
- No API gateway until MVP-6

---

## 🧠 12. CORE PRINCIPLE

**The system evolves through controlled MVP slices, not feature accumulation.**

---

*This execution plan defines the complete evolution of BorneMap across 6 MVP phases, with strict architectural boundaries and MVP isolation.*
