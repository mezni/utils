# BorneMap — MVP-1: DISCOVERY CORE

## Version: 2.0 (Sprint-Based Execution)

---

## 🎯 OBJECTIVE

Deliver a fully working geospatial EV station discovery product using real OSM data and PostGIS.

**Users can:**
- Open map (web + mobile)
- See real EV charging stations
- Move map → fetch nearby stations (PostGIS)
- Tap station → view details

---

## 🧱 1. SYSTEM SCOPE

### Technology Stack

**Backend:**
- driver-service (Rust + Actix)
- PostGIS (platform_db)

**Frontend:**
- mobile-driver (Expo 54)
- web-driver (React + Leaflet)
- @bm/api-client (shared)

**Infrastructure:**
- docker-compose only
- migrations only

### Forbidden

- auth-service
- admin-service
- analytics
- dashboard UI
- any new services
- direct DB access from frontend

---

## 🗄️ 2. DATA FOUNDATION (CRITICAL)

### 2.1 Source of Truth

`platform_db.inventory.station`

### 2.2 Data Origin

**OSM Tunisia Import:**
- Geofabrik extract
- filtered charging stations
- converted into PostGIS format

### 2.3 Schema Requirements

Each station must have:

- `id` (STA-xxx or OSM-derived)
- `name`
- `status` (active / maintenance)
- `latitude`
- `longitude`
- `location` (GEOGRAPHY POINT)

---

## ⚙️ 3. BACKEND (driver-service)

### Role

Pure geospatial query engine

### Endpoints

1. **Get all stations**
   ```
   GET /api/v1/stations
   ```

2. **Get station by ID**
   ```
   GET /api/v1/stations/{id}
   ```

3. **Nearby query (CORE FEATURE)**
   ```
   GET /api/v1/stations/nearby?lat&lng&radius
   ```

### PostGIS Behavior

- Uses `ST_DWithin`
- Orders by distance (`<->`)
- Filters `status = active`

### Performance Requirement

- `< 200ms` local response
- Indexed geography column required

---

## 🔌 4. API CLIENT (@bm/api-client)

### Rule

**ALL frontend traffic goes through this layer.**

### Functions

- `getStations()`
- `getStationById(id)`
- `getNearbyStations(lat, lng, radius)`

### Constraints

- No `fetch` outside this package
- Fully typed responses
- Shared with both apps

---

## 📱 5. FRONTEND SYSTEM

### Apps

- `mobile-driver` (Expo 54)
- `web-driver` (React + Leaflet)

### Architecture

```
MapContainer → React Query → API Client → driver-service
```

### State Management

- **Zustand:** selected station, map center
- **React Query:** stations data

---

## 🗺️ 6. MAP SYSTEM (CORE ENGINE)

### Abstract Layer

- `MapContainer.web.ts`
- `MapContainer.native.ts`

### Responsibilities

- Render map
- Render markers
- Handle movement
- Emit center changes

### Rules

- ❌ NO map logic outside this layer
- ❌ NO platform logic in UI components

---

## 📍 7. NEARBY SYSTEM (CORE LOOP)

### Flow

```
user moves map
→ update center
→ debounce (300–500ms)
→ API call
→ PostGIS query
→ markers update
```

### Requirements

- Smooth marker update
- No flicker
- No full map rerender
- Cached results via React Query

---

## 🧾 8. STATION DETAILS

### Trigger

Marker click

### UI

- Mobile → bottom sheet
- Web → side panel

### Data

```
GET /api/v1/stations/{id}
```

### Must Show

- name
- status
- location
- connectors (future-ready field)
- distance (computed)

---

## 🎨 9. UX REQUIREMENTS (PRO MAX RULE)

### Mandatory

- Skeleton loading (NOT spinner)
- Smooth transitions
- No blank screens
- Haptic feedback (mobile)
- Empty states designed

### Empty States

- No stations in area
- GPS unavailable
- Network error

---

## ⚡ 10. PERFORMANCE RULES

### Must

- Debounce map movement
- Cache API responses
- Memoize markers
- Avoid rerenders on pan/zoom

### Targets

- Map interaction latency < 100ms perceived
- API response < 200ms local

---

## 🧪 11. MVP-1 VALIDATION CHECKLIST

### Functional

- [ ] Map loads
- [ ] Stations render from DB
- [ ] Nearby updates work
- [ ] Station detail opens

### Architecture

- [ ] No `fetch` outside api-client
- [ ] No DB access outside driver-service
- [ ] MapContainer respected

### Data

- [ ] OSM stations visible
- [ ] PostGIS query correct
- [ ] Indexing enabled

---

## 🚫 12. HARD CONSTRAINTS

- ❌ NO auth
- ❌ NO admin system
- ❌ NO analytics
- ❌ NO new services
- ❌ NO backend expansion beyond driver-service
- ❌ NO MVP-2 logic leakage

---

## 🧠 13. CORE PRINCIPLE

**MVP-1 is a geospatial query product, not a frontend project.**

---

## 🧭 FINAL SYSTEM VIEW

```
OSM Tunisia
   ↓
PostGIS (platform_db)
   ↓
driver-service (Rust)
   ↓
@bm/api-client
   ↓
React Query
   ↓
Map UI (web + mobile)
```

---

---

# 🚀 MVP-1 SPRINT PLAN — DISCOVERY CORE

## Version: 1.0

**Total Duration:** 4–6 sprints (recommended)

---

## 🧠 OVERALL STRATEGY

MVP-1 is built as a vertical slice, not layered development.

Each sprint must:
- Produce runnable software
- Not wait for later sprints
- Avoid speculative features

---

## 🧱 SPRINT 0 — INFRA & DATA FOUNDATION

### 🎯 Goal

Make the system "real" (DB + OSM + PostGIS alive)

### Tasks

**Database:**
- [ ] Setup platform_db (PostGIS enabled)
- [ ] Setup analytics_db (empty, reserved)
- [ ] Create schemas: `inventory`, `gis` (read-only concept)

**OSM Import:**
- [ ] Download Tunisia OSM extract
- [ ] Filter `amenity=charging_station`
- [ ] Convert → PostGIS insert format
- [ ] Seed 50–300 stations

**Validation:**
- [ ] Run SQL queries manually
- [ ] Validate geospatial indexing
- [ ] Ensure nearby queries work

### Definition of Done

- [x] DB running in Docker
- [x] PostGIS enabled
- [x] ≥50 real stations inserted
- [x] `ST_DWithin` query works

---

## 🧱 SPRINT 1 — BACKEND CORE (driver-service)

### 🎯 Goal

Expose geospatial API

### Tasks

**Rust Service:**
- [ ] Actix setup
- [ ] SQLx connection
- [ ] repository/service/handler layering

**Endpoints:**
- [ ] `GET /api/v1/stations`
- [ ] `GET /api/v1/stations/{id}`
- [ ] `GET /api/v1/stations/nearby`

**PostGIS Logic:**
- [ ] Distance calculation
- [ ] Ordering by proximity
- [ ] Active-only filtering

### Definition of Done

- [x] API returns real DB data
- [x] Nearby query works correctly
- [x] Latency acceptable (<200ms local)

---

## 🔌 SPRINT 2 — API CLIENT LAYER

### 🎯 Goal

Single source of truth for frontend API

### Tasks

**Create @bm/api-client:**
- [ ] `getStations()`
- [ ] `getStationById()`
- [ ] `getNearbyStations()`

**Rules:**
- [ ] NO `fetch` outside this package
- [ ] Fully typed responses
- [ ] Shared between web + mobile

### Definition of Done

- [x] Frontend can query backend via single layer
- [x] Types are shared and consistent

---

## 📱 SPRINT 3 — FRONTEND FOUNDATION

### 🎯 Goal

Both apps boot with map shell

### Tasks

**Apps:**
- [ ] mobile-driver (Expo 54)
- [ ] web-driver (Leaflet)

**Core Setup:**
- [ ] React Query
- [ ] Zustand store
- [ ] MapContainer abstraction
- [ ] Design tokens wired

### Definition of Done

- [x] Both apps run
- [x] Map renders empty state
- [x] No API logic yet

---

## 🗺️ SPRINT 4 — MAP + MARKERS SYSTEM

### 🎯 Goal

Display real stations on map

### Tasks

**Map Engine:**
- [ ] MapContainer.web/native
- [ ] Marker rendering system

**Data Flow:**
- [ ] React Query → nearby API
- [ ] Render markers
- [ ] Update on map move

### Definition of Done

- [x] Stations visible on map
- [x] Movement triggers refresh
- [x] No performance issues

---

## 📍 SPRINT 5 — NEARBY SEARCH CORE LOOP

### 🎯 Goal

Real-time geospatial interaction

### Tasks

**Map Movement:**
- [ ] Map movement listener
- [ ] Debounce (300–500ms)
- [ ] Query re-trigger
- [ ] Marker diff update

**UX Rules:**
- [ ] No flicker
- [ ] No full rerender
- [ ] Cached responses

### Definition of Done

- [x] Smooth dynamic updates
- [x] PostGIS queries stable
- [x] UX feels "live"

---

## 🧾 SPRINT 6 — STATION DETAILS UX

### 🎯 Goal

Complete discovery loop

### Tasks

**UI:**
- [ ] Station click handler
- [ ] Bottom sheet (mobile)
- [ ] Side panel (web)
- [ ] Detail API integration

**Data Shown:**
- [ ] name
- [ ] status
- [ ] location
- [ ] connectors (future-ready)

### Definition of Done

- [x] Full click → detail flow works
- [x] Smooth transitions
- [x] No UI lag

---

## 🧪 SPRINT 7 — STABILIZATION + CI ENFORCEMENT

### 🎯 Goal

Lock architecture

### Tasks

**CI Rules:**
- [ ] Fetch guard
- [ ] Map guard
- [ ] DB guard

**Testing:**
- [ ] Contract tests
- [ ] Basic E2E map test
- [ ] Performance checks

### Definition of Done

- [x] Architecture violations blocked
- [x] MVP-1 is stable
- [x] No regression possible without CI failure

---

## 🚫 HARD RULES ACROSS ALL SPRINTS

- ❌ NO auth system
- ❌ NO admin/dashboard work
- ❌ NO analytics
- ❌ NO new services beyond driver-service
- ❌ NO bypassing API client
- ❌ NO direct DB access outside backend

---

## 🧠 EXECUTION MODEL

Each sprint must produce:

```
design → implement → validate → lock → CI protect
```

**No sprint is "incomplete but usable".**

---

## 🧭 FINAL MVP-1 OUTPUT

At the end:

- ✅ Real OSM-powered EV map
- ✅ PostGIS-driven nearby search
- ✅ Clean frontend (web + mobile)
- ✅ Fully typed API layer
- ✅ CI-enforced architecture

---

*MVP-1 is complete when all 8 sprints pass and CI enforcement is active.*
