# MVP-1: Station Discovery

## Version: 1.0
## Status: Active
## Timeline: 6-8 weeks
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🎯 OBJECTIVE

Deliver a working map-based EV station discovery system.

Users must be able to:

- Open map
- See stations
- Find nearby stations
- View station details

**No authentication, no admin, no dashboards.**

---

## 🧠 EXECUTION PRINCIPLE

**Build only what is specified. Nothing more. Nothing ahead of MVP.**

---

## 📦 SYSTEM SCOPE

### Allowed Code Areas

**Frontend:**
- source/front/apps/mobile-driver
- source/front/apps/web-driver
- source/front/packages/*

**Backend:**
- source/services/driver-service (Rust)

### Forbidden

- auth-service
- admin-service
- dashboard
- new services
- unplanned APIs
- direct DB access from frontend

---

## 🔌 API CONTRACT (MVP-1 LOCKED)

**All endpoints MUST follow /api/v1/* pattern:**

- GET /api/v1/stations
- GET /api/v1/stations/nearby
- GET /api/v1/stations/{id}

**No new endpoints allowed.**

---

## 🧱 EXECUTION PHASES

### PHASE 1 — BACKEND FOUNDATION

**Goal:** Expose station data API

**Tasks:**
- Implement driver-service (Rust)
- Connect to platform_db
- Implement endpoints:
  - GET /stations
  - GET /stations/nearby
  - GET /stations/{id}

**Output:**
- JSON station model
- Working PostGIS query for nearby search

---

### PHASE 2 — FRONTEND CORE SETUP

**Goal:** Initialize apps + shared packages

**Tasks:**
- Setup monorepo structure
- Ensure:
  - @bm/types
  - @bm/api-client
  - @bm/utils
  - @bm/design-tokens
- Setup React Query
- Setup MapContainer abstraction

---

### PHASE 3 — MAP SYSTEM (CRITICAL)

**Goal:** Render map correctly on both platforms

**Tasks:**
- mobile-driver → react-native-maps
- web-driver → Leaflet
- Implement:
  - MapContainer.native.ts
  - MapContainer.web.ts

**Rules:**
- No map library usage outside MapContainer
- No duplication of map logic
- No platform checks in UI components

---

### PHASE 4 — STATION DATA INTEGRATION

**Goal:** Connect frontend to backend

**Tasks:**
- Create @bm/api-client calls
- Implement React Query hooks:
  - useStations()
  - useNearbyStations()
- Display stations on map

---

### PHASE 5 — MARKER SYSTEM

**Goal:** Show stations visually

**Tasks:**
- Render station markers
- Add clustering (if needed)
- Optimize rendering (memoization)

---

### PHASE 6 — NEARBY SEARCH

**Goal:** Dynamic station filtering by location

**Tasks:**
- Use GPS (mobile) / geolocation (web)
- Call:
  - /stations/nearby?lat&lng&radius
- Update map in real time

---

### PHASE 7 — STATION DETAIL VIEW

**Goal:** User can inspect a station

**Tasks:**
- Bottom sheet (mobile)
- Side panel (web)
- Show:
  - name
  - location
  - chargers
  - status

---

### PHASE 8 — UX POLISH (PRO MAX RULE)

**Goal:** Make experience production-grade

**Tasks:**
- Skeleton loading states
- Empty states:
  - no stations nearby
- Error states:
  - retry button
- Haptics (mobile)
- Smooth transitions

---

### PHASE 9 — ANALYTICS EVENTS (MINIMAL)

**Goal:** Track basic usage

**Events:**
- MapViewed
- StationOpened
- NearbySearchExecuted

---

## 🧪 VALIDATION CHECKPOINTS

**After each phase, must verify:**

- [ ] No new services introduced
- [ ] API still /api/v1/*
- [ ] Map abstraction respected
- [ ] No direct fetch() in apps
- [ ] No architecture changes

---

## 🚫 STOP CONDITIONS

**Execution must stop if:**

- LLM invents new endpoints
- Frontend bypasses api-client
- New service is added
- MVP scope expands
- Map logic leaks outside MapContainer

---

## 📊 DEFINITION OF DONE (MVP-1)

**MVP-1 is complete when:**

- [ ] Map loads in both apps
- [ ] Stations render correctly
- [ ] Nearby search works
- [ ] Station detail view works
- [ ] Basic analytics events fire
- [ ] No architecture violations

---

## 🧠 EXECUTION FLOW (LLM RULE)

**OpenCode MUST follow:**

1. Read MVP scope
2. Confirm API contract
3. Implement backend (Rust)
4. Implement frontend
5. Connect API
6. Add UX polish
7. Validate constraints
8. Log changes

---

## 🔌 API CONTRACT DETAILS

### GET /api/v1/stations

**Query Parameters:**
- `limit` (number, default: 20)
- `offset` (number, default: 0)
- `status` (string, optional)

**Response:**
```json
{
  "id": "STA-001",
  "name": "Station A",
  "lat": 36.8,
  "lng": 10.2,
  "status": "available"
}
```

### GET /api/v1/stations/nearby

**Query Parameters:**
- `latitude` (number, required)
- `longitude` (number, required)
- `radius` (number, default: 1000)

**Response:**
```json
{
  "id": "STA-002",
  "name": "Station B",
  "distance": 120
}
```

### GET /api/v1/stations/{id}

**Response:**
```json
{
  "id": "STA-001",
  "name": "Station A",
  "location": {
    "lat": 36.8,
    "lng": 10.2
  },
  "status": "available",
  "chargers": [...]
}
```

---

## 🎯 SYSTEM ARCHITECTURE

### Backend Stack (Rust)

**Service:** driver-service (Rust)
**Port:** 3000
**Database:** platform_db (PostgreSQL + PostGIS)

### Frontend Stack

**Apps:**
- mobile-driver (Expo)
- web-driver (React + Leaflet)

**Packages:**
- @bm/types
- @bm/api-client
- @bm/utils
- @bm/design-tokens

---

## 🛡️ QUALITY REQUIREMENTS

**Every feature must include:**
- [ ] loading state
- [ ] empty state
- [ ] error state
- [ ] retry mechanism
- [ ] mobile-first UX behavior

---

## 📊 SUCCESS METRICS

### Functional
- [ ] Stations load correctly
- [ ] Nearby search works
- [ ] Station details display
- [ ] Map interactions smooth

### Performance
- [ ] Map rendering < 500ms
- [ ] Nearby search < 2 seconds
- [ ] No memory leaks

### Quality
- [ ] All tests passing
- [ ] Error states implemented
- [ ] Empty states defined
- [ ] Loading states present

---

## 🚫 HARD STOP RULES

**Execution FAILS if:**

- auth is introduced
- dashboard is touched
- new endpoints added
- architecture changes
- map logic leaks outside MapContainer

---

## 🎯 MVP-1 SCOPE DEFINITION

**INCLUDES:**
- Map view (mobile + web)
- Station markers
- Nearby search (PostGIS)
- Station detail view
- Basic analytics events

**EXCLUDES:**
- Authentication
- Admin features
- Partner flows
- Future MVP features

---

## 🧠 CORE PRINCIPLE

**MVP-1 is not a feature set. It is a single vertical slice of reality.**

**If you want next step, I can generate:**

- 🧠 SpecKit for station discovery (ready for OpenCode)
- 📱 frontend MapContainer full implementation
- 🔌 @bm/api-client architecture
- 🧪 test strategy for MVP-1 (map flow E2E)

**Just tell me.**

---

*This MVP establishes the foundation for station discovery, providing essential functionality for the core use case.*
