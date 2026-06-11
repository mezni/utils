# MVP-1: Discovery Core

**Objective:** Deliver a high-performance, map-first EV station discovery experience for drivers in Tunisia.

```
User opens app → sees map → finds nearby stations → views station details → interactions tracked
```

**Core principle:** MVP-1 is ONLY discovery. No auth, no admin, no RBAC.

---

## Out of Scope (Strict)

- Authentication (Keycloak)
- User accounts
- Favorites persistence
- Partner/admin dashboard
- Payments
- Charging session management
- RBAC enforcement
- Analytics dashboards (only raw events allowed)

---

## Dependency Stack

MVP-1 requires completion in this order:

### Infrastructure
- Docker Compose running
- Postgres + PostGIS alive

### Data Layer
- platform_db initialized
- inventory schema migrated
- Seed stations inserted

### Backend
- Driver Service running
- Clickstream Service running

### Design System (NEW HARD DEPENDENCY)
- tokens.ts
- Map UI primitives
- Skeleton / Empty / Error components

---

## Data Requirements

### platform_db (MVP-1 subset)

| Schema | Tables | Status |
|---|---|---|
| inventory | partner (seeded only) | Active |
| inventory | station | Active |
| inventory | charger | Optional but recommended |
| gis | OSM | Optional (not required for MVP-1) |

### Seed Dataset (Minimum Viable UX)

- 10 stations
- 3 partners
- 30 chargers total
- Tunis + surrounding area only

---

## Backend

### Driver Service (CORE) — 8080

**Endpoints:**

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/stations` | List stations (lightweight payload) |
| GET | `/api/v1/stations/nearby` | PostGIS radius query (lat, lng, radius_m) |
| GET | `/api/v1/stations/{id}` | Station detail with charger list |

**Core logic:**
- Nearby query via PostGIS `ST_DWithin`
- Indexed lat/lng
- Target: < 200ms response time
- No nested heavy joins
- No user-specific logic

### Clickstream Service — 8082

**Endpoints:**

| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/events` | Single event ingestion |
| POST | `/api/v1/events/batch` | Batch event ingestion |

**Events (MVP-1 only):**
- `map_open`
- `station_view`
- `station_click`
- `nearby_search`
- `map_pan`
- `map_zoom`

**Rule:** Must never block UX (fire-and-forget)

---

## Design System (Critical Path)

Required before any frontend screen work.

### tokens.ts

- Colors (light/dark)
- Spacing scale
- Typography scale
- Radii
- Shadows

### UI Primitives

- Button (CTA + haptics)
- Skeleton loader (map + list)
- Empty state
- Error state
- Bottom sheet base
- Map marker component

### UX Rules

- Skeleton-first loading
- Optimistic UI interactions
- Reanimated v3 only
- Dark mode mandatory
- No hardcoded design tokens

---

## Frontend

### Driver Mobile App (PRIMARY PRODUCT)

**Screens:**

1. **Map Screen** (core)
   - Full-screen map
   - Station markers
   - Clustering (optional)
   - Live nearby fetch

2. **Station Bottom Sheet**
   - Name
   - Distance
   - Chargers list
   - Status (available/occupied/offline)

3. **Loading States**
   - Skeleton map
   - Skeleton bottom sheet

### Driver Web App (optional parity)

- Leaflet map
- Simplified UI
- Same API

---

## Integration Flow (Critical UX Loop)

```
App Open
  ↓
Fetch /stations/nearby
  ↓
Render Map
  ↓
User taps station
  ↓
Fetch /stations/{id}
  ↓
Bottom sheet opens
  ↓
Clickstream event sent
```

---

## Clickstream Pipeline

```
Frontend → Clickstream Service → analytics_db.raw_events
```

**Rules:**
- No transformation logic
- No aggregation
- Only raw ingestion
- Fire-and-forget

---

## Stabilization Sprint (Mandatory)

No MVP-2 starts until this clears.

### Performance
- PostGIS query < 200ms
- Map render smooth (no jitter)
- Marker rendering optimized

### UX
- Skeleton consistency check
- Empty states verified
- Error states handled gracefully

### API
- Contract tests for all endpoints
- Payload size audit

### Data
- Seed dataset validation
- Station correctness check

---

## Success Criteria

### Functional
- Map loads successfully
- Stations appear correctly
- Station details open correctly

### Performance
- Nearby search < 200ms
- Smooth map interaction

### UX
- No blank states
- Full skeleton coverage
- No layout shift

### Observability
- All interactions generate events
- Clickstream ingestion stable

---

## Sprint Breakdown

### Sprint 0 — Infrastructure Bootstrap (Blocking)

| ID | Task | Exit Criteria |
|---|---|---|
| INF-1 | Initialize monorepo structure (/source, /infra) | Directory structure created |
| INF-2 | Docker Compose setup (Postgres + PostGIS + analytics_db) | `docker compose up` boots full stack |
| INF-3 | PostGIS enablement + verify ST_DWithin | Spatial query works |
| INF-4 | Healthcheck endpoints (/health) | Services respond |

### Sprint 1 — Data Layer (platform_db)

| ID | Task | Exit Criteria |
|---|---|---|
| DB-1 | Create inventory schema (partner, station, charger, connector_type, charger_status) | Tables created |
| DB-2 | Indexing strategy (lat/lng, partner_id, station_id) | Indexes in place |
| DB-3 | PostGIS integration (geometry column, radius queries) | Spatial queries work |
| DB-4 | Seed dataset (10 stations, 3 partners, 30 chargers, Tunisia) | Data queryable |

### Sprint 2 — Driver Service (Core Backend)

| ID | Task | Exit Criteria |
|---|---|---|
| API-1 | Initialize Actix-web service with /api/v1 base router | Service boots |
| API-2 | `GET /api/v1/stations` (lightweight payload) | Returns stations |
| API-3 | `GET /api/v1/stations/{id}` (charger list included) | Returns detail |
| API-4 | `GET /api/v1/stations/nearby` (PostGIS ST_DWithin) | Correct results, < 200ms |
| API-5 | Response normalization layer (DTO mapping, consistent JSON) | No DB leakage |

### Sprint 3 — Clickstream Service

| ID | Task | Exit Criteria |
|---|---|---|
| EVT-1 | Initialize Actix-web service with `/api/v1/events` + `/api/v1/events/batch` | Service boots |
| EVT-2 | raw_events insert (JSONB payload, no validation beyond schema) | Events stored |
| EVT-3 | Event taxonomy enforcement (MVP-1 events only) | Invalid events rejected |
| EVT-4 | Async ingestion (fire-and-forget, non-blocking) | No UX latency impact |

### Sprint 4 — Design System (Critical Path Blocker)

| ID | Task | Exit Criteria |
|---|---|---|
| DS-1 | tokens.ts (colors light/dark, spacing, typography, shadows, radii) | Zero hardcoded styles |
| DS-2 | Skeleton component (map + list) | Skeleton renders |
| DS-3 | Empty state component (no stations, GPS unavailable) | Empty state renders |
| DS-4 | Error state component (retry CTA, network error) | Error state renders |
| DS-5 | Button system (primary CTA, haptics integration) | Button fires haptics |
| DS-6 | Bottom sheet base (Reanimated v3, reusable) | Sheet animates |

### Sprint 5 — Mobile Driver App (Core UX)

| ID | Task | Exit Criteria |
|---|---|---|
| APP-1 | Expo SDK 54 setup (project init, routing) | App boots on device |
| APP-2 | Map screen (react-native-maps, marker rendering) | Map displays |
| APP-3 | Nearby fetch integration (call driver-service, dynamic markers) | Markers update |
| APP-4 | Station bottom sheet (open on marker tap, station details) | Sheet shows details |
| APP-5 | Skeleton loading states (map skeleton, sheet skeleton) | Skeletons shown |
| APP-6 | Event tracking (map_open, station_click, station_view, map_pan, map_zoom) | Events sent |

### Sprint 6 — Integration (System Binding)

| ID | Task | Exit Criteria |
|---|---|---|
| INT-1 | API wiring validation (mobile → driver-service, mobile → clickstream-service) | All requests succeed |
| INT-2 | Event correctness check (verify event flow consistency) | Events match actions |
| INT-3 | End-to-end UX flow (open → map → stations → tap → details → event) | Full journey works |
| INT-4 | Payload optimization (reduce size, remove redundant fields) | Payloads minimal |

### Sprint 7 — Stabilization (Mandatory Final Gate)

| ID | Task | Exit Criteria |
|---|---|---|
| STAB-1 | PostGIS performance tuning (analyze query plan, index usage) | < 200ms |
| STAB-2 | API contract tests (driver-service + clickstream endpoints) | All pass |
| STAB-3 | UX audit (skeleton coverage, no blank states, error states) | Fully covered |
| STAB-4 | Map performance tuning (reduce re-renders, fix marker jitter) | Smooth 60fps |
| STAB-5 | Event pipeline validation (no missing events, no duplicates) | Consistent |

---

## Architecture Summary (MVP-1)

```
Mobile App
   ↓
Driver Service (Rust :8080)
   ↓
platform_db (PostGIS :5432)
   ↓
Clickstream Service (Rust :8082)
   ↓
analytics_db (:5433)
```
