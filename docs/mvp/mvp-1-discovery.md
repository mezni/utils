# MVP-1: Discovery Core

**Focus:** Deliver high-quality map-based EV station discovery

---

## Scope

| Included | Excluded |
|---|---|
| Station discovery (map + nearby) | Admin CRUD (MVP-2) |
| Station details view | Auth / RBAC (MVP-3) |
| Basic charger info | Analytics dashboards (MVP-4) |
| Driver mobile app (primary) | Web driver MVP-2 parity |
| Clickstream event capture | Performance optimization (MVP-5) |
| platform_db + analytics_db setup | Production infra (MVP-6) |

---

## Execution Order

1. platform_db schema + PostGIS
2. driver-service (MVP-1 APIs)
3. clickstream-service
4. mobile-driver app map UI
5. integration (UX working end-to-end)
6. stabilization sprint

---

## Task Breakdown

### 1. Database — platform_db init

- Inventory schema creation (partner, station, charger)
- PostGIS extension enable
- Indexes for geospatial queries
- Seed stations data

### 2. Database — analytics_db init

- raw_events table
- Indexes for event queries

### 3. Driver Service (8080)

- Setup Rust Actix service with sqlx, tokio, serde, tracing
- `GET /api/v1/stations` — map bounds query
- `GET /api/v1/stations/nearby` — radius search
- `GET /api/v1/stations/{id}` — station details
- PostGIS integration (radius query)

### 4. Clickstream Service (8082)

- Setup Rust Actix service
- `POST /api/v1/events` — event capture
- `POST /api/v1/events/batch` — batch events
- Payload validation
- Append-only insert into raw_events

### 5. Mobile Driver App

- Expo SDK 54 setup
- Map screen (react-native-maps)
- Station markers rendering
- Bottom sheet station detail
- Nearby search flow
- Skeleton loading states
- Gesture-first navigation
- Haptic feedback for primary actions
- Reanimated-only animations

### 6. Integration

- Driver app → driver-service (station queries)
- Driver app → clickstream-service (event capture)
- Map radius query wired to backend

### 7. Stabilization Sprint (MANDATORY)

- Fix map jitter issues
- Optimize PostGIS query latency
- Reduce API response payload
- UX polishing (loading states, skeletons)
- Event consistency validation

---

## API Contracts

See `/docs/backend/api-contracts.md` for full details.

---

## Performance Targets

- `/stations/nearby` → < 200ms response time
- Map load → < 2s initial render
- Event ingestion → fire-and-forget, non-blocking

---

## Success Criteria

1. Driver can open map and see stations
2. Driver can get nearby stations by location
3. Driver can view station details with charger info
4. Events are captured and stored in analytics_db
5. All endpoints respond correctly under /api/v1/
6. Stabilization sprint completed with all known issues resolved
