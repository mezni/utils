# BorneMap Architecture — v2.0

## 1. High-Level Architecture

```
                ┌─────────────────────┐
                │   Admin Service     │
                │ (Write EV domain)   │
                └─────────┬───────────┘
                          │
                          ▼
                    ┌───────────┐
                    │    EV     │  ← Source of Truth
                    │ (domain)  │
                    └─────┬─────┘
                          │ DB TRIGGERS
                          ▼
                    ┌───────────┐
                    │    GIS    │  ← Derived Projection
                    │ (read model│
                    └─────┬─────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
 ┌─────────────────┐           ┌─────────────────┐
 │ Driver Service  │           │ Auth Service    │
 │ (read-only)     │           │ (users schema)  │
 └─────────────────┘           └─────────────────┘
```

## 2. Domain Ownership Model

| Domain | Responsibility | Owner |
|--------|---------------|-------|
| `ev` | Business domain (partners, stations, connectors) | Admin Service |
| `gis` | Geospatial projection + query optimization | Database (auto-generated via triggers) |
| `users` | Authentication + identity | Auth Service |

## 3. Hard Boundaries (NON-NEGOTIABLE)

### EV DOMAIN
- Only Admin Service writes
- Source of truth for all business data
- No GIS logic in application code

### GIS DOMAIN
- Fully derived via DB triggers
- No manual writes from any service
- No business logic — purely spatial

### USERS DOMAIN
- Only Auth Service writes
- JWT authority for identity

## 4. Data Flow

### 4.1 Write Flow (Admin → EV)
```
Admin Service
   ↓
ev.partners / ev.stations / ev.connectors
   ↓
DB Trigger
   ↓
gis.station_locations (auto-update via gis.sync_station_location())
```

### 4.2 Read Flow (Driver → GIS-first)
```
Driver Service
   ↓
CALL gis.nearby_stations(lat, lng, radius)
   ↓
GIS projection (fast spatial index lookup via GiST)
   ↓
JOIN ev data (inside DB function only)
   ↓
Return enriched response
```

### 4.3 Auth Flow
```
Auth Service
   ↓
users schema
   ↓
JWT issuance
   ↓
Driver/Admin validate via middleware
```

## 5. GIS Layer Design

### 5.1 Projection Table
`gis.station_locations` contains:
- `station_id` (FK to ev.stations)
- `geom` (PostGIS GEOGRAPHY(POINT, 4326))

### 5.2 Core Function (Single Entry Point)
`gis.nearby_stations(lat, lng, radius_m)` — internally:
- `ST_DWithin(geom, point, radius)` for spatial filtering
- `JOIN ev.stations` for business data
- `ORDER BY distance ASC`
- `LIMIT N` (configurable)

### 5.3 Synchronization
Trigger `trg_sync_station_location` on `ev.stations`:
- `AFTER INSERT OR UPDATE OF latitude, longitude`
- Calls `gis.sync_station_location()` to upsert into `gis.station_locations`

## 6. Service Architecture (Clean Architecture)

### Admin Service (WRITE ONLY)
- **Responsibilities:** partners CRUD, stations CRUD, connectors CRUD
- **Access:** Writes → ev; Reads → optional ev
- **Forbidden:** ❌ gis access; ❌ spatial logic

### Driver Service (READ ONLY)
- **Responsibilities:** nearby stations, station details, map data
- **Access:** Reads → `gis.nearby_stations()`; ev only via joins inside DB function
- **Forbidden:** ❌ writes; ❌ direct GIS SQL; ❌ business mutation

### Auth Service
- **Responsibilities:** users, login/register, JWT, roles
- **Access:** users schema only

## 7. Logical Layering (All Services)

```
Presentation Layer (HTTP, actix-web/axum)
   ↓
Application Layer (Use Cases / Services)
   ↓
Domain Layer (Business Rules, Entities, Value Objects)
   ↓
Infrastructure Layer (DB repositories, Postgres, GIS functions)
```

## 8. Database Architecture

```
platform_db
│
├── users  → Auth Service
├── ev     → Admin Service (source of truth)
└── gis    → DB-generated projection (read-only)
```

### EV Schema (Source of Truth)
- `ev.partners` — charging network partners
- `ev.stations` — charging stations
- `ev.connectors` — individual connectors per station

### GIS Schema (Derived Only)
- `gis.station_locations` — spatial projection of stations
- `gis.nearby_stations()` — query function
- `gis.sync_station_location()` — trigger function

## 9. Key Design Improvements (v1 → v2)

| Improvement | v1 | v2 |
|-------------|----|----|
| GIS handling | Service-side joins, repeated spatial SQL | True read model with DB triggers |
| Query entry | Ad-hoc queries | Single `gis.nearby_stations()` |
| CQRS | Mixed read/write responsibilities | Strong CQRS: Admin writes EV, Driver reads GIS |
| Service complexity | Heavy driver service | Thin driver: HTTP → DB function → response |

## 10. Evolution Path

| Phase | Description |
|-------|-------------|
| Phase 1 (Current) | EV + GIS + Admin + Driver + Auth |
| Phase 2 | Caching layer for GIS queries (Redis) |
| Phase 3 | Event streaming (clickstream, analytics) |
| Phase 4 | Multi-region GIS scaling |
| Phase 5 | Read replicas for heavy map traffic |

## 11. Final Architectural Rules (LOCKED)

1. `ev` is the only source of truth
2. `gis` is always derived
3. Admin Service is the only writer to `ev`
4. Driver Service is read-only
5. Auth Service owns `users`
6. No cross-service DB writes
7. Spatial logic lives only in Postgres
8. All map queries go through `gis.nearby_stations()`
9. Services never implement GIS logic
10. Triggers must not contain business logic
