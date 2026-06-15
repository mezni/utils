# BorneMap Architecture — MVP-1

## System Overview

BorneMap is a geospatial EV charging station discovery platform for Tunisia. MVP-1 implements the core discovery pipeline: OSM data → PostGIS → Rust API → Mobile map.

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                     🌐 Public Internet                        │
│  ┌──────────────────────────────────────────────────┐        │
│  │         Driver Mobile (Expo SDK 54)              │        │
│  │  MapView + react-native-maps + 300ms debounce    │        │
│  └──────────────────────┬───────────────────────────┘        │
│                         │ HTTP :80                            │
│                         ▼                                     │
│  ┌──────────────────────────────────────────────────┐        │
│  │  🚦 Traefik v3 (bornemap-gateway)                │        │
│  │  Route: /api/v1/driver/* → driver-service:3001   │        │
│  └──────────────────────┬───────────────────────────┘        │
│                         │                                    │
│                         ▼                                    │
│  ┌──────────────────────────────────────────────────┐        │
│  │  ⚙️ driver-service (Rust + Actix-Web 4)          │        │
│  │  Port :3001 · Read-only geospatial engine         │        │
│  │  Dependencies: SQLx 0.7, serde, tracing, geo-core │        │
│  └──────┬───────────────────────────────────────────┘        │
│         │ SQLx Pool (read)                                    │
│         ▼                                                     │
│  ┌──────────────────────────────────────────────────┐        │
│  │  🗄️ PostgreSQL 17 + PostGIS 3.4                  │        │
│  │  bornemap_platform                                │        │
│  │  ┌──────────────────┐  ┌──────────────────┐      │        │
│  │  │  inventory       │  │  gis             │      │        │
│  │  │  ┌────────────┐  │  │  ┌────────────┐  │      │        │
│  │  │  │ partners   │  │  │  │ osm_stations│──│──────│──→ GIST│
│  │  │  ├────────────┤  │  │  │ (Point,4326)│  │      │        │
│  │  │  │ stations───│──│──│→ │ trigger     │  │      │        │
│  │  │  ├────────────┤  │  │  └────────────┘  │      │        │
│  │  │  │ chargers   │  │  │  fn:             │      │        │
│  │  │  └────────────┘  │  │  get_nearby_     │      │        │
│  │  └──────────────────┘  │  stations()      │      │        │
│  │  ┌──────────────┐      └──────────────────┘      │        │
│  │  │ configuration│      (read-optimized cache)     │        │
│  │  │ plug_types   │                                 │        │
│  │  └──────────────┘                                 │        │
│  └──────────────────────────────────────────────────┘        │
│                         ▲                                     │
│  ┌──────────────────────┴───────────────────────────┐        │
│  │  🛰️ import-tunisia-osm.sh                       │        │
│  │  Overpass API → psql INSERT INTO gis.osm_stations│        │
│  └──────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────┘
```

## Layer Responsibilities

### 1. Data Layer (PostgreSQL 17 + PostGIS 3.4)
- Source of truth: `inventory.stations`
- Read cache: `gis.osm_stations` with GIST spatial index
- Query: `gis.get_nearby_stations()` — all geo-logic in SQL
- Cross-schema sync via PL/pgSQL trigger

### 2. Service Layer (Rust + Actix-Web)
- `driver-service` (:3001): read-only geospatial API
- Thin HTTP-to-DB bridge: validate → query → map → respond
- No business logic — all filtering in PostGIS

### 3. Gateway (Traefik v3)
- Single ingress point on port 80
- Route prefix stripping: `/api/v1/driver/*` → `driver-service:3001`
- Intended for future multi-service routing

### 4. Client (Expo SDK 54)
- MapView centered on Tunis (36.8065, 10.1815)
- Station markers from API with `tracksViewChanges = false`
- 300ms debounce on region change for API calls
- React.memo on all marker components

### 5. Import (bash + Overpass API)
- One-shot script: fetch OSM Tunisia EV stations → insert into `gis.osm_stations`
- Source=`'OSM_IMPORT'` to distinguish from platform-synced records

## Data Flow

```
OSM Import Flow:
  Overpass API → curl → awk/psql → gis.osm_stations (source='OSM_IMPORT')

Platform Sync Flow:
  INSERT/UPDATE inventory.stations → trigger → gis.osm_stations (source='PLATFORM_SYNC')

Query Flow:
  GET /api/v1/stations/nearby?lon=X&lat=Y&radius=Z
    → geo-core validates bounds
    → SQLx calls gis.get_nearby_stations(X, Y, Z)
    → PostGIS uses GIST index + ST_DWithin
    → Returns JSON with station list + charger aggregates
    → Maps to NearbyStationDto and responds

Render Flow:
  API response → mobile-app state → MapView markers
    → 300ms debounce on pan
    → React.memo markers
    → tracksViewChanges = false
```

## Deferred Components (Future Phases)

| Component | Phase | Port | Purpose |
|-----------|-------|------|---------|
| auth-service | Phase 2 | 3000 | Keycloak bridge, JWT validation |
| admin-service | Phase 3 | 3002 | CRUD for partners, stations, chargers |
| clickstream-service | Phase 5 | 3003 | Append-only analytics ingestion |
| web-driver | Phase 6 | — | Leaflet-based web map |
| admin-dashboard | Phase 7 | — | Admin UI for governance |
| Keycloak | Phase 2 | 8080 | Identity provider (internal) |

## MVP-1 Service Configuration

### Environment Variables
```
DATABASE_URL=postgres://bornemap:bornemap@postgres:5432/bornemap_platform
DRIVER_SERVICE_HOST=0.0.0.0
DRIVER_SERVICE_PORT=3001
RUST_LOG=info,driver_service=debug
```

### Docker Services
| Service | Image | Port(s) | Depends On |
|---------|-------|---------|------------|
| postgres | postgis/postgis:17-3.4 | 5432 | — |
| driver-service | build (source) | 3001 | postgres |
| traefik | traefik:v3.0 | 80, 8080 | driver-service |

## Security Model (MVP-1)

- **No authentication**: All requests accepted with mock identity
- **No encryption in-transit**: HTTP only (local dev)
- **No secrets management**: DATABASE_URL in env file
- **Database**: trust auth for local dev, password auth in compose

Full security model (JWT, RBAC, Keycloak) introduced in Phase 2.
