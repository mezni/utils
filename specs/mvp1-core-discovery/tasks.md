# Tasks: MVP-1 Core Geospatial Discovery

**Branch**: `mvp1-core-discovery` | **Date**: 2026-06-14

## Phase A — Database Layer

### A1: Create platform-init.sql
- [ ] `CREATE EXTENSION postgis`
- [ ] Create schemas: `configuration`, `inventory`, `gis`
- [ ] Create `configuration.plug_types` table with seeds (ccs2, type2, chademo)
- [ ] Create `inventory.partners` table with constraints
- [ ] Create `inventory.stations` table with constraints
- [ ] Create `inventory.chargers` table with FK to plug_types
- [ ] Create `gis.osm_stations` table with GEOMETRY(Point, 4326)
- [ ] Create GIST index on `gis.osm_stations.coordinates`

### A2: Create functions.sql
- [ ] Implement `gis.get_nearby_stations(driver_lon, driver_lat, radius)`
- [ ] TABLE return: id, name, address, distance_meters, lat, lon, available_chargers JSONB
- [ ] Use `ST_DWithin` with geography cast
- [ ] Aggregate chargers via `jsonb_agg` from inventory.chargers
- [ ] Filter `is_available = TRUE`

### A3: Create triggers.sql
- [ ] Implement `gis.sync_inventory_station_to_gis_cache()`
- [ ] Handle `is_live = FALSE` → DELETE from gis.osm_stations
- [ ] Handle insert/update → UPSERT into gis.osm_stations
- [ ] Bind trigger on `inventory.stations` AFTER INSERT OR UPDATE

## Phase B — Rust Libraries

### B1: geo-core
- [ ] Define `TUNISIA_MIN_LON`, `MAX_LON`, `MIN_LAT`, `MAX_LAT` constants
- [ ] Implement `is_within_tunisia(lon, lat) -> bool`

### B2: db-core
- [ ] Implement `create_platform_pool(database_url) -> PgPool`
- [ ] Config: max_connections=20, min_connections=5, acquire_timeout=3s, idle_timeout=60s

### B3: services-shared
- [ ] Define `ChargerDto` (serde + sqlx::types::Json compatible)
- [ ] Define `NearbyStationRow` with `FromRow` + `Json<Vec<ChargerDto>>`
- [ ] Define `PlatformId` newtype with prefix validation
- [ ] Define `ClaimsContext` with `mock_mvp1_context()`
- [ ] Implement `init_platform_subscriber(service_name)`

## Phase C — Driver Service

### C1: Service skeleton
- [ ] Create `main.rs` with Actix-Web `HttpServer`
- [ ] Create `config.rs` reading env vars (DATABASE_URL, HOST, PORT)
- [ ] Create `db.rs` initializing pool via db-core

### C2: Routes & handlers
- [ ] `GET /api/v1/health` → 200 OK
- [ ] `GET /api/v1/stations/nearby?longitude&latitude&radius` → 200 { stations: [...] }
- [ ] Validate coordinates via geo-core
- [ ] Map DB Row → JSON response

### C3: Error handling
- [ ] 400 for out-of-bounds coordinates
- [ ] 422 for missing/negative radius
- [ ] 503 for DB connection failures

## Phase D — OSM Import

### D1: import-tunisia-osm.sh
- [ ] Query Overpass API for `amenity=charging_station` in Tunisia
- [ ] Parse response with awk/jq
- [ ] Insert into `gis.osm_stations` via psql
- [ ] Handle dedup via ON CONFLICT

### D2: seed-mvp1-data.sql
- [ ] Insert 3-5 demo stations directly into gis.osm_stations (source='SEED')
- [ ] Each station gets name, address, coordinates, is_available=TRUE

## Phase E — Shared Mobile Lib

### E1: constants.ts
- [ ] TUNISIA_GEO_BOUNDS, TUNIS_INITIAL_REGION
- [ ] STATION_AVAILABILITY, CHARGER_STATUS const enums

### E2: types.ts
- [ ] Coordinate, ChargerDto, NearbyStationDto interfaces
- [ ] StationAvailabilityType, ChargerStatusType types

### E3: index.ts
- [ ] Barrel export
- [ ] `verifyCoordinateWithinTunisia()` utility

### E4: Build config
- [ ] package.json, tsconfig.json

## Phase F — Mobile App

### F1: Project init
- [ ] app.json with Expo SDK 54 config
- [ ] package.json with dependencies (react-native-maps, expo-location)

### F2: Map screen
- [ ] MapView centered on Tunis (36.8065, 10.1815)
- [ ] Station markers from API response (bare markers, no callout on tap)
- [ ] 300ms debounce on region change
- [ ] React.memo on markers
- [ ] tracksViewChanges = false

### F3: API service
- [ ] fetch wrapper for driver-service API
- [ ] Error state handling
- [ ] Loading state

## Phase G — Web Driver

### G1: Project init
- [ ] Vite + React + TypeScript project setup
- [ ] package.json with leaflet, react-leaflet, @types/leaflet
- [ ] tsconfig.json, vite.config.ts
- [ ] index.html entry point

### G2: Map screen
- [ ] MapContainer centered on Tunis (36.8065, 10.1815)
- [ ] TileLayer with OpenStreetMap tiles
- [ ] Station markers from API response (bare markers, no popup on click)
- [ ] 300ms debounce on viewport change (onMoveEnd)
- [ ] React.memo on marker components

### G3: API service
- [ ] fetch wrapper for driver-service API
- [ ] Error state handling
- [ ] Loading state

## Phase H — Infrastructure

### G1: Dockerfiles
- [ ] `driver-service/Dockerfile` — multi-stage Rust build
- [ ] Use `rust:1.85-slim-bookworm` for builder, `debian:bookworm-slim` for runtime

### G2: docker-compose.yml
- [ ] `postgis` service (postgis/postgis:17-3.4)
- [ ] `driver-service` service (build from source)
- [ ] `traefik` service (v3.0)
- [ ] Shared network `bornemap-platform-mesh`
- [ ] Volume for postgres data

### G3: Traefik config
- [ ] `traefik.yml` — static config (entrypoints, providers)
- [ ] `dynamic.yml` — route: `/api/v1/driver/*` → driver-service:3001

### G4: Supporting files
- [ ] `.env.example` with default DATABASE_URL
- [ ] `Makefile` with targets: up, down, build, logs, import-osm, seed
