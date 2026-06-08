# Implementation Plan

Delivery strategy and sprint roadmap for BorneMap.

## Delivery Philosophy

Build in MVP cycles. Each MVP is complete and deployable. Validate the product before adding infrastructure. Add infrastructure only when the product direction is confirmed. Never break what a previous MVP delivered.

The database schema is designed once with the full platform in mind. Each MVP activates a subset of that schema. The frontend apps are built once and connected progressively to better backends.

## MVP Map

```
MVP-1   Python + mock data + three apps         ← current
MVP-2   Rust services + PostGIS + CI/CD
MVP-3   Authentication + user management
MVP-4   GIS synchronization
MVP-5   Analytics and reporting
MVP-6   Production hardening + Traefik + launch
```

## MVP-1 — Core Product Loop

**Goal**: A partner creates stations and chargers through the dashboard. A driver finds nearby stations on a map. The full loop works end to end with real data.

**Stack**: Python FastAPI, PostgreSQL (no PostGIS), React + Vite, React Native + Expo SDK 54.

**What this MVP proves**: The product concept is valid. The data model works. The team can ship.

### MVP-1 Scope

**In scope**:
- One Python FastAPI service with 16 endpoints
- PostgreSQL with `inventory` schema (partner, station, charger)
- `gis` schema created empty and reserved
- Simple Euclidean distance for nearby endpoint
- Driver Web App — Map screen and Station Detail screen
- Driver Mobile App — Map screen and Station Detail screen
- Dashboard App — Overview, Partners, Stations, Chargers screens
- Dev seeds with 3 partners, 15 real Tunisian stations, 24 chargers

**Out of scope**:
- Authentication, Keycloak, JWT, user accounts
- Favorites, reviews, profile
- PostGIS, spatial indexes, OSM import
- GIS synchronization
- Analytics
- Docker Compose, CI/CD, Traefik, TLS
- RTL and i18n
- Production deployment

### Sprint 1.1 — Backend and Database

**Duration**: 2 weeks

**Goal**: FastAPI service running with all 16 endpoints returning real data from PostgreSQL.

**Tasks**:

**Project setup**:
- Initialize Python project under `source/services/bornemap-service/`
- Set up FastAPI, SQLAlchemy, Alembic, psycopg2, pydantic, uvicorn
- Configure `.env` with `DATABASE_URL` and `API_PREFIX=/api`
- Set up local PostgreSQL database named `ev_platform`

**Migrations**:
- `0001_schemas.sql` — create `inventory` and `gis` schemas
- `0002_inventory_tables.sql` — partner, station, charger tables with UUID PKs
- `0003_indexes.sql` — FK indexes on `station.partner_id`, `charger.station_id`

**Seeds**:
- 3 partners with real Tunisian company names
- 15 stations across Tunis, Sfax, Sousse, Bizerte, Nabeul, Hammamet, Monastir, Djerba, Kairouan, Gabès
- 24 chargers with varied connector types (Type 2, CCS, CHAdeMO) and power ratings

**Endpoints**:
- `GET /api/health` — returns service name and db connectivity status
- `GET /api/stations/nearby` — lat, lng, radius_km, returns stations ordered by Euclidean distance with `distance_m` field
- `GET /api/stations` — list all stations, optional `partner_id` filter, returns charger counts
- `GET /api/stations/:id` — station detail with full charger list
- `POST /api/stations` — create station, returns created record
- `PUT /api/stations/:id` — update station fields, returns updated record
- `DELETE /api/stations/:id` — delete station, returns 204
- `GET /api/partners` — list all partners
- `GET /api/partners/:id` — partner detail
- `POST /api/partners` — create partner
- `PUT /api/partners/:id` — update partner name
- `DELETE /api/partners/:id` — delete partner
- `GET /api/chargers` — list chargers, optional `station_id` filter
- `GET /api/chargers/:id` — charger detail
- `POST /api/chargers` — create charger
- `PUT /api/chargers/:id` — update charger
- `DELETE /api/chargers/:id` — delete charger

**Smoke tests**:
- One test per endpoint covering happy path and not-found case
- Test that nearby returns stations ordered by distance
- Test that charger list filters correctly by `station_id`

**Done when**:
- `GET /api/health` returns `{"status":"ok","service":"bornemap-service","db":"ok"}`
- Nearby endpoint returns stations ordered by distance for Tunis center coordinates
- All 16 CRUD endpoints return correct responses against seeded database
- All smoke tests pass

### Sprint 1.2 — Dashboard App

**Duration**: 2 weeks

**Goal**: Dashboard App fully functional for managing partners, stations, and chargers with real API data.

**Tasks**:

**Project setup**:
- Initialize under `source/apps/dashboard/` with Vite + React + TypeScript + Tailwind
- Configure Tailwind extending `source/packages/ui/tailwind.config.base.js`
- Set up React Router with four routes
- Configure `VITE_API_BASE_URL` pointing to `http://localhost:8000`

**Design token setup**:
- Create `source/packages/ui/src/tokens/colors.ts` with full token set from constitution section 5.1
- Create `source/packages/ui/tailwind.config.base.js` extending all tokens
- Both Dashboard and Driver Web import from this base

**AppShell component**:
- Fixed left sidebar — w-64, white background, border-r
- Brand header — BorneMap logo mark in `brand.primary`, Admin label
- Navigation items — Overview, Partners, Stations, Chargers
- Active item style — bg `brand.sageLight`, text `brand.primary`
- Bottom settings link
- Main area — TopBar + scrollable PageContent on `surface.background`

**Overview screen**:
- Three StatCards showing real counts from API — total partners, total stations, total chargers
- Simple station table with name, partner, charger count, status badge

**Partners screen**:
- DataTable with columns: name, created date, station count, actions
- Create partner button opens modal with name field
- Edit partner inline via modal
- Delete partner with confirmation dialog
- Empty state when no partners
- Loading skeleton while fetching

**Stations screen**:
- DataTable with columns: name, address, partner name, charger count, actions
- Partner filter dropdown populated from API
- Create station form — name, address, latitude, longitude, partner selection
- Edit station via modal
- Delete station with confirmation
- Latitude and longitude validated as numbers in correct range

**Chargers screen**:
- DataTable with columns: station name, connector type, power kw, status badge, actions
- Station filter dropdown populated from API
- Create charger form — station selection, connector type, power kw, status
- Edit charger via modal — status update is the primary use case
- Delete charger with confirmation
- StatusBadge component — available (green), in_use (amber), maintenance (red)

**Error handling on all screens**:
- API unreachable — ErrorState with retry button
- Empty data — EmptyState with create prompt
- Form validation — inline field errors before submit

**Done when**:
- Partner created in Dashboard appears in Partners table immediately
- Station created for that partner appears in Stations table with correct partner name
- Charger created for that station appears in Chargers table with StatusBadge
- Editing any entity updates the table without page reload
- Deleting any entity removes it from the table
- All filter dropdowns populated from real API data
- Overview stat cards show real counts

### Sprint 1.3 — Driver Web App

**Duration**: 2 weeks

**Goal**: Driver Web App shows a Leaflet map with real station markers and station detail from the API.

**Tasks**:

**Project setup**:
- Initialize under `source/apps/driver-web/` with Vite + React + TypeScript + Tailwind
- Add Leaflet and react-leaflet dependencies
- Configure Tailwind extending shared token base
- Set up React Router with two routes: `/` and `/stations/:id`
- Configure `VITE_API_BASE_URL`

**Map screen**:
- Full-bleed Leaflet map, height 100vh
- OpenStreetMap tiles
- Initial view centered on Tunisia (lat 33.8869, lng 9.5375, zoom 7)
- Fetch all stations on mount via `GET /api/stations`
- Render CircleMarker per station:
  - Fill `brand.glow` (#00E676) if `available_count > 0`
  - Fill `status.maintenance` (#EF4444) if all chargers unavailable
  - White border, radius 8
- Popup on marker click showing station name, address, available/total chargers, distance
- Navigate to Station Detail on popup link click
- Floating top bar — brand name BorneMap in `brand.primary`, white background, border-b
- Loading state — top bar shows spinner while fetching
- Error state — top bar shows error message with retry link
- ZoomControls — floating +/- buttons on right side

**Station Detail screen**:
- Fetch station by ID via `GET /api/stations/:id`
- Back button navigates to map
- Station name, address, partner name
- Charger list — each row shows connector type, power kw, StatusBadge
- Loading skeleton while fetching
- Error state if station not found (404)

**Done when**:
- Map loads with all 15 seeded stations as markers
- Green markers for stations with available chargers, red for unavailable
- Marker popup shows correct station info
- Station Detail shows real charger data
- Station created in Dashboard appears on map after page reload
- Changing charger status in Dashboard changes marker color after reload

### Sprint 1.4 — Driver Mobile App

**Duration**: 2 weeks

**Goal**: Driver Mobile App shows a map with real station markers and station detail on iOS and Android.

**Tasks**:

**Project setup**:
- Initialize under `source/apps/driver-mobile/` with Expo SDK 54
- Configure expo-router with two screens
- Add react-native-maps 1.18.0 and expo-location ~18.0.0
- Configure `EXPO_PUBLIC_API_BASE_URL`

**Map screen**:
- react-native-maps full-bleed map, flex 1
- Request expo-location foreground permission on mount
- If granted: use device coordinates for initial region and nearby fetch
- If denied: use Tunisia center (lat 33.8869, lng 9.5375) — no crash, no error modal
- Fetch stations via `GET /api/stations/nearby` with resolved coordinates and `radius_km` 500
- Render Marker per station:
  - `pinColor` green (#00E676) if `available_count > 0`
  - `pinColor` red (#EF4444) if no available chargers
- Tap marker shows callout with station name and available count
- Navigate to Station Detail on callout tap using expo-router push
- SafeAreaView with header showing BorneMap brand name
- ActivityIndicator while fetching
- Text error message on fetch failure

**Station Detail screen**:
- Fetch station by ID via `GET /api/stations/:id`
- Back button (expo-router back)
- ScrollView with station name, address
- FlatList or map of chargers with connector type, power kw, status text
- Status displayed as colored text — green, amber, red matching token values
- Loading indicator while fetching
- Error message if fetch fails

**Done when**:
- Map loads with stations on iOS simulator
- Map loads with stations on Android emulator
- Location denied uses Tunisia center — no crash
- Marker colors reflect availability
- Station Detail shows real charger data
- App does not crash on network error — shows error text

### Sprint 1.5 — Integration and Hardening

**Duration**: 1 week

**Goal**: Full product loop verified end to end. Everything is consistent and documented.

**Tasks**:

**Full loop verification**:
- Create partner in Dashboard → create station → create chargers → reload Driver Web map → station marker appears with correct color → click to view detail → open Driver Mobile → station appears on mobile map
- Change charger status to maintenance in Dashboard → reload both driver apps → marker color changes correctly
- Delete station in Dashboard → reload both driver apps → station disappears

**Backend fix sweep**:
- Run all smoke tests — all pass
- Test nearby endpoint with coordinates far from Tunisia — returns empty list correctly
- Test create station with missing required fields — returns 422 with field errors
- Test delete partner that has stations — returns appropriate error (or cascades correctly — decision must be documented)
- Verify all endpoints return correct HTTP status codes

**Dashboard fix sweep**:
- Test every form with all fields empty — validation messages appear
- Test station form with non-numeric latitude — rejected
- Test latitude out of range (-90 to 90) and longitude out of range — rejected
- Test API unreachable — ErrorState appears on all four screens
- Test Chrome, Firefox, Safari — no visual regressions

**Driver Web fix sweep**:
- Test with no stations in database — EmptyState or empty map (no crash)
- Test API unreachable — error message in top bar
- Test Chrome, Firefox, Safari

**Driver Mobile fix sweep**:
- Test on iOS simulator — no crashes
- Test on Android emulator — no crashes
- Test with location permission denied — map loads at Tunisia center
- Test with API unreachable — error text appears (no crash)

**Performance check**:
- Nearby endpoint with 15 stations responds under 200ms
- Station list responds under 200ms
- No N+1 queries on station detail (chargers loaded in one query)

**Documentation**:
- Write `docs/guides/onboarding.md` — how to run full stack from scratch
- Write `docs/api/bornemap-service.md` — all 16 endpoints with request and response shapes
- Write `docs/project/phases/mvp-01-status.md` with sprint outcomes and done criteria
- Record any decisions made during hardening in `docs/project/decisions.md`

### MVP-1 Done Criteria

- [ ] All 16 endpoints return correct data against real database
- [ ] Nearby endpoint returns stations ordered by distance
- [ ] Smoke tests all pass
- [ ] Dashboard — partner, station, charger CRUD fully working
- [ ] Dashboard — filter dropdowns populated from real API
- [ ] Dashboard — all form validations working
- [ ] Driver Web — map shows real markers with correct colors
- [ ] Driver Web — station detail shows real charger data
- [ ] Driver Web — works on Chrome, Firefox, Safari
- [ ] Driver Mobile — map shows real markers on iOS simulator
- [ ] Driver Mobile — map shows real markers on Android emulator
- [ ] Driver Mobile — location denied handled gracefully
- [ ] Full loop tested: create in dashboard → visible in driver apps
- [ ] All apps handle API unreachable gracefully
- [ ] No N+1 queries in any endpoint
- [ ] Onboarding guide tested from scratch on clean machine
- [ ] API documentation complete and accurate
- [ ] Zero Class A bugs open

## MVP-2 — Rust Services + PostGIS + CI/CD

**Goal**: Replace the Python service with production-grade Rust services. Add real spatial queries via PostGIS. Automate validation with GitHub Actions. Introduce Docker Compose for local development.

**What changes**: Backend only. Frontend apps update their base URL. No schema changes that break existing data.

### MVP-2 Scope

**In scope**:
- Driver Service (Actix-web) — public discovery endpoints
- Admin Service (Actix-web) — partner, station, charger CRUD
- Shared crates: `ev-core` (NanoIDs, types), `ev-db` (pool, pagination)
- PostGIS extension and spatial indexes on station coordinates
- Real `ST_DWithin` spatial query replacing Euclidean distance
- Migrate UUIDs to NanoID prefixed identifiers (data migration)
- Docker Compose stack — PostgreSQL + PostGIS, Driver Service, Admin Service
- GitHub Actions CI for Rust lint, test, build and frontend lint, build
- Path-scoped CI workflows per service and app

**Out of scope**:
- Authentication (MVP-3)
- GIS schema population, OSM import, trigger (MVP-4)
- Analytics (MVP-5)
- Traefik, TLS, production deployment (MVP-6)

### Sprint 2.1 — Rust Workspace and Shared Crates

**Duration**: 1 week

**Goal**: Cargo workspace compiles. Shared crates are built and tested.

**Tasks**:
- Initialize Cargo workspace at `source/` root
- Create `source/services/driver-service/` and `source/services/admin-service/` as empty Actix-web binaries
- Create `source/services/crates/ev-core/` — NanoID generation for all six prefixes, ConnectorType and ChargerStatus enums
- Create `source/services/crates/ev-db/` — PgPool setup, OffsetParams, PaginatedResponse
- Unit tests for all ID generation functions
- `cargo build --all` succeeds
- `cargo test -p ev-core` passes

### Sprint 2.2 — Database Migration to PostGIS and NanoIDs

**Duration**: 1 week

**Goal**: Database upgraded with PostGIS, spatial index, NanoID data migration.

**Tasks**:
- Add PostGIS extension migration
- Add spatial index migration on station coordinates
- Write data migration converting all existing UUIDs to NanoID prefixed format
- Update all FK references in the migration
- Verify migration runs cleanly on MVP-1 seed data
- Verify `ST_DWithin` query works on migrated data
- Write canonical migrations to `database/migrations/`

### Sprint 2.3 — Driver Service

**Duration**: 2 weeks

**Goal**: Driver Service implements all public discovery endpoints against real PostGIS database.

**Tasks**:
- Config struct from environment (`DATABASE_URL`, `SERVICE_PORT`, `API_PREFIX`)
- AppState with PgPool
- Typed AppError implementing ResponseError
- `GET /api/health` with database check
- `GET /api/stations/nearby` — `ST_DWithin` spatial query, returns `distance_m`
- `GET /api/stations/markers` — bbox query, minimal payload for map rendering
- `GET /api/stations/search` — text search with optional filters
- `GET /api/stations/:id` — detail with charger list and rating summary stub
- `GET /api/stations/:id/reviews` — stub returning empty list
- Integration tests with real test database for all endpoints
- Dockerfile for Driver Service

### Sprint 2.4 — Admin Service

**Duration**: 2 weeks

**Goal**: Admin Service implements full CRUD replacing the Python service.

**Tasks**:
- Same config and AppState pattern as Driver Service
- `GET /api/health`
- Full partner CRUD — five endpoints
- Full station CRUD — five endpoints
- Full charger CRUD — five endpoints
- `PUT /api/stations/:id/availability` — writes to station_availability table
- Dev scope header `X-Partner-Id` for testing (replaced by JWT in MVP-3)
- Integration tests for all endpoints including scope filtering
- Dockerfile for Admin Service

### Sprint 2.5 — Docker Compose and CI/CD

**Duration**: 1 week

**Goal**: Full local stack runs in Docker Compose. CI validates on every push.

**Tasks**:

**Docker Compose**:
- `infra/compose/docker-compose.yml` with postgis/postgis:16-3.4, Driver Service, Admin Service, pgAdmin
- Health checks for all containers
- `depends_on` with `service_healthy` conditions
- Both services run migrations on startup via `sqlx::migrate!`

**GitHub Actions**:
- `ci.yml` — full workspace lint and test on every push
- `ci-driver-service.yml` — path-scoped, includes PostgreSQL service container
- `ci-admin-service.yml` — same pattern
- `ci-driver-web.yml` — path-scoped to `apps/driver-web` and `packages`
- `ci-driver-mobile.yml` — TypeScript check only
- `ci-dashboard.yml` — path-scoped to `apps/dashboard` and `packages`

**Frontend URL update**:
- Both web apps update `VITE_API_BASE_URL` to point to Driver Service for discovery and Admin Service for management
- Driver Mobile updates `EXPO_PUBLIC_API_BASE_URL`

### Sprint 2.6 — MVP-2 Hardening

**Duration**: 1 week

**Tasks**:
- Full loop verified with Rust services — create in Dashboard → visible on Driver Web and Mobile
- `cargo test --all` passes
- `cargo clippy --all-targets -- -D warnings` passes with zero warnings
- Spatial query performance verified with EXPLAIN ANALYZE
- Docker Compose starts cleanly from zero on a clean machine
- All CI workflows pass on main branch
- Update `docs/api/` for both services
- Update `docs/guides/onboarding.md` for Docker Compose workflow
- Write `docs/project/phases/mvp-02-status.md`

### MVP-2 Done Criteria

- [ ] `cargo build --all` succeeds with zero warnings
- [ ] `cargo test --all` passes
- [ ] All Docker Compose services start and pass health checks
- [ ] `ST_DWithin` spatial query confirmed by EXPLAIN ANALYZE
- [ ] NanoID migration runs cleanly on MVP-1 seed data
- [ ] All CI workflows pass
- [ ] Full loop verified with Rust services
- [ ] Frontend apps connected to Rust services
- [ ] API documentation updated
- [ ] Zero Class A bugs

## MVP-3 — Authentication and User Management

**Goal**: Keycloak runs and issues JWTs. Both services enforce authentication. First-login provisioning creates user records. All three apps have working auth flows. RTL and i18n are introduced.

## MVP-4 — GIS Synchronization

**Goal**: Station coordinates are synchronized to the GIS schema via a PostgreSQL trigger. Driver Service uses real PostGIS spatial queries against the GIS layer. Map reflects precise station positions.

## MVP-5 — Analytics and Reporting

**Goal**: All three apps fire analytics events. Events persist through Clickstream Service into PostgreSQL. Aggregate jobs run. Admin Dashboard shows real analytics data.

## MVP-6 — Production Hardening and Launch

**Goal**: The platform is production-safe, fully tested, and deployable on a real server.

---

**See `constitution.md` section on MVPs for full details on each MVP scope, sprints, and done criteria.**
