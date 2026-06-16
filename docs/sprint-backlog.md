# BorneMap — Sprint Backlog

**Version**: 1.1
**Last updated**: 2026-06-15
**Convention**: Each MVP has one pre-sprint (doc generation) followed by 1-week code sprints. Pre-sprints are not time-boxed — complete before first code sprint starts.

---

## MVP-1 — Infra Kickoff

### Pre-Sprint 1.0 — Documentation
- [x] Complete `domain-model.md` (remaining entities: Favorite, Audit Log)
- [x] Write `db-schema.md` (empty schema DDL per schema: gis, inventory, users)
- [x] Write `env-vars.md` (MVP-1 scope: DB URLs, service ports)
- [x] Write `docker-compose-map.md` (MVP-1 containers: postgres, keycloak, analytics_db)
- [ ] Write Service READMEs stubs (auth-service, driver-service, admin-service)
- [x] Update `docs/mvp-1/STATUS.md`

### Sprint 1.1 — Monorepo Scaffold & Service Shells
- [ ] Initialize monorepo (Cargo workspace + pnpm workspaces)
- [ ] Scaffold `apps/mobile-driver`, `apps/web-driver`, `apps/dashboard` (empty shells)
- [ ] Scaffold `packages/shared-types`, `packages/shared-ui`, `packages/shared-hooks`, `packages/api-client`
- [ ] Scaffold `crates/db-models`, `crates/validation`
- [ ] Scaffold `services/auth-service` (Rust/Actix-web, health endpoint only)
- [ ] Scaffold `services/driver-service` (Rust/Actix-web, health endpoint only)
- [ ] Scaffold `services/admin-service` (Rust/Actix-web, health endpoint only)
- [ ] `GET /api/v1/health` + `GET /api/v1/health/ready` on all three services
- [ ] Update `docs/mvp-1/STATUS.md`

### Sprint 1.2 — Databases & Infrastructure
- [ ] `docker-compose.yml`: postgres/PostGIS container (`platform_db`)
- [ ] `docker-compose.yml`: `keycloak_db` container
- [ ] `docker-compose.yml`: `analytics_db` container
- [ ] Empty schema creation migrations: `gis`, `inventory`, `users` in `platform_db`
- [ ] Verify all services connect to DB on startup (`/health/ready` returns ok)
- [ ] `.env.example` with all MVP-1 env vars
- [ ] Update `docs/mvp-1/STATUS.md`

### Sprint 1.3 — Driver Apps Base Map & Dashboard Shell
- [ ] `mobile-driver`: Expo SDK 54 base map (react-native-maps), no data, Tunisia center
- [ ] `web-driver`: React + Leaflet base map, no data, Tunisia center
- [ ] `dashboard`: React + shadcn/ui blank page shell, routing scaffold
- [ ] `packages/shared-ui`: base layout components used by both driver apps
- [ ] Verify all three clients run without errors
- [ ] Update `docs/mvp-1/STATUS.md` → Status: Complete

---

## MVP-2 — OSM Import & Nearby

### Pre-Sprint 2.0 — Documentation
- [x] Write `gis-spec.md` (OSM importer contract, nearby function spec)
- [x] Write `api-contracts.md` — GIS Service `/api/v1/nearby`, `/api/v1/stations/{id}`, `/api/v1/internal/cache/invalidate`
- [x] Write `error-catalog.md` — MVP-2 scope (GIS/nearby errors)
- [x] Update `docs/mvp-2/STATUS.md`

### Sprint 2.1 — OSM Importer
- [ ] `infra/osm-importer/` docker-compose service scaffold
- [ ] OSM data fetch script (Tunisia bounding box)
- [ ] ETL: populate `gis.osm_stations` from OSM data
- [ ] ETL: populate `gis.osm_roads` from OSM data
- [ ] ETL: populate `gis.osm_cities` from OSM data
- [ ] GIST spatial indexes on all geometry columns
- [ ] Importer run documented in `infra/osm-importer/README.md`
- [ ] Update `docs/mvp-2/STATUS.md`

### Sprint 2.2 — GIS Service & Redis Cache
- [ ] `gis.nearby()` SQL function (ST_DWithin, geography, WGS84)
- [ ] Unit test for `gis.nearby()` with known fixture coordinates
- [ ] Scaffold GIS Service (Rust/Actix-web, `/api/v1/nearby`, `/api/v1/stations/{id}`)
- [ ] GIS Service repository layer calling `gis.nearby()`
- [ ] Redis container in docker-compose
- [ ] GIS Service: cache `gis.nearby()` results in Redis (key: `nearby:{lat}:{lon}:{radius}`, TTL 120s)
- [ ] GIS Service: cache `station:{id}` in Redis (TTL 300s)
- [ ] Cache bust on station/charger writes (internal endpoint, Docker network only)
- [ ] Cache TTL policy defined
- [ ] Integration test for `/api/v1/nearby` endpoint
- [ ] Error handling: invalid coordinates, out-of-range radius, cache degraded mode
- [ ] Update `docs/mvp-2/STATUS.md`

### Sprint 2.3 — Map Markers on Driver Apps
- [ ] `packages/api-client`: typed client for `/api/v1/nearby`
- [ ] `packages/shared-hooks`: `useNearby(lat, lon, radius)` hook
- [ ] `mobile-driver`: render station markers from `useNearby` on map
- [ ] `web-driver`: render station markers from `useNearby` on Leaflet map
- [ ] Marker visual differentiation: commercial vs private home station
- [ ] Update `docs/mvp-2/STATUS.md` → Status: Complete

---

## MVP-3 — Keycloak Auth

### Pre-Sprint 3.0 — Documentation
- [x] Write `auth-flows.md` (all flows: driver register/login, social login, partner invite, partner approval, token refresh)
- [x] Update `api-contracts.md` — Auth Service endpoints (register, partner invite/approve, users/me)
- [x] Update `error-catalog.md` — auth errors
- [x] Update `docs/mvp-3/STATUS.md`

### Sprint 3.1 — Keycloak Setup via kcadm
- [ ] Keycloak container in docker-compose with `keycloak_db`
- [ ] `infra/keycloak/` kcadm scripts: create `bornemap-drivers` realm
- [ ] `infra/keycloak/` kcadm scripts: create `bornemap-staff` realm
- [ ] Configure Google IdP in `bornemap-drivers` realm
- [ ] Configure Facebook IdP in `bornemap-drivers` realm
- [ ] Driver self-registration enabled, auto-validated in `bornemap-drivers`
- [ ] Partner/Admin roles defined in `bornemap-staff` realm
- [ ] `.env.example` updated with Keycloak vars
- [ ] Update `docs/mvp-3/STATUS.md`

### Sprint 3.2 — Auth Service & JWT Validation
- [ ] Auth Service: JWKS-based JWT validation middleware (shared across services)
- [ ] Auth Service: `POST /api/v1/auth/register` (driver, creates `users` profile on first login)
- [ ] Auth Service: user profile creation on Keycloak post-login hook
- [ ] Shared JWT validation middleware extracted to `crates/` or shared module
- [ ] Integration tests for token validation
- [ ] Update `docs/mvp-3/STATUS.md`

### Sprint 3.3 — Partner Invite & Approval Flows
- [ ] Auth Service: `POST /api/v1/admin/partners/invite` — create partner + Keycloak user via admin API
- [ ] Auth Service: `POST /api/v1/admin/partners/:id/approve` — approve self-registered partner
- [ ] Auth Service: `POST /api/v1/admin/partners/:id/reject` — reject self-registered partner
- [ ] Partner self-registration flow on dashboard (restricted access until approved)
- [ ] Email invite link flow (Keycloak-managed)
- [ ] Integration tests for invite + approval flows
- [ ] Update `docs/mvp-3/STATUS.md`

### Sprint 3.4 — Auth Integration in Client Apps
- [ ] `mobile-driver`: login/register screens (username/password + social login buttons)
- [ ] `web-driver`: login/register screens
- [ ] `dashboard`: login screen (`bornemap-staff` realm)
- [ ] `packages/shared-hooks`: `useAuth()` hook (token storage, refresh, logout)
- [ ] `packages/api-client`: attach JWT to authenticated requests
- [ ] Protected routes in dashboard (redirect to login if unauthenticated)
- [ ] Update `docs/mvp-3/STATUS.md` → Status: Complete

---

## MVP-4 — Partner/Admin Dashboard

### Pre-Sprint 4.0 — Documentation
- [x] Write `ui-screens.md` — all screens (mobile driver, web driver, dashboard: StationsList/StationDetail/StationForm, PartnerList/PartnerDetail/PartnerForm, PendingApprovals, AnalyticsDashboard, Settings, error screens)
- [x] Update `api-contracts.md` — Admin Service CRUD endpoints + partner endpoints + analytics
- [x] Update `error-catalog.md` — dashboard/admin errors (OPR_*, STA_* admin)
- [x] Update `docs/mvp-4/STATUS.md`

### Sprint 4.1 — Partner Management (Admin)
- [ ] Admin Service: `GET /api/v1/admin/partners` (list, paginated)
- [ ] Admin Service: `GET /api/v1/admin/partners/:id`
- [ ] Admin Service: `PATCH /api/v1/admin/partners/:id`
- [ ] Admin Service: `DELETE /api/v1/admin/partners/:id` (soft delete)
- [ ] Dashboard: partner list screen (sortable, filterable table)
- [ ] Dashboard: partner detail/edit screen
- [ ] Audit log write on every partner mutation
- [ ] Integration tests for partner CRUD
- [ ] Update `docs/mvp-4/STATUS.md`

### Sprint 4.2 — Station Management
- [ ] Driver Service: `POST /api/v1/stations` (create, partner-scoped)
- [ ] Driver Service: `GET /api/v1/partner/stations` (partner's own list)
- [ ] Driver Service: `GET /api/v1/partner/stations/:id` (partner's own detail)
- [ ] Driver Service: `PATCH /api/v1/stations/:id` (partner-scoped)
- [ ] Driver Service: `DELETE /api/v1/stations/:id` (soft delete)
- [ ] Admin Service: `GET /api/v1/admin/stations` (cross-partner list)
- [ ] Admin Service: `GET /api/v1/admin/stations/:id` (cross-partner detail)
- [ ] Admin Service: `PATCH /api/v1/admin/stations/:id/status` (admin override)
- [ ] Ownership check on all Driver Service write endpoints (partner_id must match JWT claim)
- [ ] Cache bust on station create/update/delete
- [ ] Dashboard: station list screen per partner
- [ ] Dashboard: station detail screen
- [ ] Dashboard: station create/edit form with map coordinate picker
- [ ] Audit log write on every station mutation
- [ ] Integration tests for station CRUD + ownership checks
- [ ] Update `docs/mvp-4/STATUS.md`

### Sprint 4.3 — Charger Management
- [ ] Driver Service: `POST /api/v1/stations/:id/chargers`
- [ ] Driver Service: `PATCH /api/v1/stations/:station_id/chargers/:charger_id`
- [ ] Driver Service: `DELETE /api/v1/stations/:station_id/chargers/:charger_id` (soft delete)
- [ ] Dashboard: charger list inline in station detail view
- [ ] Dashboard: charger add/edit modal (connector type, power kw, status)
- [ ] Audit log write on every charger mutation
- [ ] Integration tests for charger CRUD
- [ ] Update `docs/mvp-4/STATUS.md` → Status: Complete

---

## MVP-5 — Analytics (Admin Service)

### Pre-Sprint 5.0 — Documentation
- [x] Update `api-contracts.md` — analytics endpoints in Admin Service (overview, sessions, energy, revenue)
- [x] Update `docs/mvp-5/STATUS.md`

### Sprint 5.1 — Event Tracking
- [ ] Define analytics event schema in `analytics_db` (`raw_events` table)
- [ ] Admin Service: write events on station/charger/partner mutations
- [ ] Driver Service: write events on `/nearby` calls (anonymous, no PII)
- [ ] Update `docs/mvp-5/STATUS.md`

### Sprint 5.2 — Reporting Endpoints & Dashboard
- [ ] Admin Service: `GET /api/v1/admin/analytics/overview` (summary stats)
- [ ] Admin Service: `GET /api/v1/admin/analytics/stations` (station counts, activity)
- [ ] Admin Service: `GET /api/v1/admin/analytics/searches` (nearby query volume)
- [ ] Admin Service: `GET /api/v1/admin/analytics/sessions` (charging sessions)
- [ ] Admin Service: `GET /api/v1/admin/analytics/energy` (energy dispensed)
- [ ] Admin Service: `GET /api/v1/admin/analytics/revenue` (revenue metrics, stub)
- [ ] Dashboard: analytics screen with charts (recharts, date range picker)
- [ ] Integration tests for analytics endpoints
- [ ] Update `docs/mvp-5/STATUS.md` → Status: Complete

---

## MVP-6 — Production Hardening

### Pre-Sprint 6.0 — Documentation
- [x] Write `docker-compose-map.md` (all containers, Traefik, Redis, full network map)
- [x] Update `env-vars.md` (production vars)
- [ ] Create ADR: production topology decisions
- [x] Update `docs/mvp-6/STATUS.md`

### Sprint 6.1 — Traefik Gateway
- [ ] Traefik container in docker-compose (TLS termination, routing rules)
- [ ] Route all service traffic through Traefik
- [ ] CORS configured per service via Traefik middleware
- [ ] Health checks routed correctly
- [ ] Update `docs/mvp-6/STATUS.md`

### Sprint 6.2 — Observability & Deployment
- [ ] Structured logging across all services (JSON format, log level via env var)
- [ ] Docker-compose health checks on all containers
- [ ] Startup dependency ordering (DB ready before services start)
- [ ] Root `README.md` — project overview, quickstart, monorepo map
- [ ] Final `api-reference/` docs generated per service
- [ ] `db/MIGRATIONS.md` finalized
- [ ] Update `docs/mvp-6/STATUS.md` → Status: Complete
