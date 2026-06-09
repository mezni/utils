# Implementation Plan — BorneMap

**Version:** 1.0
**Status:** Approved
**Last updated:** 2026-06-09

---

## Delivery Philosophy

Build in MVP cycles. Each MVP is complete and deployable. Validate the product before adding infrastructure. The database schema is designed once with the full platform in mind — each MVP activates a subset. Frontend apps are built once and connected progressively to better backends.

---

## MVP Map

| MVP | Goal | Key Output |
|---|---|---|
| MVP-1 | Core loop — json-server + Dashboard + driver apps | Mock API, admin + partner dashboard, map discovery |
| MVP-2 | Real backend — Rust + PostgreSQL + PostGIS | Spatial queries, Docker Compose, CI/CD |
| MVP-3 | Auth + users + RTL | Keycloak, JWT, role-based access, i18n |
| MVP-4 | GIS sync | OSM, trigger-based sync, spatial GIS layer |
| MVP-5 | Analytics | Event tracking, aggregates, reporting |
| MVP-6 | Production + launch | Traefik, TLS, all features, launch-ready |

---

## MVP-1 — Core Product Loop

**Goal:** An admin creates and manages partners, stations, and chargers. A partner manages their own stations and chargers. A driver finds nearby stations on a map. The full loop works end to end using json-server as the mock backend.

**Stack:** json-server, React + Vite (Dashboard), React + Vite (Driver Web), React Native + Expo SDK 54 (Driver Mobile).

**Build order:** Mock API + Design System → Dashboard (admin view) → Dashboard (partner view) → Driver Web → Driver Mobile → Hardening.

### MVP-1 Scope

**In scope:**
- json-server mock API with db.json containing seeded data and routes.json for /api prefix
- Design token package — full color, typography, spacing, radius, shadow tokens
- Dashboard App — admin view with Overview, Partners, Stations, Chargers screens
- Dashboard App — partner view with Overview, My Stations, My Chargers, Availability screens
- Dev role switcher in sidebar to toggle between admin and partner view
- Driver Web App — Map screen and Station Detail screen
- Driver Mobile App — Map screen and Station Detail screen
- Partner entity with type (business/personal), is_verified, is_live, is_active, full audit fields

**Out of scope:**
- Authentication of any kind
- Favorites, reviews, profile
- Real database or migrations
- PostGIS or spatial math
- GIS sync
- Analytics
- Docker Compose, CI/CD, Traefik
- RTL and i18n
- Production deployment

### MVP-1 Sprints

#### Sprint 1.1 — Mock API and Design System Foundation

**Duration:** 1 week
**Goal:** json-server runs with seeded data under /api prefix. Design tokens defined and consumable by all apps.

**Tasks:**

*json-server setup:*
- Initialize source/mock/ folder
- Write source/mock/db.json with four resources: partners, stations, chargers, station_availability
- Partner objects include: id, name, type, is_verified, is_live, is_active, created_at, created_by, updated_at, updated_by
- Station objects include: id, partner_id, name, address, latitude, longitude, created_at, created_by, updated_at, updated_by
- Charger objects include: id, station_id, connector_type, power_kw, status, created_at, created_by, updated_at, updated_by
- Station_availability objects include: id, station_id, status, updated_by, updated_at
- Seed with 3 partners in distinct states, 15 stations across Tunisian cities, 24 chargers, 15 availability records
- Write source/mock/routes.json mapping /api/* to /$1
- Verify all resources reachable under /api prefix
- Verify filter by partner_id on stations works
- Verify filter by station_id on chargers works

*Design tokens:*
- Initialize source/packages/ui/ as pnpm package
- Create all token files: colors.ts, typography.ts, spacing.ts, radius.ts, shadows.ts, native.ts, index.ts
- Create tailwind.config.base.js extending all tokens

*pnpm workspace:*
- pnpm-workspace.yaml listing source/apps/* and source/packages/*
- Root package.json with scripts: mock, dev:dashboard, dev:web, dev:mobile, dev

**Done when:**
- pnpm mock starts json-server with all resources reachable
- GET /api/partners returns 3 partners with all fields including type and flags
- GET /api/stations?partner_id=1 returns only partner 1 stations
- GET /api/chargers?station_id=1 returns only station 1 chargers
- Token files compile without TypeScript errors
- tailwind.config.base.js resolves brand.primary to #007943

---

#### Sprint 1.2 — Dashboard Admin View

**Duration:** 2 weeks
**Goal:** Dashboard App admin view is fully functional for managing partners, stations, and chargers.

**Tasks:**

*Project setup:*
- Initialize source/apps/dashboard/ with Vite + React + TypeScript + Tailwind
- Configure Tailwind extending shared token base
- Set up React Router with routes for both admin and partner views
- Configure VITE_API_BASE_URL=http://localhost:3001

*AppShell component:*
- Fixed left sidebar w-64, surface.sidebar, border-r border.default
- Brand header — lightning bolt icon in brand.primary, BorneMap name, role label
- NavigationItem — icon, label, active state bg brand.sageLight text brand.primary
- Bottom area — dev role switcher toggle (Admin / Partner) and partner selector dropdown
- TopBar — h-16, white, border-b, page title, placeholder avatar
- PageContent — flex-1, overflow-y-auto, p-6, bg surface.background

*Dev role switcher:*
- Toggle button at sidebar bottom: "Admin View" / "Partner View"
- When Partner View active: partner selector dropdown appears showing all partners
- Selected partner context used to filter all partner view queries
- State stored in React context, not in URL or localStorage
- Clearly labeled "Dev Only — removed in MVP-3"

*Shared components:*
- StatCard, DataTable, StatusBadge, Modal, EmptyState, ErrorState, Skeleton, Button, Input

*Admin Overview screen:*
- StatCards: total partners, total stations, total chargers
- Recent stations table with name, partner name, charger count

*Admin Partners screen:*
- DataTable columns: name, type badge (business/personal), verified badge, live badge, active toggle, actions
- Add Partner — Modal with name field and type select (Business / Personal)
- is_verified, is_live, is_active default to false, false, true on creation
- Verify action — button visible only when is_verified = false, sets is_verified to true
- If verifying also sets is_live to true when partner has stations — note in UI
- Deactivate / Reactivate — toggles is_active
- Edit — Modal with name and type fields
- Delete — confirmation Modal

*Admin Stations screen:*
- DataTable columns: name, address, partner name, charger count, actions
- Partner filter dropdown — all partners
- Add Station — Modal with name, address, lat, lng, partner select
- Lat validation: -90 to 90 — field error if invalid
- Lng validation: -180 to 180 — field error if invalid
- Edit and Delete

*Admin Chargers screen:*
- DataTable columns: station name, connector type, power kw, status badge, actions
- Station filter dropdown — all stations
- Add Charger — Modal with station select, connector type select, power kw, status select
- Edit and Delete

*Error handling on all screens:*
- API offline — ErrorState with retry
- Empty data — EmptyState with create prompt
- Form required fields — inline validation before submit

**Done when:**
- Partner created with type, flags visible in table
- Verify action sets is_verified = true and reflects in badge
- Deactivate toggles is_active and reflects in table
- Station and charger CRUD working with correct partner/station names
- Overview stat cards show real counts
- ErrorState when json-server stopped

---

#### Sprint 1.3 — Dashboard Partner View

**Duration:** 1 week
**Goal:** Dashboard App partner view is functional for a partner managing their own data.

**Tasks:**

*Partner sidebar navigation:*
- Overview, My Stations, My Chargers, Availability
- Uses selected mock partner from dev role switcher context

*Partner Overview screen:*
- StatCards: own stations count, own chargers count, available chargers count
- Status bar at top: Verified / Awaiting Verification, Live / Not Live, Active / Suspended — driven by the selected mock partner's flags
- Own stations table with name, charger count, availability status

*Partner My Stations screen:*
- DataTable showing only stations where partner_id matches selected mock partner
- Add Station — partner_id pre-filled and locked to the mock partner
- Edit own station
- Delete own station

*Partner My Chargers screen:*
- DataTable showing only chargers belonging to the mock partner's stations
- Station filter scoped to own stations only
- Add Charger — station select shows only own stations
- Edit charger (primary use: status update)
- Delete charger

*Partner Availability screen:*
- Table of own stations with current availability status (from station_availability resource)
- Three-option quick toggle per station: Available / Partial / Unavailable
- Toggle POSTs a new row to station_availability resource
- Current status shown is the latest entry per station

**Done when:**
- Switching to Partner View in dev switcher and selecting partner 1 shows only partner 1 stations and chargers
- Switching to partner 2 shows only partner 2 data
- Partner status bar reflects the selected partner's flags
- Availability toggle creates a new station_availability record in json-server
- Partner cannot see other partners' data through the UI

---

#### Sprint 1.4 — Driver Web App

**Duration:** 2 weeks
**Goal:** Driver Web App shows a Leaflet map with real station markers from json-server and navigates to station detail.

**Tasks:**

*Project setup:*
- Initialize source/apps/driver-web/ with Vite + React + TypeScript + Tailwind + Leaflet
- Configure Tailwind extending shared token base
- Set up React Router: / and /stations/:id
- Configure VITE_API_BASE_URL=http://localhost:3001

*Driver-specific components:*
- StationCard, ChargerRow, ZoomControls

*Map screen:*
- Full-bleed Leaflet MapContainer, height calc(100vh - 56px)
- OpenStreetMap tiles
- Initial center Tunisia (33.8869, 9.5375), zoom 7
- Fetch all stations and chargers from API on mount
- Filter out stations belonging to partners where is_active = false OR is_verified = false OR is_live = false
- Compute available_count per station client-side from charger list
- Compute distance_km from Tunisia center using Haversine formula
- CircleMarker per visible station: radius 8, white border weight 2, fillColor brand.glow (#00E676) if available_count > 0, fillColor status.maintenance (#EF4444) if available_count = 0
- Popup on click — name, address, available/total, View Detail link
- Floating TopBar — BorneMap brand name in brand.primary
- Loading and error states in TopBar

*Station Detail screen:*
- Fetch station by ID and its chargers
- Back button to map
- Station name, address
- ChargerRow list with StatusBadge
- Loading skeleton and ErrorState

**Done when:**
- Map loads with stations belonging only to verified, live, active partners
- Partner 3 (is_verified: false) stations do not appear on map
- Marker colors reflect availability
- Station Detail shows real charger data
- Changing charger status in Dashboard → reload Driver Web shows updated color

---

#### Sprint 1.5 — Driver Mobile App

**Duration:** 2 weeks
**Goal:** Driver Mobile App shows a map with real station markers from json-server on iOS and Android.

**Tasks:**

*Project setup:*
- Initialize source/apps/driver-mobile/ with Expo SDK 54
- Configure expo-router with stack navigator
- Add react-native-maps 1.18.0, expo-location ~18.0.0
- Import native tokens from source/packages/ui/src/tokens/native.ts
- Configure EXPO_PUBLIC_API_BASE_URL=http://localhost:3001

*Map screen:*
- Request expo-location foreground permission on mount
- If granted: use device coordinates
- If denied: use Tunisia center (33.8869, 9.5375) — no error, no crash
- Fetch all stations and chargers from API
- Filter out stations from unverified, inactive, or not-live partners client-side
- Compute available_count per station client-side
- MapView full-bleed, PROVIDER_DEFAULT
- Marker per visible station: pinColor #00E676 if available_count > 0, pinColor #EF4444 if available_count = 0
- Callout with station name and available/total
- Tap callout navigates to Station Detail
- SafeAreaView header with BorneMap brand name
- ActivityIndicator while fetching, error text on failure

*Station Detail screen:*
- Fetch station and chargers
- ScrollView — name, address
- Charger list with status as colored text
- ActivityIndicator while fetching, error text on failure

**Done when:**
- Map loads on iOS simulator and Android emulator
- Only stations from verified, live, active partners shown
- Marker colors correct
- Location denied uses Tunisia center — no crash
- Station Detail shows charger list

---

#### Sprint 1.6 — Integration and Hardening

**Duration:** 1 week
**Goal:** Full product loop verified. Edge cases handled. Documentation complete.

**Tasks:**

*Full loop verification:*
- Admin creates partner (type: business) → partner appears in admin table
- Admin verifies partner → is_verified badge turns green
- Admin sets is_live → partner stations become visible on driver apps
- Partner creates station → appears in partner's My Stations
- Partner creates chargers → appear in partner's My Chargers
- Partner updates charger status to maintenance → marker turns red on both driver apps on reload
- Partner updates availability to partial → visible on Availability screen
- Admin deactivates partner → partner stations disappear from driver apps on reload
- Admin deletes station → disappears from both driver apps

*Fix sweep:*

json-server:
- All CRUD verified on all four resources
- Filter queries verified

Dashboard admin view:
- All forms validated — empty required fields show errors
- Lat/lng out of range shows field errors
- ErrorState on all screens when API offline
- Chrome, Firefox, Safari

Dashboard partner view:
- Switching between partners shows correct scoped data
- Partner cannot reach other partner data through URL manipulation — note that this is dev-only, full enforcement in MVP-3

Driver Web:
- Unverified partner stations not shown — verified by checking partner 3 stations are absent
- API offline — ErrorState
- Chrome, Firefox, Safari

Driver Mobile:
- Same visibility rule verified
- Location denied — no crash
- API offline — error text, no crash
- iOS simulator and Android emulator

*Decision to record:*
- What happens when admin deletes a partner that has stations — cascade or block. Record in docs/project/decisions.md.

*Documentation:*
- Write docs/guides/onboarding.md
- Write docs/api/mock-api.md — all resources, fields, filter params, json-server limitations
- Write docs/project/phases/mvp-01-status.md
- Record any decisions in docs/project/decisions.md

**Done when:**
- Full loop verified end to end
- Partner visibility rules enforced in both driver apps
- All three apps handle API offline gracefully
- Onboarding guide tested from scratch
- Documentation complete
- Zero Class A bugs

### MVP-1 Done Criteria

- [ ] json-server starts with pnpm mock
- [ ] All four resources reachable under /api prefix
- [ ] Partner objects contain type, all three flags, full audit fields
- [ ] Dashboard admin — partner CRUD with type and flag management
- [ ] Dashboard admin — verify action sets is_verified
- [ ] Dashboard admin — deactivate/reactivate toggles is_active
- [ ] Dashboard admin — station and charger CRUD
- [ ] Dashboard partner — scoped to selected mock partner only
- [ ] Dashboard partner — status bar reflects partner flags
- [ ] Dashboard partner — availability toggle creates new record
- [ ] Driver Web — only verified/live/active partner stations shown
- [ ] Driver Web — marker colors correct
- [ ] Driver Web — station detail with charger list
- [ ] Driver Mobile — only verified/live/active partner stations shown
- [ ] Driver Mobile — works on iOS simulator and Android emulator
- [ ] Driver Mobile — location denied handled gracefully
- [ ] Full loop: admin create → partner manage → driver discovers
- [ ] Partner deactivated → stations disappear from driver apps
- [ ] All apps handle API offline gracefully
- [ ] Onboarding guide tested from scratch
- [ ] API documentation complete
- [ ] Zero Class A bugs

---

## MVP-2 — Rust Services + PostgreSQL + PostGIS + CI/CD

**Goal:** Replace json-server with Rust services backed by PostgreSQL + PostGIS. Introduce Docker Compose and GitHub Actions. Frontend apps update base URLs.

### MVP-2 Scope

**In scope:**
- Cargo workspace at source/ root
- ev-core — NanoID generation, shared enums
- ev-db — PgPool, pagination structs
- Driver Service — public discovery with ST_DWithin spatial query
- Admin Service — full CRUD including partner type, flags, audit fields
- PostgreSQL + PostGIS schema migrations
- Dev seeds replacing db.json
- Docker Compose stack
- GitHub Actions CI — six path-scoped workflows
- Frontend apps update base URLs

**Out of scope:**
- Authentication (MVP-3)
- GIS schema population (MVP-4)
- Analytics (MVP-5)
- Traefik (MVP-6)

### MVP-2 Sprints

#### Sprint 2.1 — Cargo Workspace and Shared Crates

Initialize workspace. Build ev-core with all NanoID functions and type enums. Build ev-db with PgPool and pagination. Unit tests pass. cargo build --all succeeds.

#### Sprint 2.2 — Database Schema

PostgreSQL + PostGIS migrations 0001–0004. Partner table with type, flags, audit fields. Station table with lat/lng constraints. Charger table with connector type and power constraints. All CHECK constraints verified. Spatial index on station coordinates. Dev seeds in database/seeds/ replacing db.json.

#### Sprint 2.3 — Driver Service

Config from env, AppState, AppError, health endpoint. GET /api/stations/nearby with ST_DWithin. GET /api/stations/markers with bbox. GET /api/stations/search with text and connector filter. GET /api/stations/:id with charger list. GET /api/stations/:id/reviews stub. All queries JOIN on inventory.partner to enforce visibility rule (is_active AND is_verified AND is_live). Integration tests with test database. Dockerfile.

#### Sprint 2.4 — Admin Service

Health endpoint. Full partner CRUD — including type, flags (verify, activate, deactivate), audit field writes. Full station CRUD. Full charger CRUD. Availability update endpoint. Dev X-Partner-Id header for scope testing. Integration tests including partner flag updates and scope filtering. Dockerfile.

#### Sprint 2.5 — Docker Compose and CI/CD

Docker Compose with health checks and depends_on. Six GitHub Actions path-scoped workflows. Both services run sqlx::migrate! on startup. Frontend apps update API base URLs to point to Rust services.

#### Sprint 2.6 — MVP-2 Hardening

cargo test --all passes. cargo clippy --all-targets -- -D warnings clean. Docker Compose starts from zero cleanly. ST_DWithin confirmed by EXPLAIN ANALYZE. Visibility rule (partner flags JOIN) confirmed in integration tests. Full loop verified with Rust services. CI green on main branch.

### MVP-2 Done Criteria

- [ ] cargo build --all with zero warnings
- [ ] cargo test --all passes
- [ ] Docker Compose starts cleanly from zero
- [ ] Partner type and flags managed via Admin Service
- [ ] Visibility rule enforced in Driver Service queries
- [ ] ST_DWithin confirmed by EXPLAIN ANALYZE
- [ ] All CI workflows pass
- [ ] Full loop verified with Rust services
- [ ] Zero Class A bugs

---

## MVP-3 — Authentication and User Management

**Goal:** Keycloak issues JWTs. Both services enforce auth. Users schema live. Partner scope enforced by JWT. Dev role switcher removed. RTL and i18n in all apps.

### MVP-3 Sprints

#### Sprint 3.1 — Keycloak Setup

Keycloak 24 in Docker Compose with PostgreSQL backend. ev-platform realm. Three roles. Google and Facebook login. partner_id claim mapper. Realm export auto-import on container start.

#### Sprint 3.2 — Users Schema and ev-auth Crate

Migrations 0005–0008. All five users tables with constraints. ev-auth crate — JWT validation, Claims struct, Role enum, JWKS cache. Unit tests. First-login provisioning function.

#### Sprint 3.3 — JWT Middleware in Both Services

OptionalAuth, RequiredAuth in Driver Service. PartnerOrAdminAuth, AdminOnlyAuth in Admin Service. Replace X-Partner-Id header with real JWT scope. Protected stubs return 501 — auth enforced, business logic deferred. Integration tests for all auth scenarios.

#### Sprint 3.4 — Auth in Driver Web App

packages/auth-client. authStore with silent refresh. silent-check-sso.html. UpgradeModal. useRequireAuth hook. Token verified not in localStorage.

#### Sprint 3.5 — Auth in Driver Mobile App

expo-auth-session with PKCE. expo-secure-store. Silent restore on relaunch. Deep link callback. Upgrade modal. Token verified not in AsyncStorage.

#### Sprint 3.6 — Dashboard Role-Based Auth and Dev Switcher Removal

Dashboard auth store with Keycloak JS. Role-aware sidebar — partner sees partner nav, admin sees admin nav. Remove dev role switcher and partner selector dropdown entirely. registered_driver token redirected out. Logout clears token.

#### Sprint 3.7 — RTL and i18n

Arabic and French in all three apps. RTL layout on all screens. Language switching without reload.

#### Sprint 3.8 — MVP-3 Hardening

Full auth loop on clean environment. Security checklist. RTL audit all screens. Dev switcher confirmed absent from codebase. All integration tests pass.

### MVP-3 Done Criteria

- [ ] Keycloak imports realm cleanly on fresh stack
- [ ] Google login works in all three apps
- [ ] JWT middleware enforces auth in both services
- [ ] Partner scope enforced by JWT partner_id claim
- [ ] Dev role switcher completely removed from codebase
- [ ] No tokens in localStorage or AsyncStorage
- [ ] RTL correct on all screens in Arabic
- [ ] Zero Class A bugs

---

## MVP-4 — GIS Synchronization

**Goal:** Station coordinates sync to GIS layer via PostgreSQL trigger. Driver Service uses real PostGIS spatial queries against the GIS layer.

### MVP-4 Sprints

#### Sprint 4.1 — OSM Import

Tunisia OSM extract from Geofabrik. osm2pgsql import. Derive roads, boundaries, amenity_points. Verify spatial queries. Write infra/osm/import.sh.

#### Sprint 4.2 — GIS Trigger

gis.sync_station() function. trg_station_gis_sync trigger. gis.resync_all_stations() procedure. GIS failure confirmed not blocking station write. Resync on all seeds verified.

#### Sprint 4.3 — Driver Service GIS Integration

Nearby and markers endpoints use gis.station_locations. Region filter on search. EXPLAIN ANALYZE confirms index usage on spatial queries.

#### Sprint 4.4 — MVP-4 Hardening

Full sync loop verified. OSM import documented. Schema documentation updated.

### MVP-4 Done Criteria

- [ ] OSM import runs without errors
- [ ] Trigger fires on station insert/update/delete
- [ ] GIS failure does not block station write
- [ ] Resync runs cleanly on all seeds
- [ ] Nearby uses GIS layer — confirmed by EXPLAIN ANALYZE
- [ ] Zero Class A bugs

---

## MVP-5 — Analytics and Reporting

**Goal:** All apps fire events. Clickstream Service persists to PostgreSQL. Admin reporting shows real data.

### MVP-5 Sprints

#### Sprint 5.1 — Analytics Schema and Clickstream Service

Migrations 0014–0016. Clickstream Service — validate and insert. packages/api-client-events — fire-and-forget, swallows all errors silently. All apps instrumented with canonical taxonomy events.

#### Sprint 5.2 — Aggregates and Reporting

Aggregate SQL jobs. Admin Service reporting endpoints. Admin Dashboard reports screen with real data. Partner Dashboard reports screen with partner-scoped data.

#### Sprint 5.3 — MVP-5 Hardening

100 events fired and verified. Duplicates blocked. Aggregate jobs verified. Event taxonomy document finalized and matches Clickstream Service validation list.

### MVP-5 Done Criteria

- [ ] Events fire from all three apps
- [ ] All events in analytics.raw_events
- [ ] Duplicate events not double-inserted
- [ ] Aggregate jobs running and accurate
- [ ] Admin and partner reporting show real data
- [ ] Event taxonomy document matches service validation list
- [ ] Zero Class A bugs

---

## MVP-6 — Production Hardening and Launch

**Goal:** Platform is production-safe, fully tested, and deployable on a real server.

### MVP-6 Sprints

#### Sprint 6.1 — Favorites Feature

Driver Service endpoints. Driver Web and Mobile favorites screens. Upgrade modal for anonymous users.

#### Sprint 6.2 — Reviews Feature

Driver Service review endpoints. Admin Service moderation endpoints. Review UI in driver apps. Moderation in Dashboard.

#### Sprint 6.3 — Profile Management

Driver Service profile endpoints. Profile screens in both driver apps. Language preference persists via user_profile.

#### Sprint 6.4 — Traefik Configuration

traefik.yml with Let's Encrypt. docker-compose.prod.yml with labels. HTTP to HTTPS redirect. Rate limiting. No direct port exposure for any service.

#### Sprint 6.5 — Security and Performance Hardening

Security audit against checklist. Load test 50 concurrent users. Query tuning. Driver Web Lighthouse Performance > 80. Bundle size audit.

#### Sprint 6.6 — Launch Readiness

Backup drill on production database. Deployment runbook tested on clean server. Final RTL audit on production. Accessibility audit — no Critical violations. Cross-browser test. Launch checklist all items checked.

### MVP-6 Done Criteria

- [ ] Traefik routes all traffic, TLS working
- [ ] No service accessible directly bypassing Traefik
- [ ] Security checklist complete
- [ ] All endpoints under 300ms at 50 concurrent users
- [ ] Favorites, reviews, profile complete
- [ ] Backup procedure tested
- [ ] Deployment runbook works on clean server
- [ ] RTL correct in production
- [ ] No WCAG Critical violations
- [ ] Zero Class A bugs

---

## Sprint Quick Reference

**MVP-1:** 1.1 Mock API + Tokens · 1.2 Dashboard Admin View · 1.3 Dashboard Partner View · 1.4 Driver Web · 1.5 Driver Mobile · 1.6 Hardening

**MVP-2:** 2.1 Cargo Workspace · 2.2 Database Schema · 2.3 Driver Service · 2.4 Admin Service · 2.5 Docker + CI/CD · 2.6 Hardening

**MVP-3:** 3.1 Keycloak · 3.2 Users + ev-auth · 3.3 JWT Middleware · 3.4 Web Auth · 3.5 Mobile Auth · 3.6 Dashboard Auth + Remove Switcher · 3.7 RTL + i18n · 3.8 Hardening

**MVP-4:** 4.1 OSM Import · 4.2 GIS Trigger · 4.3 Driver Service GIS · 4.4 Hardening

**MVP-5:** 5.1 Clickstream · 5.2 Aggregates + Reporting · 5.3 Hardening

**MVP-6:** 6.1 Favorites · 6.2 Reviews · 6.3 Profile · 6.4 Traefik · 6.5 Security + Performance · 6.6 Launch Readiness
