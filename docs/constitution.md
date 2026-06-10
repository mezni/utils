# Project Constitution — BorneMap

**Version:** 1.0
**Status:** Approved
**Last updated:** 2026-06-10

---

## 1. Purpose

BorneMap is an EV station discovery and management platform for Tunisia.
It serves four user types:

- **Public Driver** — browses stations without login
- **Registered Driver** — authenticated driver with favorites, reviews, and profile
- **Partner** — manages own stations and chargers through a dashboard
- **Admin** — manages the entire platform globally

### Current platform scope

- Public station discovery — map, nearby, search, detail
- Partner and admin management — stations, chargers, partners
- Manual availability updates
- GIS synchronization via PostgreSQL triggers
- Clickstream analytics

### Explicitly out of scope across all MVPs

- OCPP and charging sessions
- Payments and billing
- Routing and navigation
- Real-time availability (OCPP-driven)
- Push notifications

These are permanently deferred. See [docs/out-of-scope-registry.md](out-of-scope-registry.md).

---

## 2. Core Principles

**Principle 1 — MVP-first delivery**
Build the minimum that proves the core loop works. Validate before adding complexity. Never introduce infrastructure the current MVP does not need. Each MVP is complete and deployable on its own.

**Principle 2 — Layered complexity**
Each MVP adds one layer on top of a stable foundation. Nothing from a previous MVP is broken by a later one. Infrastructure replaces itself cleanly — Rust replaces json-server, not a different data model.

**Principle 3 — Dashboard first**
The Dashboard App is always built before the driver apps within any MVP that introduces new data entities. Data must exist before discovery is meaningful.

**Principle 4 — Single source of truth**
Every entity has exactly one authoritative owner. No ambiguity about where data is written or read from. All other representations are derived.

**Principle 5 — Simple operations**
The platform must be operable by one person. Every operational task must have a documented runbook. Complexity that cannot be operated simply is not acceptable.

**Principle 6 — Domain separation by schema**
Business data, GIS data, user data, and analytics data are separated by PostgreSQL schema and by service responsibility. Cross-schema writes are forbidden except where explicitly permitted by this constitution.

**Principle 7 — Public access is a first-class concern**
Anonymous public browsing must always work. Authentication is never required to view stations, markers, or search results. Auth is triggered only at the moment a gated action is attempted.

**Principle 8 — RTL and Arabic are not afterthoughts**
Arabic language support and RTL layout are built from the start of MVP-3. Any screen that does not work correctly in Arabic RTL from that point forward is a Class A bug.

**Principle 9 — Visual consistency across all surfaces**
All three applications share the same design token foundation defined in `source/packages/ui`. No hardcoded visual values in application code.

**Principle 10 — API prefix consistency**
All backend endpoints are served under the `/api` prefix. This applies to json-server in MVP-1 and all Rust services from MVP-2 onward.

**Principle 11 — Tooling separation**
Code is written in SpecKit. UX and UI design is done in Impeccable. Planning, architecture, and documentation stay in this assistant. These roles do not overlap.

---

## 3. Roles and Access Model

### Public Driver

Anonymous. No login required.

**Can:**
- View nearby stations of verified, live, active partners
- View map markers
- Search and filter stations
- View station detail
- View public reviews and ratings

**Cannot:**
- Favorite stations
- Write reviews
- Access any profile

### Registered Driver

Authenticated. Keycloak role: `registered_driver`. Introduced in MVP-3.

Everything a Public Driver can do, plus:
- Manage favorites
- Create, update, delete own reviews
- View and update own profile

### Partner

Authenticated. Keycloak role: `partner`. Introduced in MVP-3.

**Can:**
- Access Dashboard App (partner view)
- View own profile and operational status
- View and manage own stations only
- View and manage own chargers only
- Update own station availability
- View own reports

**Rules:**
- A partner user belongs to exactly one partner — enforced by `users.partner_membership` PK on `user_id`
- Every partner operation is scoped to the partner's own data — enforced by JWT `partner_id` claim in service middleware
- A partner cannot read or write another partner's data

### Admin

Authenticated. Keycloak role: `admin`. Introduced in MVP-3.

**Can:**
- Access Dashboard App (admin view)
- Create and manage all partners
- Verify, activate, deactivate partners
- Manage all stations and chargers
- Moderate reviews
- Access global reporting

---

## 4. Partner Entity

A partner is an organization or individual who owns and operates EV charging stations on the BorneMap platform.

### Partner Types

- `business` — A company, organization, or commercial entity operating EV charging stations. Has a legal name and registration.
- `personal` — An individual operating one or more stations privately. Simplified onboarding, fewer required fields.

### Partner Flags

- **`is_verified`** — An admin has reviewed and approved the partner's identity. Unverified partners exist in the system but their stations are not visible to public drivers. Only admins can set this flag. Default: `false`.
- **`is_live`** — The partner has at least one station that is active and visible on the map. A verified partner with no stations is not live. A partner cannot be live without being verified — enforced at database level by `CHECK` constraint. Default: `false`.
- **`is_active`** — The account is operationally enabled. An inactive partner cannot authenticate and all their stations are hidden from public discovery. Used for suspension, offboarding, or temporary deactivation. Distinct from `is_verified`. Default: `true`.

### Partner Visibility Rule

A partner's stations are visible to public drivers only when all three conditions are true:
```
partner.is_active    = true
partner.is_verified  = true
partner.is_live      = true
```

This is enforced in Driver Service query logic via a `JOIN` on `inventory.partner` from MVP-2 onward. In MVP-1 it is enforced client-side in the driver apps.

### Partner Audit Trail

Every partner record carries a full audit trail:
- `created_at` — immutable timestamp set on insert
- `created_by` — `USR-...` of the admin who created the record, nullable
- `updated_at` — updated on every write
- `updated_by` — `USR-...` of the last user who wrote to the record, nullable

The audit trail pattern (`created_at`, `created_by`, `updated_at`, `updated_by`) applies to all inventory tables.

---

## 5. Frontend Applications

Three frontend applications:

### Driver Web App — React + Vite

Map-centric. Leaflet + OpenStreetMap. Full-bleed map layout with floating UI elements. Public and authenticated driver experience.

### Driver Mobile App — React Native + Expo SDK 54

`react-native-maps`. Full-bleed map layout. Bottom sheet pattern. Primary driver surface overall.

### Dashboard App — React + Vite

Single application serving both Partner and Admin roles. Role determined from JWT on login from MVP-3. In MVP-1 and MVP-2 a dev role switcher simulates the two views. Fixed left sidebar navigation.

### Build Order Rule

The Dashboard App is always built before the driver apps within any MVP. Data must exist before discovery is meaningful.

### Dev Role Switcher (MVP-1 and MVP-2 only)

A toggle in the sidebar bottom area allows switching between Admin view and Partner view during development. A partner selection dropdown simulates which partner is active. Both the toggle and the dropdown are removed entirely in MVP-3 Sprint 3.6 when real Keycloak auth arrives.

### Dashboard Navigation — Admin View

- Overview
- Partners
- Stations
- Chargers

### Dashboard Navigation — Partner View

- Overview
- My Stations
- My Chargers
- Availability

### Expo SDK Version

Expo SDK 54 exclusively. No upgrade without an approved ADR.

| Package | Version |
|---|---|
| React Native | 0.76.5 |
| React | 18.3.1 |
| Expo Router | ~4.0.0 |
| expo-location | ~18.0.0 |
| react-native-maps | 1.18.0 |

### Map Library Rules

- Driver Web App uses Leaflet with OpenStreetMap tiles — no API key required
- Driver Mobile App uses react-native-maps with default provider
- No paid map library or tile provider without an approved ADR

### Accessibility and Language Rules

- All web applications target WCAG 2.1 AA minimum — from MVP-3
- Arabic and French are the two supported languages — from MVP-3
- RTL layout correct on every screen in Arabic — from MVP-3
- Language switching works without a page reload

---

## 6. Design System

Defined in `source/packages/ui`. Single source of truth for all visual values across all three applications.

### 6.1 Color Tokens

| Token | Value | Usage |
|---|---|---|
| `brand.primary` | `#007943` | Primary green. CTAs, active states, links. |
| `brand.primaryDark` | `#005c32` | Darker green for gradients, pressed states. |
| `brand.sageLight` | `#EAF0E6` | Selected states, map terrain, active nav. |
| `brand.glow` | `#00E676` | Live map pin markers. Driver apps only. |
| `surface.background` | `#F8FAF6` | Page and screen canvas. |
| `surface.card` | `#FFFFFF` | Cards, panels, modals. |
| `surface.sidebar` | `#FFFFFF` | Dashboard sidebar. |
| `surface.mapTerrain` | `#EAF0E6` | Map canvas base. Driver apps only. |
| `text.main` | `#111827` | Primary text. Headings and body. |
| `text.muted` | `#6B7280` | Secondary text. Labels, metadata. |
| `border.default` | `#E5E7EB` | Standard dividers and card borders. |
| `border.subtle` | `#F3F4F6` | Light dividers inside cards. |
| `status.available` | `#10B981` | Available charger or healthy station. |
| `status.availableBg` | `#ECFDF5` | Available badge background. |
| `status.inUse` | `#F59E0B` | Station or charger in use. |
| `status.inUseBg` | `#FFFBEB` | In-use badge background. |
| `status.maintenance` | `#EF4444` | Maintenance or offline. |
| `status.maintenanceBg` | `#FEF2F2` | Maintenance badge background. |

Plus full neutral gray scale (`neutral.50` → `neutral.900`).

### 6.2 Typography Tokens

| Token | Value |
|---|---|
| `font.family.sans` | `Plus Jakarta Sans, Inter, system-ui, sans-serif` |
| `font.family.arabic` | `Cairo, system-ui, sans-serif` |

**Font sizes:** xs(10) sm(12) base(14) lg(16) xl(18) 2xl(20) 3xl(24)
**Font weights:** regular(400) medium(500) semibold(600) bold(700) extrabold(800)

Driver apps use Plus Jakarta Sans. Dashboard uses Inter. Arabic content uses Cairo.

### 6.3 Spacing, Radius, and Shadows

**Spacing base:** 4px
**Scale:** 4, 8, 12, 16, 20, 24, 32, 40, 48, 64, 80, 96

**Radius:** sm(4) md(8) lg(12) xl(16) 2xl(20) 3xl(24) full(9999)

**Shadows:**
- `shadow.card` — Subtle elevation for cards and panels
- `shadow.panel` — Medium elevation for floating panels
- `shadow.float` — High elevation for bottom sheets
- `shadow.pin` — Neon glow for live map markers using `brand.glow`

### 6.4 Token Delivery

- Web applications extend `source/packages/ui/tailwind.config.base.js`.
- Driver Mobile App imports from `source/packages/ui/src/tokens/native.ts` — plain JavaScript values for React Native StyleSheet.
- **Rule:** `native.ts` must stay synchronized with `colors.ts`. Any token added to `colors` is added to `native` in the same commit.

### 6.5 Theme Boundaries

- **Driver apps** use: all brand tokens, `surface.background`, `surface.card`, `surface.mapTerrain`, `brand.glow`, `shadow.pin`, Plus Jakarta Sans.
- **Dashboard** uses: all brand tokens, `surface.background`, `surface.card`, `surface.sidebar`, Inter. Does **not** use `brand.glow`, `surface.mapTerrain`, or `shadow.pin`.

### 6.6 Layout Patterns

**Driver app layout:** Full-bleed map. Floating search bar and filter pills on top. Bottom sheet station card. Bottom tab bar with raised center action button. Map terrain: `surface.mapTerrain`. Available markers: `brand.glow` with `shadow.pin`. Unavailable markers: `status.maintenance`.

**Dashboard layout:** Fixed left sidebar w-64 (256px), `surface.sidebar`, `border-r border.default`. Top header bar h-16. Main content area `surface.background`. Active nav item: `bg brand.sageLight`, `text brand.primary`.

### 6.7 Component Ownership

**Shared components** — `packages/ui`, used by Driver Web and Dashboard:
Button, Input, Select, Checkbox, Toggle, Textarea, Modal, Toast, Alert, Badge, Skeleton, EmptyState, ErrorState, Table, StatCard, StatusBadge

**Driver-specific components** — Driver Web and Driver Mobile:
MobileShell, MobileTopBar, SearchBar, FilterPills, MapPinMarker, BottomStationCard, SpecRow, BottomTabBar, CenterActionButton, ZoomControls, StationCard, ChargerRow, ReviewCard

**Dashboard-specific components** — Dashboard only:
AppShell, Sidebar, NavigationItem, TopBar, PageContent, DataCard, DataTable

---

## 7. Backend Services

### MVP-1 — json-server

| Property | Value |
|---|---|
| Tool | json-server |
| Data | `source/mock/db.json` |
| Port | 3001 |
| Routes | `source/mock/routes.json` (`"/api/*"` → `"/$1"`) |

json-server provides a REST API from a single JSON file. All three frontend apps call the same base URL. No code, no database, no migrations.

### MVP-2 and Beyond — Rust Services

| Service | Framework | Port |
|---|---|---|
| driver-service | Actix-web | 8080 |
| admin-service | Actix-web | 8081 |
| clickstream-service | Actix-web | 8082 (from MVP-5) |

Keycloak introduced in MVP-3. Traefik introduced in MVP-6.

**No additional services without an approved ADR.**

### API Prefix Rule

All endpoints in all MVPs are under the `/api` prefix. This never changes. json-server `routes.json` maps this from day one. When Rust services replace json-server, frontend apps change only the base URL — no endpoint path changes.

### Service Responsibilities (MVP-2+)

- **Driver Service:** public discovery (nearby, markers, search, detail), authenticated driver features, first-login provisioning. Reads from `inventory` and `gis`. Writes only to `users`.
- **Admin Service:** partner CRUD including type, flags, and audit fields; station CRUD; charger CRUD; availability updates; review moderation; reporting. Writes to `inventory`. Reads from `users` and `analytics` for reporting.
- **Clickstream Service (MVP-5):** event ingestion, validation, direct insert to `analytics.raw_events`.

---

## 8. Rust Service Architecture (MVP-2+)

Every Rust service follows the same internal structure:

```
source/services/<name>/
  src/
    main.rs       startup: config, pool, migrations, HttpServer
    config.rs     Config struct, from_env()
    router.rs     configure(api_prefix) function
    errors.rs     AppError enum, ResponseError impl
    handlers/     one file per domain
    db/           one file per domain, sqlx::query_as! only
  tests/
    integration/  one test file per handler file
  Cargo.toml
  Dockerfile
```

### Rules

- Config loaded once from environment via `Config::from_env()`, panics on missing required variables
- Every service calls `sqlx::migrate!("../../database/migrations").run(&pool)` on startup before accepting requests
- All SQL uses `sqlx::query_as!` or `sqlx::query!` macros — no ORM, no string interpolation
- All values passed as bind parameters
- Every service exposes `GET /api/health` that executes `SELECT 1` and returns db status
- Errors map to consistent JSON: `{"error":"not_found"}`, `{"error":"bad_request","message":"..."}`, `{"error":"internal_error"}`

---

## 9. Shared Rust Crates (MVP-2+)

| Crate | Purpose | Used by |
|---|---|---|
| `ev-core` | NanoID generation, ConnectorType and ChargerStatus enums | All services |
| `ev-db` | PgPool setup, OffsetParams, PaginatedResponse | All services |
| `ev-auth` | JWT validation, Claims struct, Role enum, JWKS cache | Driver Service, Admin Service |

`ev-geo` deferred until proven necessary after MVP-4. All geometry in SQL via PostGIS until then.

### Rules

- No service reimplements logic that belongs in a shared crate
- No circular dependencies between crates
- `ev-auth` never used by Clickstream Service — analytics ingestion is a public endpoint

### NanoID Prefix Registry

| Function | Prefix | Entity |
|---|---|---|
| `new_usr()` | `USR-...` | user_account |
| `new_prt()` | `PRT-...` | partner |
| `new_stn()` | `STN-...` | station |
| `new_chg()` | `CHG-...` | charger |
| `new_rev()` | `REV-...` | review |
| `new_evt()` | `EVT-...` | analytics event |

NanoID alphabet is alphanumeric only (A-Z, a-z, 0-9) for URL safety. Sequential integers never exposed in public APIs.

---

## 10. Shared Frontend Packages

| Package | Purpose | Used by |
|---|---|---|
| `packages/ui` | Design tokens, Tailwind base config, shared components | Driver Web, Dashboard |
| `packages/ui/native` | Native token export for React Native StyleSheet | Driver Mobile |
| `packages/api-client-driver` | Typed fetch client for Driver Service | Driver Web, Driver Mobile |
| `packages/api-client-admin` | Typed fetch client for Admin Service | Dashboard |
| `packages/api-client-events` | Fire-and-forget analytics client | All three apps |
| `packages/auth-client` | Keycloak JS adapter, in-memory token management | Driver Web, Dashboard |

### Rules

- `api-client-driver` must not be imported by Dashboard
- `api-client-admin` must not be imported by driver apps
- `native.ts` must stay synchronized with `colors.ts`
- Analytics errors are always swallowed silently — analytics never breaks the UI

---

## 11. Directory Structure

```
source/
  mock/
    db.json              MVP-1 mock data
    routes.json          /api/* prefix mapping
  services/
    driver-service/      Rust Actix-web (from MVP-2)
    admin-service/       Rust Actix-web (from MVP-2)
    clickstream-service/ Rust Actix-web (from MVP-5)
    crates/
      ev-core/           (from MVP-2)
      ev-db/             (from MVP-2)
      ev-auth/           (from MVP-3)
  apps/
    driver-web/          React + Vite
    driver-mobile/       React Native + Expo SDK 54
    dashboard/           React + Vite
  packages/
    ui/                  Design tokens + Tailwind base config
    api-client-driver/   (from MVP-2)
    api-client-admin/    (from MVP-2)
    api-client-events/   (from MVP-5)
    auth-client/         (from MVP-3)

database/
  migrations/            Canonical SQL migrations (from MVP-2)
  seeds/                 Dev seed data (from MVP-2)

docs/
  constitution.md
  implementation-plan.md
  glossary.md
  out-of-scope-registry.md
  adr/
  api/
  schema/
  design/
  ops/
  project/
    backlog.md
    bugs.md
    decisions.md
    sprints/
    phases/
  testing/
  guides/

.github/
  workflows/             (from MVP-2)
```

**Rule:** Adding a backend service means adding a folder under `source/services/`. Adding a frontend app means adding a folder under `source/apps/`. No other structural change.

---

## 12. Data Architecture

PostgreSQL is the single database across all MVPs. Database name: `ev_platform`. Schemas introduced progressively.

### Schema Introduction by MVP

- **MVP-1:** `source/mock/db.json` (json-server, no real database)
- **MVP-2:** `inventory` schema, `gis` schema (empty)
- **MVP-3:** `users` schema
- **MVP-4:** `gis` schema populated
- **MVP-5:** `analytics` schema

### MVP-2 Schema — inventory

**`inventory.partner`**

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT PK | `PRT-...` NanoID |
| `name` | TEXT NOT NULL | Partner display name |
| `type` | TEXT NOT NULL | `business` \| `personal`, default `business` |
| `is_verified` | BOOLEAN | Admin-approved identity, default `false` |
| `is_live` | BOOLEAN | Has visible stations, default `false` |
| `is_active` | BOOLEAN | Account enabled, default `true` |
| `created_at` | TIMESTAMPTZ | Immutable after insert |
| `created_by` | TEXT | `USR-...` nullable |
| `updated_at` | TIMESTAMPTZ | Updated on every write |
| `updated_by` | TEXT | `USR-...` nullable |

**Constraints:**
- `type IN ('business', 'personal')`
- `is_live = false OR is_verified = true` (is_live cannot be true without is_verified)

**`inventory.station`**

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT PK | `STN-...` NanoID |
| `partner_id` | TEXT FK | References `inventory.partner(id)` |
| `name` | TEXT NOT NULL | Station display name |
| `address` | TEXT | Nullable |
| `latitude` | NUMERIC(10,7) | NOT NULL, CHECK BETWEEN -90 AND 90 |
| `longitude` | NUMERIC(10,7) | NOT NULL, CHECK BETWEEN -180 AND 180 |
| `created_at` | TIMESTAMPTZ | Immutable after insert |
| `created_by` | TEXT | `USR-...` nullable |
| `updated_at` | TIMESTAMPTZ | Updated on every write |
| `updated_by` | TEXT | `USR-...` nullable |

**Critical rule:** `inventory.station` is the source of truth for all station data including coordinates. This never changes across any MVP.

**`inventory.charger`**

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT PK | `CHG-...` NanoID |
| `station_id` | TEXT FK | References `inventory.station(id)` |
| `connector_type` | TEXT NOT NULL | `type2` \| `ccs` \| `chademo` \| `type1` |
| `power_kw` | NUMERIC(6,2) | NOT NULL, CHECK > 0 |
| `status` | TEXT NOT NULL | `available` \| `in_use` \| `maintenance` \| `offline`, default `available` |
| `created_at` | TIMESTAMPTZ | Immutable after insert |
| `created_by` | TEXT | `USR-...` nullable |
| `updated_at` | TIMESTAMPTZ | Updated on every write |
| `updated_by` | TEXT | `USR-...` nullable |

**`inventory.station_availability`**

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT PK | Auto-generated |
| `station_id` | TEXT FK | References `inventory.station(id)` |
| `status` | TEXT NOT NULL | `available` \| `partial` \| `unavailable` |
| `updated_by` | TEXT | `USR-...` nullable |
| `updated_at` | TIMESTAMPTZ | Set on every insert |

Append-only. The current availability is the most recent row by `updated_at` for a given `station_id`. Never updated in place.

### MVP-3 Schema — users

**`users.user_account`**

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT PK | `USR-...` NanoID |
| `keycloak_sub` | TEXT UNIQUE | Stable Keycloak identifier |
| `email` | TEXT UNIQUE | From Keycloak token |
| `role` | TEXT | `registered_driver` \| `partner` \| `admin` |
| `created_at` | TIMESTAMPTZ | First-login provisioning timestamp |
| `last_login_at` | TIMESTAMPTZ | Updated on every authenticated call |

**`users.user_profile`**

| Column | Type | Notes |
|---|---|---|
| `user_id` | TEXT PK FK | References `users.user_account` |
| `display_name` | TEXT | Nullable |
| `avatar_url` | TEXT | Nullable |
| `phone` | TEXT | Nullable |
| `preferred_language` | TEXT | `fr` \| `ar`, default `fr` |
| `updated_at` | TIMESTAMPTZ | |

**`users.partner_membership`**

| Column | Type | Notes |
|---|---|---|
| `user_id` | TEXT PK FK | References `users.user_account`. PRIMARY KEY enforces one partner per user |
| `partner_id` | TEXT FK | References `inventory.partner` |
| `created_at` | TIMESTAMPTZ | |

**`users.favorite_station`**

| Column | Type | Notes |
|---|---|---|
| `user_id` | TEXT FK | References `users.user_account` |
| `station_id` | TEXT FK | References `inventory.station` |
| `created_at` | TIMESTAMPTZ | |
| PRIMARY KEY | `(user_id, station_id)` | |

**`users.station_review`**

| Column | Type | Notes |
|---|---|---|
| `id` | TEXT PK | `REV-...` NanoID |
| `station_id` | TEXT FK | References `inventory.station` |
| `user_id` | TEXT FK | References `users.user_account` |
| `rating` | SMALLINT | CHECK BETWEEN 1 AND 5 |
| `content` | TEXT | Nullable |
| `status` | TEXT | `visible` \| `hidden`, default `visible` |
| `created_at` | TIMESTAMPTZ | |
| `updated_at` | TIMESTAMPTZ | |
| UNIQUE | `(station_id, user_id)` | One review per user per station |

### MVP-4 Schema — gis

| Table | Notes |
|---|---|
| `gis.osm_nodes` | (osm_id BIGINT PK, tags JSONB, geom Point 4326) |
| `gis.osm_ways` | (osm_id BIGINT PK, tags JSONB, geom LineString 4326) |
| `gis.roads` | (id BIGSERIAL PK, osm_id, name, road_type, geom LineString 4326) |
| `gis.boundaries` | (id BIGSERIAL PK, osm_id, name, admin_level INT, geom MultiPolygon 4326) |
| `gis.amenity_points` | (id BIGSERIAL PK, osm_id, amenity_type, name, tags JSONB, geom Point 4326) |
| `gis.station_locations` | (station_id TEXT PK, geom Point 4326, snapped_road_id BIGINT, region_id BIGINT, updated_at TIMESTAMPTZ) |

**Critical rule:** `gis.station_locations` is written only by the PostgreSQL trigger `trg_station_gis_sync`. No application code writes here directly.

### MVP-5 Schema — analytics

| Table | Notes |
|---|---|
| `analytics.raw_events` | (id TEXT PK EVT-..., event_name TEXT, session_id TEXT, user_id TEXT nullable, payload JSONB, occurred_at TIMESTAMPTZ, received_at TIMESTAMPTZ) |
| `analytics.event_aggregates` | (id TEXT PK, aggregate_name TEXT, period_start TIMESTAMPTZ, period_end TIMESTAMPTZ, dimensions JSONB, value NUMERIC, computed_at TIMESTAMPTZ) |

### Cross-Schema Access Rules

| From | To | Permitted | Purpose |
|---|---|---|---|
| Admin Service | inventory | write | CRUD operations |
| Admin Service | users | read | moderation, reporting |
| Admin Service | analytics | read | reporting |
| Driver Service | inventory | read | discovery |
| Driver Service | gis | read | spatial queries |
| Driver Service | users | write | profile, favorites, reviews |
| Clickstream Service | analytics | write | event ingestion |
| Trigger function | gis | write | GIS sync only |

Any access not in this table is a constitution violation.

### Migration Order

**MVP-2:**
1. `0001_extensions.sql`
2. `0002_schemas.sql` — inventory, gis
3. `0003_inventory_tables.sql` — partner, station, charger, station_availability
4. `0004_inventory_indexes.sql`

**MVP-3:**
5. `0005_users_schema.sql`
6. `0006_users_tables.sql`
7. `0007_users_indexes.sql`

**MVP-4:**
8. `0008_gis_tables.sql`
9. `0009_gis_indexes.sql`
10. `0010_gis_sync_function.sql`
11. `0011_gis_sync_trigger.sql`
12. `0012_gis_resync.sql`

**MVP-5:**
13. `0013_analytics_schema.sql`
14. `0014_analytics_tables.sql`
15. `0015_analytics_indexes.sql`

Migrations are never edited after commit. Corrective changes are new migration files.

---

## 13. GIS Synchronization (MVP-4)

`inventory.station` is the source of truth for station coordinates across all MVPs.

MVP-4 introduces a PostgreSQL trigger on `inventory.station` firing after every `INSERT`, `UPDATE`, and `DELETE`. The trigger calls `gis.sync_station()` which computes PostGIS geometry, finds the nearest road, finds the containing administrative boundary, and upserts into `gis.station_locations`. The trigger fires within the same transaction. GIS failure logs a `WARNING` but does not block the station write.

Required spatial indexes before trigger activation:
```sql
CREATE INDEX idx_roads_geom ON gis.roads USING GIST (geom);
CREATE INDEX idx_boundaries_geom ON gis.boundaries USING GIST (geom);
CREATE INDEX idx_station_locations_geom ON gis.station_locations USING GIST (geom);
```

`gis.resync_all_stations()` rebuilds all GIS artifacts on demand.

---

## 14. Authentication and Authorization (MVP-3+)

Before MVP-3: All endpoints open. No user accounts. Dashboard operates without login via dev role switcher.

From MVP-3:

- Keycloak owns all authentication — no service implements its own token issuance
- JWT validated against Keycloak JWKS — cached on startup, refreshed every 5 minutes, never fetched per request
- First-login provisioning — upsert on `keycloak_sub` in Driver Service on first authenticated call
- Role enforcement in Actix-web middleware before any handler runs
- Partner scope via JWT `partner_id` claim — applied as mandatory filter in Admin Service middleware
- Web tokens in memory only — never `localStorage` or `sessionStorage`
- Mobile tokens in `expo-secure-store` only — never `AsyncStorage`
- Auth triggered only at gated action — never proactively on public routes

---

## 15. Analytics (MVP-5+)

Frontend → `POST /api/events` → Clickstream Service → `analytics.raw_events`

No message broker. Direct PostgreSQL insert. All events must be in the canonical taxonomy in `docs/guides/event-taxonomy.md`. Unknown event names rejected with 400. Analytics errors always swallowed silently in frontend — analytics never breaks the UI.

---

## 16. Runtime and Deployment

### MVP-1

Local only. json-server + three frontend dev servers. No Docker.

### MVP-2

Docker Compose introduced:
- `postgis/postgis:16-3.4` — PostgreSQL + PostGIS
- `driver-service` — Rust binary, port 8080 internal
- `admin-service` — Rust binary, port 8081 internal
- `pgadmin` — Dev only, port 5050

### MVP-3 adds

- `postgres-app` — renamed from `postgres`, `postgis/postgis:16-3.4`, database `ev_platform`
- `postgres-keycloak` — `postgres:16`, database `keycloak_db`
- `quay.io/keycloak/keycloak:24.0` — port 8180 internal, backed by `postgres-keycloak`

Keycloak auto-imports realm from `infra/keycloak/realm-export.json` on first start.

### MVP-5 adds

- `clickstream-service` — Rust binary, port 8082 internal

### MVP-6 Production Stack

| Service | Image |
|---|---|
| postgres | `postgis/postgis:16-3.4` |
| keycloak | `quay.io/keycloak/keycloak:24.0` |
| driver-service | Rust binary |
| admin-service | Rust binary |
| clickstream-service | Rust binary |
| traefik | `traefik:v3`, ports 80 and 443 public only |

Only Traefik exposes public ports. All other services use internal Docker networking.

Images are built locally from source. No image registry. No image push.

### CI/CD (from MVP-2)

GitHub Actions. Six path-scoped workflow files. On every push: lint, test, build. No automated deployment — always manual by runbook.

### Deployment (MVP-6)

1. SSH into server
2. `cd /opt/ev-platform`
3. `git pull origin main`
4. `docker compose -f docker-compose.prod.yml build`
5. `docker compose -f docker-compose.prod.yml up -d`
6. Verify all health endpoints
7. Check logs for startup errors
8. Rollback: `git checkout <sha>` && repeat steps 4-5

Host-managed environment files. Secrets on host only — never in images or repository.

---

## 17. Non-Negotiable Rules

### Data rules

- `inventory.station` is the source of truth for stations — always, across all MVPs
- `gis` is never the master of any business entity
- Analytics lives in the `analytics` schema exclusively
- No additional schemas without an approved ADR
- Migrations never edited after commit — add corrective migrations instead
- `is_live` cannot be `true` when `is_verified` is `false` — enforced at database level

### API rules

- All endpoints under `/api` prefix — from MVP-1, always
- Every service exposes `GET /api/health` with database connectivity check
- All SQL uses bind parameters — no string interpolation ever
- No sequential integers in public APIs — from MVP-2

### Infrastructure rules

- Only Traefik exposes public ports — from MVP-6
- Keycloak owns all authentication — from MVP-3
- No service implements its own token issuance
- Secrets never in committed files or container images
- No image registry — images built on host
- No additional Docker containers without an approved ADR

### Frontend rules

- No hardcoded visual values — tokens only, from MVP-1
- Tokens never in `localStorage` or `AsyncStorage` — from MVP-3
- RTL correct on every screen in Arabic — from MVP-3
- Public browsing never triggers auth prompt — from MVP-3
- `native.ts` stays synchronized with `colors.ts`
- Expo SDK 54 — no upgrade without an ADR
- OpenStreetMap tiles — no paid provider without an ADR
- Analytics errors never surface to the user — from MVP-5
- Dev role switcher removed entirely in MVP-3 Sprint 3.6

### Partner rules

- Partner stations hidden from public when `is_active`, `is_verified`, or `is_live` is `false`
- `is_live` cannot be `true` without `is_verified` — database `CHECK` constraint
- Partner scope enforced by JWT `partner_id` claim — from MVP-3
- Audit fields (`created_at`, `created_by`, `updated_at`, `updated_by`) on all inventory tables

### Tooling rules

- Code → SpecKit
- UX/UI → Impeccable
- Planning, docs, architecture → this assistant

### Permanently deferred

- OCPP, payments, routing — require ADR and plan revision to introduce

---

## 18. Decision Records

ADRs in `docs/adr/`. Required before any non-trivial architecture change. Small decisions in `docs/project/decisions.md`. ADRs never edited — superseding decision gets a new ADR.

### ADR Index

| ID | Title | Status |
|---|---|---|
| ADR-001 | PostgreSQL as single database | Accepted |
| ADR-002 | Schema separation | Accepted |
| ADR-003 | Prefixed NanoIDs over UUIDs (from MVP-2) | Accepted |
| ADR-004 | Direct analytics insert over message broker | Accepted |
| ADR-005 | Rust + Actix-web for backend (from MVP-2) | Accepted |
| ADR-006 | Bare metal + Docker Compose over Kubernetes | Accepted |
| ADR-007 | Keycloak for authentication (from MVP-3) | Accepted |
| ADR-008 | PostgreSQL trigger for GIS sync (from MVP-4) | Accepted |
| ADR-009 | Monorepo with source/ root | Accepted |
| ADR-010 | Traefik as edge router (from MVP-6) | Accepted |
| ADR-011 | React + Vite for web applications | Accepted |
| ADR-012 | React Native + Expo SDK 54 | Accepted |
| ADR-013 | Single Dashboard App for partner and admin | Accepted |
| ADR-014 | Leaflet + OpenStreetMap | Accepted |
| ADR-015 | Local image builds — no registry | Accepted |
| ADR-016 | json-server for MVP-1 mock API | Accepted |
| ADR-017 | Multiple MVP cycle delivery strategy | Accepted |
| ADR-018 | Dashboard built before driver apps in every MVP | Accepted |
| ADR-019 | Partner type field (business / personal) | Accepted |
| ADR-020 | Partner operational flags (is_verified, is_live, is_active) | Accepted |
| ADR-021 | Audit trail on all inventory tables | Accepted |

---

## 19. Definition of Done

### Sprint Done

- All tasks complete or deferred with written reason in sprint file
- No Class A bugs open
- All tests pass
- Manual smoke test completed
- Sprint summary written in `docs/project/sprints/sprint-NN.md`

### MVP Done

- All sprints meet Sprint Done criteria
- Phase status file complete in `docs/project/phases/`
- Documentation updated to reflect what was built
- Zero Class A bugs
- Hardening sprint completed and checklist verified
- Onboarding guide tested from scratch on a clean machine

### Bug Classification

**Class A** — blocks correctness, security, or user access. Resolved before MVP closes.
Examples: wrong data returned, endpoint missing /api prefix, migration fails, station visible despite unverified partner, is_live true without is_verified.

**Class B** — degrades quality, does not block. Resolved before target MVP closes.
Examples: slow query, missing validation message, UI misalignment.

**Class C** — improvement or nice-to-have. No mandatory target.
Examples: refactor opportunity, minor UX polish, documentation gap.
