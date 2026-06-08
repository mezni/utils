# Constitution

The permanent rules and principles governing BorneMap across all MVPs.

## 1. Purpose

BorneMap is an EV station discovery and management platform for Tunisia.

It serves four user types:
- **Public Driver** — browses stations without login
- **Registered Driver** — authenticated driver with favorites, reviews, and profile
- **Partner** — manages own stations and chargers through a dashboard
- **Admin** — manages the entire platform globally

### Current Platform Scope

- Public station discovery — map, nearby, search, detail
- Partner and admin management — stations, chargers, partners
- Manual availability updates
- GIS synchronization via PostgreSQL triggers
- Clickstream analytics

### Explicitly Out of Scope Across All MVPs

These are permanently deferred and will never be added without a new constitution revision:
- OCPP and charging sessions
- Payments and billing
- Routing and navigation
- Real-time availability (OCPP-driven)
- Push notifications

See `out-of-scope-registry.md` for rationale.

## 2. Core Principles

### Principle 1 — MVP-first Delivery

Build the minimum that proves the core loop works. Validate before adding complexity. Never introduce infrastructure that the current MVP does not need. Each MVP is complete and deployable on its own.

### Principle 2 — Layered Complexity

Each MVP adds one layer on top of a stable foundation. Nothing from a previous MVP is broken by a later one. Infrastructure replaces itself cleanly — the Python service is replaced by Rust, not rebuilt from scratch with a different data model.

### Principle 3 — Single Source of Truth

Every entity has exactly one authoritative owner. No ambiguity about where data is written or read from. All other representations are derived.

### Principle 4 — Simple Operations

The platform must be operable by one person. Every operational task must have a documented runbook. Complexity that cannot be operated simply is not acceptable.

### Principle 5 — Domain Separation by Schema

Business data, GIS data, user data, and analytics data are separated by PostgreSQL schema and by service responsibility. Cross-schema writes are forbidden except where explicitly permitted by this constitution.

### Principle 6 — Public Access Is a First-Class Concern

Anonymous public browsing must always work. Authentication is never required to view stations, markers, or search results. Auth is triggered only at the moment a gated action is attempted.

### Principle 7 — RTL and Arabic Are Not Afterthoughts

Arabic language support and RTL layout are built from the start of MVP-3. Any screen that does not work correctly in Arabic RTL from that point forward is a Class A bug.

### Principle 8 — Visual Consistency Across All Surfaces

All three applications share the same design token foundation. Brand identity, color semantics, spacing, and typography are defined once in `source/packages/ui` and consumed everywhere. No hardcoded visual values anywhere in application code.

### Principle 9 — API Prefix Consistency

All backend service endpoints are served under the `/api` prefix. No endpoint is exposed without this prefix. This rule applies from MVP-1 forward.

### Principle 10 — Tooling Separation

Code is written in SpecKit. UX and UI design is done in Impeccable. Planning, architecture, and documentation are maintained in this assistant. These roles do not overlap.

## 3. Roles and Access Model

### Public Driver

- Anonymous. No login required.
- **Can**: View nearby stations, view map markers, search and filter stations, view station detail, view public reviews and ratings
- **Cannot**: Favorite stations, write reviews, access any profile

### Registered Driver

- Authenticated. Keycloak role: `registered_driver`
- Introduced in MVP-3.
- **Can**: Everything a Public Driver can do, plus manage favorites, create/update/delete own reviews, view and update own profile

### Partner

- Authenticated. Keycloak role: `partner`
- Introduced in MVP-3.
- **Can**: Access Dashboard App (partner view), view and manage own stations only, view and manage own chargers only, update own station availability, view own reports
- **Rules**:
  - A partner user belongs to exactly one partner. Enforced by `users.partner_membership` PK on `user_id`.
  - Every partner operation is scoped to the partner's own data. Enforced by JWT `partner_id` claim in service middleware.
  - A partner can never read or write another partner's data.

### Admin

- Authenticated. Keycloak role: `admin`
- Introduced in MVP-3.
- **Can**: Access Dashboard App (admin view), manage all users/partners/stations/chargers, moderate reviews, access global reporting

## 4. Frontend Applications

The platform has three frontend applications:

### Driver Web App

- **Stack**: React + Vite
- **Purpose**: Public and authenticated driver experience
- **Map**: Leaflet with OpenStreetMap tiles. Full-bleed map layout with floating UI elements.

### Driver Mobile App

- **Stack**: React Native + Expo SDK 54
- **Purpose**: Public and authenticated driver experience on iOS and Android
- **Map**: react-native-maps with default provider. Full-bleed map layout.

### Dashboard App

- **Stack**: React + Vite
- **Purpose**: Single application serving both Partner and Admin roles
- **Auth**: Role determined from JWT on login (from MVP-3). In MVP-1 and MVP-2 operates without auth.
- **Layout**: Fixed left sidebar navigation

### Shared Frontend Rules

- All three applications consume tokens from `source/packages/ui`
- Driver Web App and Dashboard App consume components from `source/packages/ui`
- Driver Mobile App consumes tokens from `source/packages/ui/src/tokens/native.ts`
- All frontend API calls target the `/api` prefix on the relevant service
- No hardcoded visual values anywhere — tokens only

### Map Library Rules

- Driver Web App uses Leaflet with OpenStreetMap tiles — no API key required
- Driver Mobile App uses react-native-maps with default provider
- No paid map library or tile provider without an approved ADR

### Expo SDK Version

The Driver Mobile App targets **Expo SDK 54** exclusively.

```
React Native:    0.76.5
React:           18.3.1
Expo Router:     ~4.0.0
expo-location:   ~18.0.0
react-native-maps: 1.18.0
```

No Expo SDK upgrade without an approved ADR.

### Accessibility and Language Rules

- All web applications target WCAG 2.1 AA minimum from MVP-3
- Arabic and French are the two supported languages — implemented in MVP-3
- RTL layout must be correct for Arabic in every screen from MVP-3 onward
- Language switching works without a page reload

## 5. Design System

The design system is defined in `source/packages/ui` and is the single source of truth for all visual values across all three applications.

### 5.1 Color Tokens

```
brand.primary        #007943   Primary green. CTAs, active states, links.
brand.primaryDark    #005c32   Darker green for gradients, pressed states.
brand.sageLight      #EAF0E6   Selected states, map terrain, active nav.
brand.glow           #00E676   Live map pin markers. Driver apps only.

surface.background   #F8FAF6   Page and screen canvas.
surface.card         #FFFFFF   Cards, panels, modals.
surface.sidebar      #FFFFFF   Dashboard sidebar.
surface.mapTerrain   #EAF0E6   Map canvas base color. Driver apps only.

text.main            #111827   Primary text. Headings and body.
text.muted           #6B7280   Secondary text. Labels, metadata.

border.default       #E5E7EB   Standard dividers and card borders.
border.subtle        #F3F4F6   Light dividers inside cards.

status.available     #10B981   Available charger or healthy station.
status.availableBg   #ECFDF5   Available badge background.
status.inUse         #F59E0B   Station or charger in use.
status.inUseBg       #FFFBEB   In-use badge background.
status.maintenance   #EF4444   Maintenance or offline.
status.maintenanceBg #FEF2F2   Maintenance badge background.

neutral.50 → neutral.900       Full neutral gray scale.
```

### 5.2 Typography Tokens

```
font.family.sans     Plus Jakarta Sans, Inter, system-ui, sans-serif
font.family.arabic   Cairo, system-ui, sans-serif

font.size    xs(10) sm(12) base(14) lg(16) xl(18) 2xl(20) 3xl(24)
font.weight  regular(400) medium(500) semibold(600) bold(700)
             extrabold(800)
```

- Driver apps use Plus Jakarta Sans
- Dashboard uses Inter

### 5.3 Spacing, Radius, and Shadows

**Spacing**: Base unit 4px. Scale: 4, 8, 12, 16, 20, 24, 32, 40, 48, 64, 80, 96

**Radius**: sm(4), md(8), lg(12), xl(16), 2xl(20), 3xl(24), full(9999)

**Shadows**:
- `shadow.card` — Subtle elevation for cards
- `shadow.panel` — Medium elevation for panels
- `shadow.float` — High elevation for bottom sheets
- `shadow.pin` — Neon glow for live map markers using `brand.glow`

### 5.4 Token Delivery

- Web applications (Driver Web, Dashboard) extend `source/packages/ui/tailwind.config.base.js`
- Driver Mobile App imports from `source/packages/ui/src/tokens/native.ts` — plain JavaScript values for React Native `StyleSheet`
- **Critical rule**: `native.ts` must stay synchronized with `colors.ts`. Any token added to `colors.ts` is added to `native.ts` in the same commit.

### 5.5 Theme Boundaries

- **Driver apps** use: all brand tokens, surface tokens including mapTerrain, glow, shadow.pin, Plus Jakarta Sans
- **Dashboard** uses: all brand tokens, surface tokens including sidebar, Inter, does not use glow or mapTerrain

### 5.6 Layout Patterns

**Driver app layout**:
- Full-bleed map
- Floating search bar and filter pills on top
- Bottom sheet or card for station detail
- Bottom tab bar with raised center action button (mobile only)
- Map terrain color: `surface.mapTerrain`
- Available markers: `brand.glow` with `shadow.pin`
- Unavailable markers: `status.maintenance`

**Dashboard layout**:
- Fixed left sidebar width 64 (256px), background `surface.sidebar`, border-r `border.default`
- Top header bar height 16 (64px)
- Main content area background `surface.background`
- Active nav item: background `brand.sageLight`, text `brand.primary`

### 5.7 Component Ownership

**Shared components** (Driver Web and Dashboard via packages/ui):
- Button, Input, Select, Checkbox, Toggle, Textarea, Modal, Toast, Alert, Badge, Skeleton, EmptyState, ErrorState, Table, StatCard, StatusBadge

**Driver-specific components**:
- MobileShell, MobileTopBar, SearchBar, FilterPills, MapPinMarker, BottomStationCard, SpecRow, BottomTabBar, CenterActionButton, ZoomControls, StationCard, ChargerRow, ReviewCard

**Dashboard-specific components**:
- AppShell, Sidebar, NavigationItem, TopBar, PageContent, DataCard, DataTable

## 6. Backend Services

### MVP-1 (Current)

One Python FastAPI service handling all endpoints.

```
Source:  source/services/bornemap-service/
Port:    8000
Prefix:  /api
```

### MVP-2 and Beyond

Python service is replaced by Rust services. The data model migrates cleanly. Frontend apps change only the base URL.

```
driver-service       Actix-web, port 8080
admin-service        Actix-web, port 8081
clickstream-service  Actix-web, port 8082
```

Keycloak is introduced in MVP-3.
Traefik is introduced in MVP-6.

**No additional services without an approved ADR.**

### API Prefix Rule

All endpoints in all services, in all MVPs, are served under the `/api` prefix. This never changes.

### Service Responsibilities (Full Platform — Post MVP-2)

- **Driver Service**: public discovery, authenticated driver features, first-login provisioning
- **Admin Service**: partner CRUD, station CRUD, charger CRUD, availability updates, review moderation, reporting
- **Clickstream Service**: event ingestion, validation, direct insert to analytics schema
- **Keycloak** (MVP-3): authentication, JWT issuance, role assignment, social login
- **Traefik** (MVP-6): sole public entrypoint, TLS, routing, rate limiting

## 7. Directory Structure

All runnable code lives under `source/`. Nothing outside `source/` is executable.

```
source/
  services/
    bornemap-service/     Python FastAPI (MVP-1)
      app/
        main.py
        config.py
        database.py
        models/
        schemas/
        routers/
        services/
      migrations/
      tests/
      requirements.txt
      .env.example
  apps/
    driver-web/           React + Vite
    driver-mobile/        React Native + Expo SDK 54
    dashboard/            React + Vite
  packages/
    ui/                   Design tokens + Tailwind base config

database/
  migrations/             Canonical SQL (used from MVP-2 onward)
  seeds/                  Dev seed data

docs/
  [documentation files]

.github/
  workflows/
```

**Rule**: Adding a new backend service means adding a folder under `source/services/`. Adding a frontend app means adding a folder under `source/apps/`. No other structural change.

## 8. Data Architecture

PostgreSQL is the single database across all MVPs. The database name is `ev_platform`. Schemas are introduced progressively.

### Schema Introduction by MVP

```
MVP-1:  inventory schema (partner, station, charger)
        gis schema (created empty — reserved)
MVP-2:  PostGIS extension, spatial indexes on gis schema
MVP-3:  users schema (user_account, user_profile,
                      partner_membership, favorite_station,
                      station_review)
MVP-4:  gis schema populated (osm_nodes, osm_ways, roads,
                               boundaries, station_locations)
        GIS trigger and sync function
MVP-5:  analytics schema (raw_events, event_aggregates)
```

### MVP-1 Schema

```
inventory.partner:
id          UUID PRIMARY KEY
name        TEXT NOT NULL
created_at  TIMESTAMPTZ DEFAULT now()

inventory.station:
id          UUID PRIMARY KEY
partner_id  UUID NOT NULL REFERENCES inventory.partner(id)
name        TEXT NOT NULL
address     TEXT
latitude    NUMERIC(10,7) NOT NULL
longitude   NUMERIC(10,7) NOT NULL
created_at  TIMESTAMPTZ DEFAULT now()
updated_at  TIMESTAMPTZ DEFAULT now()

inventory.charger:
id              UUID PRIMARY KEY
station_id      UUID NOT NULL REFERENCES inventory.station(id)
connector_type  TEXT NOT NULL
power_kw        NUMERIC(6,2) NOT NULL
status          TEXT NOT NULL DEFAULT 'available'
updated_at      TIMESTAMPTZ DEFAULT now()
```

### Key Constraints

- All IDs are UUID v4 in MVP-1
- Latitude range: -90 to 90. Longitude range: -180 to 180
- Charger status enum: `available`, `in_use`, `maintenance`
- Connector types: `Type2`, `CCS`, `CHAdeMO`, etc. (from API schema)

### Writing Data

Only the backend service writes to the database. Frontend apps are read-only until MVP-3 (auth). Dashboard and Driver Web submit forms → FastAPI writes.

### Identifier Registry (MVP-2 Onward)

```
Prefix  Entity
USR-    user_account
PRT-    partner
STN-    station
CHG-    charger
REV-    review
EVT-    analytics event
```

Identifiers in MVP-1 are UUID v4. NanoID prefixed identifiers are introduced in MVP-2 with the Rust migration.

### Cross-Schema Access Rules (Full Platform)

| From | To | Permitted | Purpose |
|------|----|-----------|---------| 
| Admin Service | inventory | write | CRUD operations |
| Admin Service | users | read | moderation, reporting |
| Admin Service | analytics | read | reporting |
| Driver Service | inventory | read | discovery |
| Driver Service | gis | read | spatial queries |
| Driver Service | users | write | profile, favorites, reviews |
| Clickstream Service | analytics | write | event ingestion |
| Trigger function | gis | write | GIS sync only |

**Any access not in this table is a constitution violation.**

## 9. GIS Synchronization (MVP-4)

### Source of Truth

`inventory.station` is the source of truth for station coordinates across all MVPs. This never changes.

### MVP-1 and MVP-2

Nearby search uses simple Euclidean distance calculation (MVP-1) or PostGIS `ST_DWithin` on raw coordinates (MVP-2). No GIS sync worker. No outbox pattern.

### MVP-4

A PostgreSQL trigger on `inventory.station` fires after every INSERT, UPDATE, and DELETE. It calls `gis.sync_station()` which upserts into `gis.station_locations`.

The trigger fires within the same transaction as the station write. GIS sync failure logs a WARNING but does not roll back the station write.

**Required spatial indexes before trigger is active**:

```sql
CREATE INDEX idx_roads_geom ON gis.roads USING GIST (geom);
CREATE INDEX idx_boundaries_geom ON gis.boundaries USING GIST (geom);
CREATE INDEX idx_station_locations_geom
    ON gis.station_locations USING GIST (geom);
```

A stored procedure `gis.resync_all_stations()` rebuilds all GIS artifacts from `inventory.station` on demand.

**No application code writes to `gis.station_locations` directly. Ever.**

## 10. Authentication and Authorization (MVP-3)

### Before MVP-3

All endpoints are open. No authentication. No user accounts. The Dashboard operates without login. Access control is enforced only by convention and by not exposing the Dashboard URL publicly.

### From MVP-3 Onward

**Authentication owner**: Keycloak. No service implements its own token issuance.

**JWT validation**: Bearer token validated against Keycloak JWKS. JWKS is cached on startup and refreshed every 5 minutes. Never fetched per request.

**First-login provisioning**: On first authenticated call to Driver Service, a `users.user_account` record is created via upsert on `keycloak_sub`. Idempotent — safe to call on every request.

**Role enforcement**: Applied in Actix-web middleware before any handler runs. Roles extracted from JWT `realm_access.roles` claim via `ev-auth` shared crate.

**Partner scope enforcement**: `partner_id` injected into JWT by Keycloak mapper. Admin Service middleware applies it as a mandatory filter on all partner-scoped queries.

**Token storage**:
- Web apps: memory only — never localStorage or sessionStorage
- Mobile app: expo-secure-store only — never AsyncStorage

**Authenticated upgrade pattern**: Public browsing never triggers an auth prompt. Auth triggered only at the moment a gated action is attempted.

## 11. Analytics (MVP-5)

### Event Flow

```
Frontend → POST /api/events → Clickstream Service
         → analytics.raw_events (direct insert)
```

No RabbitMQ. No message broker. Direct PostgreSQL insert.

### Rules

- Every event must be in the canonical taxonomy in `docs/guides/event-taxonomy.md`
- Unknown event names rejected with HTTP 400
- Every event carries: `event_name`, `session_id`, `occurred_at`, optional `user_id`
- Anonymous events valid — `user_id` nullable
- `EVT-...` NanoID for deduplication
- Analytics errors are always swallowed silently in the frontend — analytics never breaks the UI

## 12. Shared Rust Crates (MVP-2 Onward)

| Crate | Purpose |
|-------|---------|
| ev-core | NanoID generation, shared enums, domain types |
| ev-db | PgPool setup, pagination structs |
| ev-auth | JWT validation, Claims, Role enum, JWKS cache |

`ev-geo` deferred until proven necessary after MVP-4. Until then all geometry in SQL via PostGIS.

### Rules

- No service reimplements logic that belongs in a shared crate
- No circular dependencies between crates
- `ev-auth` never used by Clickstream Service (public endpoint)

## 13. Non-Negotiable Rules

These rules apply across every MVP and every sprint. Violating any of these is a Class A issue that must be resolved before the MVP closes.

### Data Rules

- `inventory.station` is the source of truth for stations across all MVPs
- `gis` schema is never the master of any business entity
- Analytics lives in the `analytics` schema exclusively
- No additional schemas without an approved ADR
- Migrations never edited after commit — add a corrective migration instead

### API Rules

- All endpoints under `/api` prefix — always, from MVP-1 forward
- Every service exposes `GET /api/health` with database connectivity check
- All SQL uses bind parameters — no string interpolation ever
- Sequential integers never exposed in public APIs

### Infrastructure Rules

- Only Traefik exposes public ports (enforced from MVP-6)
- Keycloak owns all authentication (enforced from MVP-3)
- No service implements its own token issuance
- Secrets never in committed files or container images
- No image registry — images built on host (from MVP-2)

### Frontend Rules

- No hardcoded visual values — tokens only, from MVP-1 forward
- Tokens never in localStorage or AsyncStorage (from MVP-3)
- RTL correct on every screen in Arabic (from MVP-3)
- Public browsing never triggers auth prompt (from MVP-3)
- `native.ts` stays synchronized with `colors.ts`
- Expo SDK version is 54 — no upgrade without ADR
- OpenStreetMap tiles — no paid provider without ADR
- Analytics errors never surface to the user (from MVP-5)

### Tooling Rules

- Code is written in SpecKit
- UX and UI design is done in Impeccable
- Planning, docs, and architecture stay in this assistant
- These roles do not overlap

### Deferred Rules

- OCPP, payments, routing are permanently out of scope
- Introducing any of these requires a new implementation plan revision and an approved ADR

## 14. Decision Records

All non-trivial architecture decisions are recorded as ADRs in `docs/adr/`.

### When an ADR is Required

- Introducing a new service, infrastructure component, or data store
- Changing the source of truth for any entity
- Changing the authentication or authorization model
- Superseding a previous ADR
- Changing the Expo SDK version
- Changing the map library or tile provider
- Changing the MVP scope

### Decision File

Small decisions go in `docs/project/decisions.md`:
- Framework selection within a layer (e.g., form validation library)
- Tool or package choices

**All decisions recorded before code. No surprises on review.**

### Once Accepted

An ADR is never edited. A superseding decision gets a new ADR that references the old one.

### Current ADR Index

| ID | Title | Status |
|----|-------|--------|
| ADR-001 | PostgreSQL as single database | Accepted |
| ADR-002 | Schema separation over database separation | Accepted |
| ADR-003 | Prefixed NanoIDs over UUIDs (from MVP-2) | Accepted |
| ADR-004 | Direct analytics insert over message broker | Accepted |
| ADR-005 | Rust + Actix-web for backend services (from MVP-2) | Accepted |
| ADR-006 | Bare metal + Docker Compose over Kubernetes | Accepted |
| ADR-007 | Keycloak for authentication (from MVP-3) | Accepted |
| ADR-008 | PostgreSQL trigger for GIS sync (from MVP-4) | Accepted |
| ADR-009 | Monorepo with source/ root | Accepted |
| ADR-010 | Traefik as edge router (from MVP-6) | Accepted |
| ADR-011 | React + Vite for web applications | Accepted |
| ADR-012 | React Native + Expo SDK 54 for mobile | Accepted |
| ADR-013 | Single Dashboard App for partner and admin | Accepted |
| ADR-014 | Leaflet + OpenStreetMap for web map | Accepted |
| ADR-015 | Local image builds — no registry | Accepted |
| ADR-016 | Python FastAPI for MVP-1 backend | Accepted |
| ADR-017 | Multiple MVP cycle delivery strategy | Accepted |

## 15. Definition of Done

### Sprint Done

- [ ] All planned tasks complete or deferred with written reason in sprint file
- [ ] No Class A bugs open
- [ ] All tests pass
- [ ] Manual smoke test completed
- [ ] Sprint summary written

### MVP Done

- [ ] All sprints meet Sprint Done criteria
- [ ] All MVP done criteria checked in phase status file
- [ ] Documentation updated to reflect what was built
- [ ] Zero Class A bugs open
- [ ] Hardening sprint completed and all checklist items verified
- [ ] Onboarding guide tested from scratch on a clean machine

### Bug Classification

**Class A** — blocks correctness, security, or user access. Resolved before MVP closes.
- *Examples*: wrong data returned, endpoint missing `/api` prefix, migration fails on clean database, map shows no stations when database has data.

**Class B** — degrades quality, does not block. Resolved before target MVP closes.
- *Examples*: slow query, missing validation message, UI misalignment.

**Class C** — improvement or nice-to-have. No mandatory target.
- *Examples*: refactor opportunity, minor UX polish, documentation gap.
