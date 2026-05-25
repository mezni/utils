# BorneMap MVP0 — Implementation Plan

**Version**: 1.0.0 | **Date**: 2026-05-25 | **Constitution**: v2.0.0

## MVP0 Scope Definition

MVP0 delivers the minimum viable platform demonstrating the core value
proposition: an EV driver discovers nearby charging stations on a map,
an administrator manages the data pipeline, and a partner operator views
their own infrastructure — all backed by a single Rust binary with
PostGIS spatial queries meeting the ≤200ms SLO.

### In Scope

- Full database schema, migrations, and deterministic sandbox seed
- Backend API: `/api/v1/stations/nearby`, CRUD for users/partners/
  stations/chargers/connector-types, authentication scaffold
- Admin Portal: AppShell, navigation, Overview dashboard, Data views
  (Partners, Stations, Chargers), Settings (Infrastructure Types)
- Partner Dashboard: Multi-tenant station/charger management
- Mobile Driver App: Map canvas, nearby discovery, station detail sheet
- Docker Compose local development stack
- GitHub Actions CI/CD pipeline (lint, test, clippy, build verification)
- All constitutional invariants enforced (semantic IDs, soft delete,
  `is_test` isolation, defensive UX, design tokens)

### Out of Scope (Post-MVP0)

- Analytics module (Phase 2.2 from execution plan)
- Security module / `<PermissionsMatrix/>`
- App Settings (branding, dropzones, map tokens)
- Reviews (`REV-` prefix reserved but unused)
- Production deployment / TLS / rate limiting
- CD (continuous deployment) — pipeline verifies builds but does not
  auto-deploy

## Constitution Check

*GATE: Must pass before Phase 0. Re-check after each phase.*

| Principle | Gate | Verification |
|-----------|------|-------------|
| I. Modular Monorepo Architecture | All code under `sources/`; single Rust binary; `/api/v1/*` namespace | File structure audit |
| II. Semantic Identity & Data Isolation | `USR-`/`PRT-`/`STN-`/`CHG-`/`CNT-` prefixes; `is_test` SQL filter; soft delete | Migration + query audit |
| III. Administrative UX Discipline | Centralized design tokens; `<ScrollableTable/>`; destructive confirmation modal | Component audit |
| IV. Mobile & Discovery Constraints | Expo Go managed; 20km default radius; LIMIT 50; `is_test` hidden | Mobile build + API audit |
| V. Deterministic Implementation | Domain layers modular; seed script repeatable; sandbox indicator | Migration replay test |

---

## Phase 0: Project Scaffolding & CI/CD

**Duration**: 3 days | **Dependencies**: None

**Deliverable**: Empty monorepo with all workspace configs, Docker
Compose, CI/CD pipeline, and placeholder directories matching the
constitution's repository structure.

### 0.1 Root Workspace Initialization

```
bornemap-monorepo/
├── Cargo.toml                    # Virtual manifest (workspace)
├── docker-compose.dev.yml        # Per docs/09-local-orchestration.md
├── .gitignore
├── .github/
│   └── workflows/
│       ├── backend.yml           # Rust: clippy, test, build
│       ├── frontend.yml          # pnpm: lint, type-check, build
│       └── docker.yml            # Docker Compose smoke test
├── sources/
│   ├── backend/
│   │   ├── Cargo.toml
│   │   ├── Dockerfile.dev        # Per docs/09-local-orchestration.md
│   │   ├── migrations/           # Empty — populated in Phase 1
│   │   ├── src/
│   │   │   └── main.rs           # Minimal Actix-web hello-world on :8080
│   │   └── sqlx-data.json        # Empty baseline
│   └── frontend/
│       ├── package.json          # Workspace root (pnpm workspaces)
│       ├── packages/
│       │   └── ui/
│       │       ├── package.json
│       │       ├── tailwind.config.ts  # Per docs/03-web-admin-ux-spec.md
│       │       └── src/
│       │           └── components/
│       │               └── ui/
│       │                   └── scrollable-table.tsx  # Placeholder
│       └── apps/
│           ├── admin-portal/
│           │   ├── package.json
│           │   └── src/
│           ├── partner-dashboard/
│           │   ├── package.json
│           │   └── src/
│           └── mobile-driver/
│               ├── app.json
│               ├── package.json  # Locked deps per docs/04-mobile-driver-ux-spec.md
│               └── src/
```

### 0.2 Docker Compose Stack

- Write `docker-compose.dev.yml` per docs/09-local-orchestration.md
- Write `sources/backend/Dockerfile.dev` per docs/09-local-orchestration.md
- Verify: `docker compose up` starts postgres (healthy) + backend
  (hello-world response on :8080)

### 0.3 Frontend Workspace Config

- `pnpm-workspace.yaml` referencing `packages/*` and `apps/*`
- `packages/ui/tailwind.config.ts` with all design tokens from
  docs/03-web-admin-ux-spec.md
- Vite + React + TypeScript scaffold for `admin-portal` and
  `partner-dashboard`
- Expo SDK 51 scaffold for `mobile-driver`

### 0.4 CI/CD — GitHub Actions Workflows

Three workflow files under `.github/workflows/`, triggered on push
and pull request to `main` and feature branches.

#### `backend.yml` — Rust Pipeline

```yaml
name: Backend CI
on:
  push:
    paths: ['sources/backend/**', 'Cargo.toml']
  pull_request:
    paths: ['sources/backend/**', 'Cargo.toml']

jobs:
  check:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgis/postgis:16-3.4-alpine
        env:
          POSTGRES_DB: bornemap_test
          POSTGRES_USER: bornemap_admin
          POSTGRES_PASSWORD: test_key
        ports: ['5432:5432']
        options: >-
          --health-cmd pg_isready
          --health-interval 5s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: sources/backend -> target
      - run: cargo fmt --check
        working-directory: sources/backend
      - run: cargo clippy --all-targets -- -D warnings
        working-directory: sources/backend
        env:
          DATABASE_URL: postgres://bornemap_admin:test_key@localhost:5432/bornemap_test
      - run: cargo test
        working-directory: sources/backend
        env:
          DATABASE_URL: postgres://bornemap_admin:test_key@localhost:5432/bornemap_test
      - run: cargo build --release
        working-directory: sources/backend
```

#### `frontend.yml` — Frontend Pipeline

```yaml
name: Frontend CI
on:
  push:
    paths: ['sources/frontend/**']
  pull_request:
    paths: ['sources/frontend/**']

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        with:
          version: 9
      - uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: pnpm
          cache-dependency-path: sources/frontend/pnpm-lock.yaml
      - run: pnpm install --frozen-lockfile
        working-directory: sources/frontend
      - run: pnpm -r lint
        working-directory: sources/frontend
      - run: pnpm -r type-check
        working-directory: sources/frontend
      - run: pnpm -r build
        working-directory: sources/frontend
```

#### `docker.yml` — Docker Compose Smoke Test

```yaml
name: Docker Smoke Test
on:
  push:
    paths: ['docker-compose.dev.yml', 'sources/backend/Dockerfile.dev']
  pull_request:
    paths: ['docker-compose.dev.yml', 'sources/backend/Dockerfile.dev']

jobs:
  smoke:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: docker compose -f docker-compose.dev.yml up -d --wait
      - run: |
          curl -sf --retry 10 --retry-delay 3 http://localhost:8080/api/v1/health || exit 1
      - run: docker compose -f docker-compose.dev.yml down -v
```

#### CI/CD Design Decisions

| Decision | Rationale |
|----------|-----------|
| Path-based triggers | Only run relevant pipeline on changed paths; saves CI minutes |
| PostgreSQL service container | `cargo test` and `cargo clippy` with SQLx require a live DB for compile-time verification |
| `sqlx-data.json` offline mode | Alternative: commit `sqlx-data.json` and use `SQLX_OFFLINE=true` to skip live DB; choose service container for tighter verification |
| `pnpm install --frozen-lockfile` | Prevents dependency drift; fails CI if lockfile is out of sync |
| No auto-deploy (CD) | MVP0 is CI-only; CD pipeline deferred to post-MVP0 when deployment target is defined |
| Separate `docker.yml` | Docker build is slow; only triggered when Compose or Dockerfile changes |

**Checkpoint**: `docker compose up` → backend responds on :8080,
`pnpm dev` in each frontend app renders a blank page without errors,
all three GitHub Actions workflows pass on a test push.

---

## Phase 1: Backend Core — Schema, Identity & CRUD

**Duration**: 5 days | **Dependencies**: Phase 0

**Deliverable**: Fully migrated database with seed data, ID generator
utility, and CRUD API endpoints for all domain entities.

### 1.1 Database Migrations

- Write `20260525000000_init.up.sql` per docs/07-database-schema.md:
  - `CREATE EXTENSION IF NOT EXISTS postgis;`
  - All enums: `user_role`, `partner_type`, `current_type`,
    `charger_status`
  - All tables: `users`, `partner_profiles`,
    `station_connector_types`, `stations`, `chargers`
  - All indexes (GIST, partial B-tree, B-tree)
- Write `20260525000001_seed_sandbox.up.sql` per docs/07-database-schema.md:
  - 2 connector types, 5 partner users, 5 partner profiles,
    100 stations, 300 chargers
  - All IDs conform to prefix + 12-char nanoid format
- Write corresponding `.down.sql` rollback scripts
- Verify: `sqlx migrate run` succeeds; seed data queryable

### 1.2 ID Generator Utility

File: `sources/backend/src/utils/id_generator.rs`

- Function `generate_id(prefix: &str) -> String`
- Uses `nanoid` crate with custom alphabet (lowercase + digits),
  12-character length
- Produces IDs like `STN-k4m2n9p1q5v8`
- Unit tests verifying prefix attachment, length, and alphabet

### 1.3 Domain Module Structure

```
sources/backend/src/
├── main.rs
├── domain/
│   ├── mod.rs
│   ├── users/
│   │   ├── mod.rs            # Route handlers
│   │   ├── repository.rs     # DB queries
│   │   └── models.rs         # Structs, DTOs
│   ├── partners/
│   │   ├── mod.rs
│   │   ├── repository.rs
│   │   └── models.rs
│   ├── stations/
│   │   ├── mod.rs
│   │   ├── repository.rs
│   │   └── models.rs
│   ├── chargers/
│   │   ├── mod.rs
│   │   ├── repository.rs
│   │   └── models.rs
│   ├── connector_types/
│   │   ├── mod.rs
│   │   ├── repository.rs
│   │   └── models.rs
│   └── infrastructure/       # Nearby discovery (Phase 2)
│       ├── mod.rs
│       └── repository.rs
└── utils/
    └── id_generator.rs
```

### 1.4 CRUD API Endpoints

All mounted under `/api/v1/`. Every list endpoint respects:
- `WHERE deleted_at IS NULL` (soft-delete filter)
- `is_test` filtering per constitutional rules
- Partner-scoped endpoints inject `owner_id` from auth context

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/v1/users` | Create user (admin, partner, driver) |
| GET | `/api/v1/users` | List users (admin only) |
| GET | `/api/v1/users/:id` | Get user detail |
| PATCH | `/api/v1/users/:id` | Update user |
| DELETE | `/api/v1/users/:id` | Soft-delete user |
| POST | `/api/v1/partners` | Create partner profile |
| GET | `/api/v1/partners` | List partner profiles |
| GET | `/api/v1/partners/:id` | Get partner detail |
| PATCH | `/api/v1/partners/:id` | Update partner profile |
| DELETE | `/api/v1/partners/:id` | Soft-delete partner profile |
| POST | `/api/v1/connector-types` | Create connector type |
| GET | `/api/v1/connector-types` | List connector types |
| PATCH | `/api/v1/connector-types/:id` | Update connector type |
| DELETE | `/api/v1/connector-types/:id` | Soft-delete connector type |
| POST | `/api/v1/stations` | Create station (with coordinates) |
| GET | `/api/v1/stations` | List stations (partner-scoped) |
| GET | `/api/v1/stations/:id` | Get station detail |
| PATCH | `/api/v1/stations/:id` | Update station |
| DELETE | `/api/v1/stations/:id` | Soft-delete station |
| POST | `/api/v1/stations/:id/chargers` | Create charger |
| GET | `/api/v1/stations/:id/chargers` | List chargers for station |
| PATCH | `/api/v1/chargers/:id` | Update charger status/details |
| DELETE | `/api/v1/chargers/:id` | Delete charger (hard delete) |

### 1.5 Authentication Scaffold

- JWT-based auth middleware (HMAC-SHA256, no external IdP for MVP0)
- POST `/api/v1/auth/login` — email + password → JWT
- POST `/api/v1/auth/register` — driver self-registration
- Auth middleware extracts `user_id` and `role` from JWT claims
- Partner-scoped routes verify `role = partner` and inject `owner_id`

**Checkpoint**: All CRUD endpoints respond correctly via `curl` /
`httpie`. Seed data queryable. ID generator produces compliant IDs.
Auth middleware blocks unauthenticated requests.

---

## Phase 2: Spatial Discovery — Nearby API & SLO Validation

**Duration**: 3 days | **Dependencies**: Phase 1

**Deliverable**: High-performance `/api/v1/stations/nearby` endpoint
meeting ≤200ms SLO against the 100-station seed dataset.

### 2.1 Nearby Discovery Endpoint

File: `sources/backend/src/domain/infrastructure/mod.rs`

- `GET /api/v1/stations/nearby` per docs/10-technical-implementations.md
- Query params: `longitude`, `latitude`, `radius_meters` (default
  20000.0), `include_test` (default false)
- Repository function `find_nearby_stations_bounded` per
  docs/10-technical-implementations.md
- SQL uses `ST_DWithin` + GIST index, `LIMIT 50`,
  `is_test` isolation filter

### 2.2 SLO Benchmark

- Write a benchmark script (e.g., `wrk` or `oha`) targeting
  `/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065`
- Run 1000 requests at concurrency 10
- Record p50, p95, p99 latencies
- **PASS**: p95 ≤ 200ms
- **FAIL**: Optimize query (EXPLAIN ANALYZE, index coverage check)
  and re-test

### 2.3 Station Detail Endpoint (Mobile)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/v1/stations/:id` | Station detail with chargers list |
| GET | `/api/v1/stations/:id/chargers` | Chargers for a specific station |

**Checkpoint**: Nearby query p95 ≤ 200ms. `is_test = true` stations
never appear when `include_test` is absent or false.

---

## Phase 3: Admin Portal — Shell, Navigation & BaseMap

**Duration**: 4 days | **Dependencies**: Phase 0 (frontend scaffold)
+ Phase 1 (API available)

**Deliverable**: Running admin portal with AppShell layout, sidebar
navigation, and interactive map showing station markers.

### 3.1 AppShell & Layout

File: `sources/frontend/apps/admin-portal/src/`

- `<AppShell/>` — sidebar + main content area
- `<SidebarNav/>` — 6 navigation items per
  docs/02-admin-workspace-topology.md:
  Overview, Users, Data, Analytics, Security, Settings
- React Router with nested routes for each section
- Sandbox Workspace Selector toggle in header
- When active: `border-t-4 border-sky-500` on entire layout

### 3.2 Design System Package

File: `sources/frontend/packages/ui/`

- `tailwind.config.ts` — all tokens from docs/03-web-admin-ux-spec.md
  (colors, radii, spacing, shadows)
- `<ScrollableTable/>` — per docs/06-defensive-ux-guardrails.md:
  min-width 800px, horizontal scroll, no word-break
- `<SettingsCard/>` — `rounded-2xl`, `shadow-card`, `p-6`
- `<SelectSetting/>` — `rounded-md`, token-driven dropdown
- `<ConfirmDeleteModal/>` — per docs/06-defensive-ux-guardrails.md:
  input field + exact-match validation + disabled button

### 3.3 BaseMap Component

File: `sources/frontend/apps/admin-portal/src/components/map/BaseMap.tsx`

- Full viewport canvas per docs/03-web-admin-ux-spec.md
- CartoDB light tiles, Tunisia center `[33.8869, 9.5375]`, zoom 7
- Station markers: green circle + lightning bolt SVG per spec
- Fetch stations from `/api/v1/stations` on mount
- Click marker → navigate to station detail or open popup

### 3.4 Overview Dashboard (Placeholder)

- Metric chips: total stations, total chargers, total partners
- Map showing all stations
- Placeholder cards for analytics (post-MVP0)

**Checkpoint**: Admin portal renders with sidebar navigation, map shows
seed station markers, sandbox toggle shows blue border indicator.

---

## Phase 4: Admin Portal — Data Views & CRUD

**Duration**: 5 days | **Dependencies**: Phase 3 + Phase 1 (API)

**Deliverable**: Full CRUD management for Partners, Stations, Chargers,
and Connector Types with defensive UX guardrails.

### 4.1 Data Section — Partners Registry

Route: `/data/partners`

- `<ScrollableTable/>` listing: ID, Display Name, Classification
  (Business/Private), Tax ID, Contact Phone, Created
- Create partner form: user fields + partner profile fields
- Classification toggle: Business → shows tax_id; Private → hides it
- Edit inline or modal form
- Delete: `<ConfirmDeleteModal/>` requiring exact `PRT-` ID input

### 4.2 Data Section — Stations

Route: `/data/stations`

- `<ScrollableTable/>` listing: ID, Name, City, Owner, Coordinates,
  Operational, is_test
- Create station form: name, address, city, coordinates (map picker or
  lng/lat inputs), owner dropdown (from partners), is_operational toggle
- Map integration: clicking a station on the BaseMap highlights row in
  table; clicking table row pans map to station
- Delete: `<ConfirmDeleteModal/>` requiring exact `STN-` ID input

### 4.3 Data Section — Chargers

Route: `/data/chargers` (nested under station detail or flat view)

- `<ScrollableTable/>` listing: ID, Station, Connector Type, Power kW,
  Current Type, Status
- Create charger form: connector type dropdown (dynamically populated
  from `station_connector_types` — per docs/02-admin-workspace-topology.md
  cross-workspace dependency), power_kw, current_type, status
- Status badge: available (green), occupied (amber), faulted (red),
  offline (gray)
- Delete: hard delete with `<ConfirmDeleteModal/>` requiring `CHG-` ID

### 4.4 Settings — Infrastructure Types

Route: `/settings/infrastructure-types`

- CRUD list for `station_connector_types`
- Name (unique), Description fields
- Save → persists to DB → immediately available in Chargers dropdown
  (cross-workspace dependency flow per docs/02-admin-workspace-topology.md)
- Delete: `<ConfirmDeleteModal/>` requiring `CNT-` ID
- RESTRICT check: if type is in use by chargers, show error instead of
  allowing delete

### 4.5 Settings — App Settings (Placeholder)

Route: `/settings/app`

- Placeholder cards for branding, map tokens, dropzones
- Non-functional in MVP0 (structure only)

**Checkpoint**: Admin can create/edit/delete all entity types.
`<ScrollableTable/>` prevents layout breakage. Destructive modals
enforce exact ID match. Connector type dropdown updates dynamically
after creating a new type in Settings.

---

## Phase 5: Partner Dashboard — Multi-Tenant Views

**Duration**: 4 days | **Dependencies**: Phase 1 (API + auth)

**Deliverable**: Partner-scoped dashboard where operators see only
their own stations and chargers.

### 5.1 Partner Auth & Context

- Login flow: partner user authenticates → JWT contains `user_id` +
  `role = partner`
- Backend middleware extracts `owner_id` from JWT
- All partner-scoped endpoints automatically filter by `owner_id`
  (Constitution Principle II: multi-tenancy at extraction tier)

### 5.2 Partner App Shell

File: `sources/frontend/apps/partner-dashboard/src/`

- Simplified layout: sidebar with Station List, Chargers, Profile
- Reuses `<ScrollableTable/>` and design tokens from `packages/ui/`
- No admin-level navigation (no Settings, no Users, no Analytics)

### 5.3 Partner Station Management

- List: only stations where `owner_id` matches authenticated partner
- Create/Edit station: same form as admin but `owner_id` locked
- Chargers management: same CRUD, scoped to partner's stations
- Delete: `<ConfirmDeleteModal/>` with exact ID match

### 5.4 Partner Profile View

- View/edit own partner profile (display_name, contact_phone, logo_url)
- Cannot change classification or tax_id without admin approval (MVP0:
  read-only for these fields)

**Checkpoint**: Partner dashboard shows only the partner's own data.
Attempting to access another partner's station ID returns 403/404.

---

## Phase 6: Mobile Driver App — Map Discovery

**Duration**: 4 days | **Dependencies**: Phase 2 (nearby API)

**Deliverable**: Expo Go mobile app with full-viewport map, nearby
station discovery, and station detail bottom sheet.

### 6.1 Expo Project Setup

File: `sources/frontend/apps/mobile-driver/`

- `npx create-expo-app` with SDK 51 template
- Install locked dependencies per docs/04-mobile-driver-ux-spec.md:
  `expo-router`, `react-native-maps`, `expo-location`,
  `@gorhom/bottom-sheet`, `react-native-reanimated`,
  `react-native-gesture-handler`, `expo-haptics`,
  `@react-native-async-storage/async-storage`
- **Never** run `expo eject` or `expo prebuild`
- File-based routing with `expo-router`

### 6.2 Map Canvas Screen

- Full viewport `react-native-maps` with Tunisia center
- Request device location via `expo-location`
- On location received: `GET /api/v1/stations/nearby?longitude=...&latitude=...`
- Default radius 20km, no `include_test` parameter (defaults false)
- Render station markers (green circle + bolt icon)
- `react-native-maps` marker clustering if >20 results

### 6.3 Station Detail Bottom Sheet

- Tap marker → `@gorhom/bottom-sheet` slides up
- Display: station name, address, city, distance, available charger count
- Charger list: connector type, power, current type, status badge
- `expo-haptics` feedback on marker tap
- "Navigate" button (opens device maps with station coordinates)

### 6.4 Search & Filter

- Radius slider: 5km / 10km / 20km / 50km
- Re-fetch on radius change
- Pull-to-refresh on map

**Checkpoint**: Mobile app loads in Expo Go, shows nearby stations on
map, bottom sheet displays station + charger details, no test stations
visible.

---

## Phase 7: Integration Validation & Polish

**Duration**: 3 days | **Dependencies**: All prior phases

**Deliverable**: End-to-end validated MVP0 with SLO confirmation and
constitutional compliance audit.

### 7.1 End-to-End Flow Validation

| Flow | Steps |
|------|-------|
| Admin creates partner + station + chargers | Login → Partners → Create → Stations → Create → Chargers → Create → Verify on map |
| Partner manages own stations | Login as partner → See only own stations → Edit charger status → Verify mobile shows update |
| Driver discovers station | Open mobile app → Grant location → See nearby stations → Tap for detail → Navigate |
| Sandbox isolation | Admin toggles sandbox → Sees test data with blue border → Mobile app does NOT show test data |
| Destructive confirmation | Admin attempts delete → Modal appears → Wrong ID → Button disabled → Correct ID → Button enabled → Confirm |

### 7.2 SLO Re-Validation

- Re-run benchmark from Phase 2.2 against full schema + seed data
- Confirm p95 ≤ 200ms at concurrency 10
- Document results in `docs/performance-baseline.md`

### 7.3 Constitutional Compliance Audit

| Principle | Audit Item | Pass Criteria |
|-----------|-----------|---------------|
| I | All code under `sources/` | Directory tree matches spec |
| I | API namespace `/api/v1/*` | No unversioned endpoints |
| II | Semantic ID format | Every generated ID matches `[PREFIX]-[12-char]` |
| II | `is_test` filter on mobile | Nearby endpoint omits test records by default |
| II | Soft delete on users/partners/stations | `deleted_at IS NOT NULL` rows excluded |
| II | Multi-tenant `owner_id` injection | Partner endpoints filter by authenticated user |
| III | Centralized design tokens | No hardcoded hex in any `.tsx` file |
| III | `<ScrollableTable/>` on data views | All data tables use component |
| III | Destructive confirmation modal | Delete actions require exact ID match |
| III | Sandbox indicator | `border-t-4 border-sky-500` visible when sandbox active |
| IV | Expo Go managed | No `expo eject` / `expo prebuild` in history |
| IV | 20km default radius / LIMIT 50 | API defaults verified |
| V | Modular domain layers | Domain modules have clean interfaces |
| V | Deterministic seed | `sqlx migrate run` + seed produces identical data |

### 7.4 Polish Items

- Error boundaries in React apps
- Loading states / skeletons on data fetches
- Toast notifications for CRUD success/failure
- Mobile: handle location permission denied gracefully
- Consistent `accent` color usage across all three apps

**Checkpoint**: All audit items pass. E2E flows complete without errors.
SLO confirmed. MVP0 ready for demo.

---

## Timeline Summary

| Phase | Duration | Cumulative |
|-------|----------|-----------|
| 0: Project Scaffolding & CI/CD | 3 days | Day 3 |
| 1: Backend Core | 5 days | Day 8 |
| 2: Spatial Discovery | 3 days | Day 11 |
| 3: Admin Portal Shell | 4 days | Day 15 |
| 4: Admin Data Views | 5 days | Day 20 |
| 5: Partner Dashboard | 4 days | Day 24 |
| 6: Mobile Driver App | 4 days | Day 28 |
| 7: Integration & Polish | 3 days | Day 31 |

**Total estimated**: 31 calendar days

## Dependency Graph

```
Phase 0 ──► Phase 1 ──► Phase 2 ──► Phase 6 (Mobile)
   │              │
   │              └──► Phase 5 (Partner Dashboard)
   │
   └──► Phase 3 ──► Phase 4 ──► Phase 7 (Integration)
                                       ▲
               Phase 5 ────────────────┘
               Phase 6 ────────────────┘
```

Parallel opportunities:
- Phase 3 + Phase 5 can overlap (different apps, shared API)
- Phase 6 can start as soon as Phase 2 is complete
- Phase 4 + Phase 5 can overlap after Phase 3 completes

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Spatial query misses SLO | Blocks mobile UX | Phase 2 benchmarks early; GIST index + LIMIT 50 as safety net |
| Expo dependency conflict | Blocks mobile app | Locked versions from day 0; test early in Phase 0.3 |
| Multi-tenant data leak | Security incident | Repository-layer `owner_id` injection; Phase 7 audit |
| Docker Compose instability | Dev environment blocked | Healthcheck on postgres; volume persistence; Phase 0.2 verification |
