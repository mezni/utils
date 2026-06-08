# AGENTS.md

Guide for OpenCode agents working in BorneMap.

## Project Overview

**BorneMap** is an EV station discovery and management platform for Tunisia. Four user types: Public Driver (anonymous), Registered Driver (authenticated), Partner (station owner), Admin (platform). MVP-1 is in progress with Python FastAPI backend and three React/React Native frontends.

**Philosophy**: Build in MVP cycles. Validate product before adding infrastructure. Never break what a previous MVP delivered. See `constitution.md` and `implementation-plan.md` for full guidance.

## Architecture and Stack

### MVP-1 Current State
- **Backend**: Python FastAPI, one service at `source/services/bornemap-service/`, port 8000
- **Database**: PostgreSQL named `ev_platform`, two schemas: `inventory` (partner, station, charger) and `gis` (empty, reserved for MVP-4)
- **Frontend Apps**: 
  - Driver Web (React + Vite + Leaflet + OpenStreetMap)
  - Driver Mobile (React Native + Expo SDK 54 + react-native-maps)
  - Dashboard (React + Vite)
- **Design tokens**: Single source of truth in `source/packages/ui/`

### Critical Non-Negotiable Rules

**API Prefix**: Every endpoint in every service, every MVP, starts with `/api`. Never expose an endpoint without this prefix. This includes `/api/health`.

**Public Access First**: Anonymous browsing must always work. Authentication never blocks station discovery, map, or search. Auth triggered only when a gated action is attempted (favorites, reviews, profile).

**Single Source of Truth by Schema**: Business data lives in `inventory` schema, owned by services. No cross-schema business writes. GIS schema never owns business entities.

**Design Tokens**: Zero hardcoded visual values anywhere. All colors, spacing, typography come from `source/packages/ui/src/tokens/colors.ts` (web) or `source/packages/ui/src/tokens/native.ts` (mobile). Token changes made once, consumed everywhere.

**Identifiers in MVP-1**: All UUIDs. NanoID-prefixed identifiers (STN-..., PRT-..., CHG-...) introduced in MVP-2.

## Installed Skills

- **impeccable**: Design review, UI polish, visual hierarchy, RTL audit. Use for any frontend interface work.
- **frontend-design**: Build production web components and layouts.
- **rust-best-practices**: Guidance for Rust (used from MVP-2).
- **git-guardrails-claude-code**: Prevent destructive git operations.
- **find-skills**: Discover additional skills as needed.

## Development Workflow

### Running MVP-1 Locally

**Prerequisites**: Python 3.11+, PostgreSQL 15+, Node 18+, Expo CLI

**Backend**:
```bash
cd source/services/bornemap-service
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://user:pass@localhost/ev_platform
alembic upgrade head
python -m uvicorn app.main:app --reload --port 8000
```

**Dashboard Web App**:
```bash
cd source/apps/dashboard
npm install
npm run dev  # runs on http://localhost:5173
```

**Driver Web App**:
```bash
cd source/apps/driver-web
npm install
npm run dev  # runs on http://localhost:5174
```

**Driver Mobile App**:
```bash
cd source/apps/driver-mobile
npm install
npx expo start
# Press 'i' for iOS simulator or 'a' for Android emulator
```

### Database Setup

- **Database name**: `ev_platform`
- **Migrations location**: `source/services/bornemap-service/migrations/` (Alembic)
- **Seeds**: Dev seeds with 3 partners, 15 Tunisian stations, 24 chargers
- **Schema naming**: Use `inventory.` prefix for business tables. `gis.` is reserved.

### Testing and Verification

**Backend smoke tests**: One per endpoint, happy path + not-found case
```bash
cd source/services/bornemap-service
pytest tests/
```

**Validation order**: Backend tests → Frontend tests → Full loop verification (create in Dashboard → visible in Driver apps)

**Must-pass checks before commit**:
- All endpoints return correct HTTP status codes
- `GET /api/health` returns `{"status":"ok","service":"bornemap-service","db":"ok"}`
- Nearby endpoint returns stations ordered by distance
- No N+1 queries on station detail endpoint
- All filter dropdowns in Dashboard populated from real API

## Design and Frontend Rules

### Layout Patterns (Built in MVP-1)

**Driver apps** (Web + Mobile):
- Full-bleed map with floating search/filter UI on top
- Bottom sheet or card for station detail
- Bottom tab bar with raised center action button (mobile only)
- Available stations: `brand.glow` (#00E676) marker
- Unavailable stations: `status.maintenance` (#EF4444) marker

**Dashboard**:
- Fixed 64px left sidebar, white background
- Top 64px header bar
- Main content on `surface.background`
- Active nav item: `background: brand.sageLight`, `text: brand.primary`

### Token Organization

**Colors** (from `colors.ts`):
- `brand.primary` (#007943) — CTAs, active states
- `brand.sageLight` (#EAF0E6) — selected/active states
- `brand.glow` (#00E676) — live map markers (driver apps only)
- `status.available` (#10B981), `status.inUse` (#F59E0B), `status.maintenance` (#EF4444)
- `surface.background`, `surface.card`, `surface.sidebar` — page canvas
- `text.main` (#111827), `text.muted` (#6B7280)

**Typography**:
- Driver apps: Plus Jakarta Sans
- Dashboard: Inter
- Arabic (MVP-3+): Cairo font

**Spacing**: Base 4px. Scale: 4, 8, 12, 16, 20, 24, 32, 40, 48, 64, 80, 96

### Tailwind Configuration

Web apps extend `source/packages/ui/tailwind.config.base.js`. Mobile app imports `source/packages/ui/src/tokens/native.ts` as plain JavaScript for React Native `StyleSheet`.

**Critical rule**: When adding a token to `colors.ts`, sync it immediately to `native.ts` in the same commit.

## Data Model (MVP-1)

### Schemas and Tables

**`inventory` schema** (source of truth for stations):
```
partner (id UUID, name, created_at)
station (id UUID, partner_id → partner, name, address, latitude, longitude, created_at, updated_at)
charger (id UUID, station_id → station, connector_type, power_kw, status, updated_at)
```

**`gis` schema** (reserved, empty in MVP-1):
Populated in MVP-4 via trigger from `inventory.station`.

### Key Constraints
- All IDs are UUID v4 in MVP-1
- Latitude range: -90 to 90. Longitude range: -180 to 180
- Charger status enum: `available`, `in_use`, `maintenance`
- Connector types: `Type2`, `CCS`, `CHAdeMO`, etc. (from API schema)

### Writing Data
Only the backend service writes to the database. Frontend apps are read-only until MVP-3 (auth). Dashboard and Driver Web submit forms → FastAPI writes.

## API Endpoints Reference (MVP-1)

All endpoints under `/api` prefix.

**Health**:
- `GET /api/health` → `{"status":"ok","service":"bornemap-service","db":"ok"}`

**Stations**:
- `GET /api/stations` — list all, optional `?partner_id=UUID`, returns charger counts
- `GET /api/stations/:id` — detail with full charger list
- `GET /api/stations/nearby?lat=X&lng=Y&radius_km=50` — ordered by Euclidean distance, returns `distance_m`
- `POST /api/stations` — create, required: name, address, latitude, longitude, partner_id
- `PUT /api/stations/:id` — update fields
- `DELETE /api/stations/:id` → 204

**Partners**:
- `GET /api/partners` — list all
- `GET /api/partners/:id` — detail
- `POST /api/partners` — create, required: name
- `PUT /api/partners/:id` — update name
- `DELETE /api/partners/:id` → 204

**Chargers**:
- `GET /api/chargers` — list all, optional `?station_id=UUID`
- `GET /api/chargers/:id` — detail
- `POST /api/chargers` — create, required: station_id, connector_type, power_kw
- `PUT /api/chargers/:id` — update (status is primary use case in MVP-1)
- `DELETE /api/chargers/:id` → 204

See `docs/api/bornemap-service.md` for request/response shapes (to be written in Sprint 1.5).

## Documentation Structure

```
docs/
  constitution.md          ← Full rules and principles
  implementation-plan.md   ← MVP roadmap and sprint tasks
  glossary.md
  out-of-scope-registry.md ← Permanently deferred: OCPP, payments, routing, realtime
  adr/                     ← Architecture Decision Records
  api/                     ← Service API documentation
  schema/                  ← Database schema docs
  design/                  ← Design system specs
  ops/                     ← Operational guides (TBD after MVP-2)
  project/
    backlog.md
    bugs.md
    decisions.md           ← Small decisions not worth an ADR
    sprints/               ← Sprint status files
    phases/                ← MVP phase status and done criteria
  testing/                 ← Test strategy, fixtures
  guides/                  ← Onboarding, deployment, etc.
```

## Decision Framework

**ADR Required** (Architecture Decision Record in `docs/adr/`):
- New service, infrastructure component, or data store
- Changing source of truth for any entity
- Changing auth or authorization model
- Changing Expo SDK version
- Changing map library or tile provider
- Changing MVP scope

**Decision File** (`docs/project/decisions.md`):
- Small choices that don't rise to ADR level
- Framework selection within a layer (e.g., form validation library)
- Tool or package choices

All decisions recorded before code. No surprises on review.

## Permanently Out of Scope

These are deferred indefinitely per constitution and will never be added without a new implementation plan:
- **OCPP** and charging sessions
- **Payments and billing**
- **Routing and navigation**
- **Real-time availability** (OCPP-driven)
- **Push notifications**

See `docs/out-of-scope-registry.md`.

## Documentation Navigation

All project documentation is in `docs/`:

- **[docs/README.md](docs/README.md)** — Index and how-to guide
- **[docs/constitution.md](docs/constitution.md)** — Permanent rules and principles (source of truth)
- **[docs/implementation-plan.md](docs/implementation-plan.md)** — MVP roadmap and sprint details
- **[docs/out-of-scope-registry.md](docs/out-of-scope-registry.md)** — Permanently deferred features
- **[docs/adr/](docs/adr/)** — Architecture Decision Records
- **[docs/project/decisions.md](docs/project/decisions.md)** — Small decisions recorded before code
- **[docs/project/backlog.md](docs/project/backlog.md)** — Feature backlog by MVP
- **[docs/project/bugs.md](docs/project/bugs.md)** — Bug tracker with classification
- **[docs/project/phases/mvp-01-status.md](docs/project/phases/mvp-01-status.md)** — Current phase status and sprint breakdown
- **[docs/api/bornemap-service.md](docs/api/bornemap-service.md)** — API endpoint reference
- **[docs/schema/inventory-schema.md](docs/schema/inventory-schema.md)** — Database schema

**Start with `docs/README.md` for navigation and onboarding.**

## OpenCode Best Practices

- Use `TodoWrite` to track multi-step tasks (breaking them into smaller steps as needed).
- Mark todos `in_progress` before working and `completed` immediately after finishing each step.
- Prefer reading existing files with `Read` over guessing with `Bash` commands.
- Use `Task` agents for open-ended codebase exploration (`explore` agent) or complex multi-step research (`general` agent).
- Verify solutions with executable commands (tests, type checking, builds) rather than prose claims.
- When in doubt, inspect config files and scripts (sources of truth) before guessing at conventions.
