<!--
  Sync Impact Report
  Version change: 0.0.0 (template) → 1.0.0
  Modified principles: All 5 template principles replaced with 11 project-specific principles
  Added sections:
    - Technology Stack & Architecture (replacing generic SECTION_2)
    - Development Workflow & Quality (replacing generic SECTION_3)
  Removed sections: None — all template placeholders replaced with concrete content
  Templates requiring updates: ✅ plan-template.md (no change needed — generic Constitution Check gate remains valid)
    ✅ spec-template.md (no change needed)
    ✅ tasks-template.md (no change needed)
    ✅ checklist-template.md (no change needed)
  Follow-up TODOs: None — all placeholders resolved
-->

# BorneMap Constitution

## Core Principles

### I. MVP-First Delivery
Build the minimum that proves the core loop works. Validate before adding
complexity. Never introduce infrastructure the current MVP does not need.

### II. Layered Complexity
Each MVP adds one layer on top of a stable foundation. Nothing from a previous
MVP is broken by a later one.

### III. Dashboard First
The Dashboard App MUST always be built before the driver apps within any MVP
that introduces new data entities. Data must exist before discovery is
meaningful.

### IV. Single Source of Truth
Every entity has exactly one authoritative owner. No ambiguity about where data
is written or read from.

### V. Simple Operations
The platform MUST be operable by one person. Every operational task MUST have a
documented runbook.

### VI. Domain Separation by Schema
Business data, GIS data, user data, and analytics data are separated by
PostgreSQL schema and service responsibility. Cross-schema writes are forbidden
except where explicitly permitted.

### VII. Public Access Is a First-Class Concern
Anonymous public browsing MUST always work. Auth is triggered only at the
moment a gated action is attempted.

### VIII. RTL and Arabic Are Not Afterthoughts
Arabic language support and RTL layout MUST be built from the start of MVP-3.
Any RTL failure from that point is a Class A bug.

### IX. Visual Consistency Across All Surfaces
All three applications share the same design token foundation defined in
`source/packages/ui`. No hardcoded visual values in application code.

### X. API Prefix Consistency
All backend endpoints are served under the `/api` prefix. This applies to
json-server in MVP-1 and all Rust services from MVP-2 onward.

### XI. Tooling Separation
Code is written in SpecKit. UX and UI design is done in Impeccable. Planning,
architecture, and documentation stay in this assistant. These roles do not
overlap.

## Technology Stack & Architecture

### Backend Services
- **MVP-1**: json-server on port 3001, data from `source/mock/db.json`, routes
  mapped via `source/mock/routes.json`
- **MVP-2+**: Rust Actix-web services — `driver-service` (port 8080),
  `admin-service` (port 8081), `clickstream-service` (port 8082)
- **MVP-3+**: Keycloak for authentication
- **MVP-6+**: Traefik as the sole public-facing entry point

### Frontend Applications
- **Driver Web App**: React + Vite, Leaflet + OpenStreetMap, full-bleed map
  layout
- **Driver Mobile App**: React Native + Expo SDK 54, `react-native-maps`,
  full-bleed map layout
- **Dashboard App**: React + Vite, fixed left sidebar layout
- Expo SDK 54 exclusively — no upgrade without an approved ADR
- WCAG 2.1 AA on all web apps from MVP-3
- Arabic and French language support from MVP-3

### Design System
Defined in `source/packages/ui` — single source of truth for all visual values:
- **Color tokens**: brand, surface, text, border, status, neutral scales
- **Typography**: Plus Jakarta Sans / Inter for driver apps, Inter for
  dashboard, Cairo for Arabic
- **Spacing**: 4px base, scale 4 8 12 16 20 24 32 40 48 64 80 96
- **Radius**: sm(4) md(8) lg(12) xl(16) 2xl(20) 3xl(24) full(9999)
- **Shadows**: card, panel, float, pin
- **Delivery**: `tailwind.config.base.js` for web, `tokens/native.ts` for
  mobile
- `native.ts` MUST stay synchronized with `colors.ts`

### Data Architecture
- **MVP-1**: All data in `source/mock/db.json` with integer IDs
- **MVP-2+**: PostgreSQL database `ev_platform` with schema separation:
  - `inventory`: partner, station, charger, station_availability
  - `users`: user_account, user_profile, partner_membership, favorite_station,
    station_review
  - `gis`: roads, boundaries, station_locations (populated MVP-4)
  - `analytics`: raw_events, event_aggregates (MVP-5+)
- `inventory.station` is the source of truth for stations — always. gis is
  never master of any business entity.
- **Identifier prefixes**: USR- (user), PRT- (partner), STN- (station),
  CHG- (charger), REV- (review), EVT- (event)
- No sequential integers in public APIs from MVP-2 onward

### GIS Synchronization (MVP-4)
- PostgreSQL trigger on `inventory.station` calls `gis.sync_station()` on
  INSERT / UPDATE / DELETE
- Trigger fires within the same transaction; GIS failure logs WARNING but does
  not block the station write
- `gis.resync_all_stations()` rebuilds all GIS artifacts on demand
- No application code ever writes to `gis.station_locations` directly

### Authentication & Authorization (MVP-3+)
- Keycloak owns all authentication
- JWT validated against JWKS (cached, never fetched per request)
- First-login provisioning via upsert on `keycloak_sub`
- Role enforcement in Actix-web middleware before handlers
- Partner scope via JWT `partner_id` claim
- Web tokens in memory only — never `localStorage`
- Mobile tokens in `expo-secure-store` — never `AsyncStorage`
- Auth triggered only at gated action — never proactively

### Analytics (MVP-5+)
- Frontend → POST `/api/events` → Clickstream Service →
  `analytics.raw_events`
- No message broker. Direct PostgreSQL insert.
- All events in canonical taxonomy. Unknown names rejected with 400.
- Analytics errors always swallowed silently in frontend.

### Roles and Access Model
- **Public Driver**: Anonymous. No login required. Introduced MVP-1.
- **Registered Driver**: Authenticated via Keycloak role
  `registered_driver`. Introduced MVP-3.
- **Partner**: Authenticated via Keycloak role `partner`. Belongs to exactly
  one partner (enforced by `users.partner_membership` PK on `user_id`). All
  operations scoped to own data (enforced by JWT `partner_id` claim).
  Introduced MVP-3.
- **Admin**: Authenticated via Keycloak role `admin`. Introduced MVP-3.

### Non-Negotiable Rules
- All endpoints under `/api` — from MVP-1, always
- Every service has `GET /api/health`
- All SQL uses bind parameters (from MVP-2)
- No sequential integers in public APIs (from MVP-2)
- Only Traefik exposes public ports (from MVP-6)
- Keycloak owns auth (from MVP-3)
- No registry — images built on host (from MVP-2)
- Secrets never in committed files
- No hardcoded visual values — tokens only, from MVP-1
- Tokens never in `localStorage` or `AsyncStorage` (from MVP-3)
- RTL correct on every screen in Arabic (from MVP-3)
- `native.ts` synchronized with `colors.ts`
- Expo SDK 54 — no upgrade without ADR
- OpenStreetMap — no paid provider without ADR
- Analytics errors never surface to user (from MVP-5)
- OCPP, payments, routing — require ADR and plan revision to introduce

## Development Workflow & Quality

### MVP Cycle Strategy
MVPs are delivered sequentially. Each MVP builds on a stable foundation:
- **MVP-1**: json-server mock API + Dashboard first + driver apps (public
  discovery)
- **MVP-2**: Rust services + PostgreSQL + PostGIS + CI/CD
- **MVP-3**: Authentication + user management + RTL + Arabic
- **MVP-4**: GIS synchronization
- **MVP-5**: Analytics and reporting
- **MVP-6**: Production hardening + Traefik + launch

### Definition of Done
**Sprint Done**:
- All tasks complete or deferred with written reason
- No Class A bugs open
- All tests pass
- Manual smoke test completed
- Sprint summary written

**MVP Done**:
- All sprints meet Sprint Done criteria
- Phase status file complete
- Documentation updated
- Zero Class A bugs
- Hardening sprint completed
- Onboarding guide tested from scratch

### Bug Classification
- **Class A**: Blocks correctness or user access. Fixed before MVP closes.
- **Class B**: Degrades quality. Fixed before target MVP.
- **Class C**: Improvement. No mandatory target.

### Decision Records
- ADRs in `docs/adr/`. Required before any non-trivial architecture change.
- Small decisions in `docs/project/decisions.md`.
- ADRs never edited — a superseding decision gets a new ADR.

## Governance

### Amendment Procedure
1. Proposed changes MUST be documented with rationale.
2. Changes to core principles require an ADR.
3. Minor clarifications and wording fixes may be applied directly.
4. After amendment, `LAST_AMENDED_DATE` is updated and version is bumped per
   the versioning policy below.

### Versioning Policy
- **MAJOR**: Backward-incompatible governance/principle removals or
  redefinitions.
- **MINOR**: New principles/sections added or materially expanded guidance.
- **PATCH**: Clarifications, wording, typo fixes, non-semantic refinements.

### Compliance Review
- All plans MUST include a Constitution Check gate before Phase 0 research
  begins.
- Complexity MUST be justified when constitution principles are violated.
- Every sprint review MUST verify compliance with constitution principles.

**Version**: 1.0.0 | **Ratified**: 2026-06-08 | **Last Amended**: 2026-06-08
