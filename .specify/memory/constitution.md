<!--
  Sync Impact Report
  ==================
  Version change: 0.0.0 (template) → 1.0.0
  Type: Initial population from template
  Modified principles: All 5 principles populated from source/docs/constitution.md
    - [PLACEHOLDER] → I. Validation Before Optimization
    - [PLACEHOLDER] → II. Strict Service Topology
    - [PLACEHOLDER] → III. Compile-Time Safety & Type Strictness
    - [PLACEHOLDER] → IV. Read/Write Separation & Transactional Integrity
    - [PLACEHOLDER] → V. Security & Identity Isolation
  Added sections:
    - Tech Stack & Platform Constraints (was SECTION_2)
    - Development Workflow & Conventions (was SECTION_3)
    - Governance with version footer
  Removed sections: None
  Templates requiring updates:
    - .specify/templates/plan-template.md: ✅ No changes needed (dynamic Constitution Check)
    - .specify/templates/spec-template.md: ✅ No changes needed
    - .specify/templates/tasks-template.md: ✅ No changes needed
    - .specify/templates/checklist-template.md: ✅ No changes needed
    - Command files in extensions/: ✅ No outdated agent references found
  Deferred TODOs: None — all placeholders resolved
-->

# BorneMap Constitution

## Core Principles

### I. Validation Before Optimization

Fast product validation through rapid iteration ("Validation before Optimization").

The platform SHALL NOT include any of the following during the validation phase:
OCPP integration or direct hardware communications; native billing or payment
processing workflows; smart charging optimization or grid load balancing telemetry;
real-time hardware status metrics or continuous charger telemetry tracking;
distributed event-driven streaming engines (Kafka, RabbitMQ, MQTT); native mobile
compilation pipelines outside Expo Go; infrastructure autoscaling policies or
advanced distributed tracing stacks.

### II. Strict Service Topology

Exactly three Actix-web microservices are defined:

- **Auth Service (:3000)**: Sole owner of the `users` schema. Integrates directly
  with the single `bornemap` Keycloak realm. No other service interacts with
  Keycloak directly.
- **Driver Service (:3001)**: Geospatial read API, inventory write operations,
  Redis cache management. Owns `inventory` schema read patterns and user
  relationship records (favorites, reviews).
- **Admin Service (:3002)**: Partner infrastructure management and analytics
  logging into the isolated `analytics_db`.

No additional microservices may be introduced without a constitution amendment
recorded via ADR.

### III. Compile-Time Safety & Type Strictness

Every Rust database query to `platform_db` MUST use compile-time type-checked
`sqlx` macros. No raw unvalidated string concatenation is permitted.

TypeScript strict mode is non-negotiable — the `any` keyword is strictly
prohibited across all user-facing applications.

Code formatting enforced via `rustfmt` + `clippy` (Rust) and `eslint` + `prettier`
(TypeScript).

### IV. Read/Write Separation & Transactional Integrity

The Driver Service functions as a read-optimized spatial data API via PostGIS
SQL functions and Redis caching, while handling its own driver-scoped
transactional writes. No asynchronous outbox patterns are deployed during the
validation phase.

All multi-table data modifications within any microservice MUST be wrapped in a
single database transaction (Unit of Work). Writes to `inventory.station` or
`inventory.charger` trigger synchronous cache-bust operations on the Redis
spatial cache managed by the Driver Service.

### V. Security & Identity Isolation

Single Keycloak mono-realm (`bornemap`). Access profiles are isolated via
granular Client Roles (`role:driver`, `role:partner`, `role:admin`) across
distinct Keycloak Clients (`mobile-driver-app`, `web-driver-app`,
`admin-partner-dashboard`).

Cleartext credentials, API keys, or security vectors are completely barred from
git tracking — handled via environmental injection using gitignored `.env` files.

Central TLS termination managed via Traefik (from MVP-6). Application-layer
access tokens processed against Keycloak JWT validation steps.

Soft delete enforced exclusively on infrastructure entities (stations, chargers,
partners) — never on users, core access configurations, or audit logs.

## Tech Stack & Platform Constraints

| Layer | Technology | Constraint |
|-------|-----------|------------|
| Mobile Driver App | Expo SDK 54 (locked), React Native, AsyncStorage | No native modules outside Expo Go before validation. Offline fallback via AsyncStorage snapshot cache. Coordinate inputs validated through shared types. |
| Web Driver App | React + Leaflet | Custom markers bundled locally. Styling via shared Tailwind tokens. Coordinate inputs validated through shared types. |
| Dashboard | React + Tailwind CSS + shadcn/ui + React Router v6 + Framer Motion (transitions only) + React Query | Framer Motion limited to route transitions |
| Backend Services | Rust / Actix-web | From MVP-1 onward |
| Shared Backend | Cargo workspace (`crates/db-models`, `crates/validation`) | sqlx compile-time queries |
| Shared Frontend | TypeScript packages (`packages/shared-types`, `shared-hooks`, `shared-ui`) | strict mode, no `any`. Data fetching, auth, and types shared; map views NOT shared (web=Leaflet, mobile=react-native-maps) |
| Database | PostgreSQL 16 + PostGIS | Single `platform_db` with `gis`, `inventory`, `users` schemas; separate `keycloak_db` and `analytics_db` |
| Identity | Keycloak | Single `bornemap` realm. Web: tokens in memory or secure browser state. Mobile: tokens in secure device storage. |
| Cache | Redis | GIS spatial tile cache managed by Driver Service from MVP-5 |
| Gateway | Traefik | TLS, routing from MVP-6 |
| Monorepo Root | `source/` | — |

**Entity ID Prefixes** (NanoID): `USR_` (user), `OPR_` (partner/operator),
`STA_` (station), `CHG_` (charger).

## Development Workflow & Conventions

**Monorepo layout** (`source/`):
- `apps/` — Frontend applications (mobile-driver, web-driver, dashboard)
- `services/` — Actix-web microservices (auth-service, driver-service, admin-service)
- `packages/` — Shared TypeScript workspace (shared-types, shared-hooks, shared-ui)
- `crates/` — Shared Rust workspace (db-models, validation)
- `infra/` — Infrastructure (docker-compose.yml, keycloak/, osm-importer/)
- `docs/` — Documentation including ADR records

**Naming conventions**: Services kebab-case with `-service` suffix; apps
kebab-case descriptive; packages/crates kebab-case with domain prefix (`shared-`,
`db-`).

**Core domain rules**:
- Admin-only partner creation via invitation or admin-validated self-registration
- Companies (partners) are the top-level grouping — no independent "networks" layer
- Private home chargers are first-class public map entities alongside commercial stations
- Private and commercial stations share identical schema constraints; specializations via nullable metadata

**Schema ownership**:
- `gis` — OpenStreetMap spatial reference data (roads, boundaries, cities)
- `inventory` — Operational infrastructure (partner, station, charger) + user interactions
- `users` — User profile mapping (owned by Auth Service, keyed to Keycloak `sub`)

**API versioning**: All endpoints prefixed with `/api/v1/`. Major version on
breaking changes only. Additive modifications inline.

## Frontend Presentation & Interaction Rules

**State-Driven Interface Checklist**: Every API-interacting screen MUST implement four states:
- **Loading**: Shimmer skeletons mirroring the target card layout (no spinners or blank screens).
- **Success**: Smooth layout animations (Framer Motion for web, LayoutAnimation for React Native).
- **Empty**: Illustrative feedback guiding users to pan to major cities (Tunis, Sousse, Sfax).
- **Error**: Structural error boundary with prominent "Retry Connection" button.

**Map Interaction**:
- Viewport debounce ≥ 300ms before querying `/api/v1/nearby`.
- Zoom-out past threshold: hide markers + overlay "Zoom in closer to view available charging stations."

**Mobile**:
- Zero custom native modules — must run in default Expo Go.
- Successful nearby queries update AsyncStorage coordinate snapshot cache.
- Offline: read AsyncStorage cache, render markers, show "Viewing cached data" banner.

**Web**:
- All styling via shared Tailwind config (`packages/shared-ui`).
- Marker SVGs/PNGs bundled locally and pre-loaded.

**Security**:
- Web: JWTs in memory or secure browser state. Mobile: JWTs in secure device storage.
- Coordinate data must pass through shared validation before reaching API query strings.

## Governance

This constitution is the final authority for architectural structure. Direct
conflicts between source implementations and configuration files are resolved in
favor of this document.

- **Amendments**: Modifications to fundamental sections (Core Principles, Tech
  Stack, Architectural Principles, or Prohibitions) strictly require an
  accompanying ADR recorded inside `docs/adr/`.
- **Evolutionary sections**: Monorepo structure, entity lists, service boundaries,
  and roadmap tasks can adapt dynamically without a formal ADR, provided this
  document is updated immediately.
- **AI Model Compliance**: Coding LLMs MUST parse this document alongside
  `.speckit/rules.md`. Any violation of the SpecKit prohibitions (Section 7 of
  `source/docs/constitution.md`) constitutes a blocking compliance error.
- **Documentation sync**: Before completing any task, update
  `docs/roadmap_status.md`, `docs/sprint_backlog.md`, and `docs/system_state.md`.

**Version**: 1.0.0 | **Ratified**: 2026-06-17 | **Last Amended**: 2026-06-17
