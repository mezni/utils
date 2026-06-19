# BorneMap Platform Architecture Constitution
## Revision 2 — June 2026

### I. Core Principles

1. **Validation before optimization**
Fast product validation through rapid iteration. The platform SHALL NOT include any of the following during the validation phase:

- OCPP integration or direct hardware communications
- Native billing or payment processing workflows
- Smart charging optimization or grid load balancing telemetry
- Real-time hardware status metrics or continuous charger telemetry tracking
- Distributed event-driven streaming engines (Kafka, RabbitMQ, MQTT)
- Native mobile compilation pipelines outside Expo Go
- Infrastructure autoscaling policies or advanced distributed tracing stacks

2. **Strict service topology**
Exactly three Actix-web microservices are defined:

- **Auth Service (:3000)**: Sole owner of the `users` schema. Integrates directly with the single `bornemap` Keycloak realm. No other service interacts with Keycloak directly. Clients never call Keycloak endpoints directly — all authentication flows are proxied through the Auth Service.
- **Driver Service (:3001)**: Geospatial read API, inventory write operations, Redis cache management. Owns `inventory` schema read patterns and user relationship records (favorites, reviews).
- **Admin Service (:3002)**: Partner infrastructure management and analytics logging into the isolated `analytics_db`.

No additional microservices may be introduced without a constitution amendment recorded via ADR.

3. **Compile-time safety & type strictness**
- Every Rust database query to `platform_db` MUST use compile-time type-checked `sqlx` macros. No raw unvalidated string concatenation is permitted.
- TypeScript strict mode is non-negotiable — the `any` keyword is strictly prohibited across all user-facing applications.
- Code formatting enforced via `rustfmt` + `clippy` (Rust) and `eslint` + `prettier` (TypeScript).

4. **Read/write separation & transactional integrity**
- The Driver Service functions as a read-optimized spatial data API via PostGIS SQL functions and Redis caching, while handling its own driver-scoped transactional writes.
- No asynchronous outbox patterns are deployed during the validation phase.
- All multi-table data modifications within any microservice MUST be wrapped in a single database transaction (Unit of Work).
- Writes to `inventory.station` or `inventory.charger` trigger synchronous cache-bust operations — initiated by Admin Service after `tx.commit()`, targeting the Driver Service-managed Redis spatial cache.

5. **Security & identity isolation**
- Single Keycloak mono-realm (`bornemap`). Access profiles are isolated via granular Client Roles (`role:driver`, `role:partner`, `role:admin`) across distinct Keycloak Clients (`mobile-driver-app`, `web-driver-app`, `dashboard-app`).
- Clients (mobile and web) MUST NOT call Keycloak token or admin endpoints directly. All auth flows (login, token refresh, logout) are routed through Auth Service REST endpoints (`/api/v1/auth/*`).
- Traefik validates JWTs locally using Keycloak public keys fetched via JWKS (`/realms/bornemap/protocol/openid-connect/certs`) and cached. Traefik does NOT call Keycloak token endpoints.
- Cleartext credentials, API keys, or security vectors are completely barred from git tracking — handled via environmental injection using gitignored `.env` files.
- Central TLS termination managed via Traefik (from MVP-6). Application-layer access tokens processed against Keycloak JWT validation steps.
- Soft delete enforced exclusively on infrastructure entities (stations, chargers, partners) — never on users, core access configurations, or audit logs.

### II. Authentication & Token Lifecycle

**Auth service as sole Keycloak proxy**
The Auth Service is the exclusive interface between the platform and Keycloak. No other service, client application, or infrastructure component may call Keycloak's token or admin APIs.

**Login flow**
1. Client POSTs credentials to `POST /api/v1/auth/login`.
2. Auth Service calls Keycloak internally: `POST /realms/bornemap/protocol/openid-connect/token`.
3. Keycloak returns `access_token` (JWT) + `refresh_token` to Auth Service.
4. Auth Service upserts a USR- profile row in `users` schema (`platform_db`).
5. Auth Service returns both tokens to the client.
6. Token storage: mobile — secure device storage; web — memory or secure browser state. Never `localStorage`.

**Authenticated request flow**
1. Client sends `Authorization: Bearer <access_token>` with every API request to Traefik.
2. Traefik validates the JWT signature, expiry (`exp`), issuer (`iss`), and audience (`aud`) locally using cached JWKS public keys.
3. On success: request is forwarded to the target service with `X-User-Id` and `X-User-Roles` headers injected.
4. On failure: Traefik returns `401 Unauthorized` immediately. No backend service is reached.

**Token refresh flow**
1. Client POSTs to `POST /api/v1/auth/refresh` with the `refresh_token`.
2. Auth Service calls Keycloak: `POST /token` with `grant_type=refresh_token`.
3. Keycloak rotates and returns new `access_token` and `refresh_token`.
4. Auth Service returns new tokens to the client.
5. If the `refresh_token` is expired, the client must re-authenticate via the login flow.

**Profile sync**
The Auth Service is the sole writer to the `users` schema in `platform_db`. On each successful login or token refresh, it upserts the user profile row keyed to the Keycloak `sub` claim. No other service reads from or writes to the `users` schema directly.

### III. Tech Stack & Platform Constraints

| Layer | Technology | Constraint |
|-------|-----------|-----------|
| Mobile driver app | Expo SDK 54 (locked), React Native, AsyncStorage | No native modules outside Expo Go before validation. Offline fallback via AsyncStorage snapshot cache. Coordinate inputs validated through shared types. |
| Web driver app | React + Leaflet | Custom markers bundled locally. Styling via shared Tailwind tokens. Coordinate inputs validated through shared types. |
| Dashboard | React + Tailwind CSS + shadcn/ui + React Router v6 + Framer Motion + React Query | Framer Motion limited to route transitions only. |
| Backend services | Rust / Actix-web | From MVP-1 onward. |
| Shared backend | Cargo workspace (crates/db-models, crates/validation) | sqlx compile-time queries. |
| Shared frontend | TypeScript packages (shared-types, shared-hooks, shared-ui) | strict mode, no `any`. Data fetching, auth, and types shared; map views NOT shared. |
| Database | PostgreSQL 16 + PostGIS | Single `platform_db` (gis, inventory, users schemas). Separate `keycloak_db` (owned by Keycloak) and `analytics_db`. |
| Identity | Keycloak | Single `bornemap` realm. Accessed only by Auth Service. Clients never reach Keycloak directly. |
| Cache | Redis | GIS spatial tile cache managed by Driver Service from MVP-5. |
| Gateway | Traefik | TLS, routing from MVP-6. JWT validation via cached JWKS only. |
| Monorepo root | `source/` | — |

Entity ID prefixes (NanoID): USR- (user), OPR- (partner/operator), STA- (station), CHG- (charger).

### IV. Database Architecture

**platform_db** — PostgreSQL 16 + PostGIS
- **gis schema**: OpenStreetMap spatial reference data (roads, boundaries, cities, raw OSM import via `osm_charging_stations_temp`).
- **inventory schema**: Operational infrastructure (partners, stations, chargers) and user interactions (favorites, reviews). Materialized views: `mv_stations_geo`, `mv_stations_summary`, `mv_stations_reviews`.
- **users schema**: User profile mapping (USR- rows), owned exclusively by Auth Service, keyed to Keycloak `sub` claim.

**keycloak_db**
A dedicated PostgreSQL database owned entirely by Keycloak. No application service or microservice connects to `keycloak_db` directly. It is provisioned and managed exclusively by Keycloak at runtime.

**analytics_db**
An isolated database for event logging, written to exclusively by the Admin Service. Separate from `platform_db` to prevent analytical workloads from impacting operational query performance.

### V. Development Workflow & Conventions

**Monorepo layout (source/)**
- `apps/` — Frontend applications (mobile-driver, web-driver, dashboard)
- `services/` — Actix-web microservices (auth-service, driver-service, admin-service)
- `packages/` — Shared TypeScript workspace (shared-types, shared-hooks, shared-ui)
- `crates/` — Shared Rust workspace (db-models, validation)
- `infra/` — Infrastructure (docker-compose.yml, keycloak/, osm-importer/)
- `docs/` — Documentation including ADR records

**Naming conventions**
- Services: kebab-case with `-service` suffix
- Apps: kebab-case descriptive
- Packages/crates: kebab-case with domain prefix (shared-, db-)

**API versioning**
All endpoints prefixed with `/api/v1/`. Major version bump on breaking changes only. Additive modifications inline.

**Core domain rules**
- Admin-only partner creation via invitation or admin-validated self-registration
- Companies (partners) are the top-level grouping — no independent networks layer
- Private home chargers are first-class public map entities alongside commercial stations
- Private and commercial stations share identical schema constraints; specializations via nullable metadata

### VI. Frontend Presentation & Interaction Rules

**State-driven interface checklist**
Every API-interacting screen MUST implement four states:

- **Loading**: Shimmer skeletons mirroring the target card layout (no spinners or blank screens).
- **Success**: Smooth layout animations (Framer Motion for web, LayoutAnimation for React Native).
- **Empty**: Illustrative feedback guiding users to pan to major cities (Tunis, Sousse, Sfax).
- **Error**: Structural error boundary with prominent Retry Connection button.

**Map interaction**
- Viewport debounce >= 300ms before querying `/api/v1/nearby`.
- Zoom-out past threshold: hide markers + overlay "Zoom in closer to view available charging stations".

**Mobile**
- Zero custom native modules — must run in default Expo Go.
- Successful nearby queries update AsyncStorage coordinate snapshot cache.
- Offline: read AsyncStorage cache, render markers, show "Viewing cached data" banner.

**Web**
- All styling via shared Tailwind config (`packages/shared-ui`).
- Marker SVGs/PNGs bundled locally and pre-loaded.

**Security**
- Web: JWTs in memory or secure browser state. Mobile: JWTs in secure device storage.
- Coordinate data must pass through shared validation before reaching API query strings.

### VII. Governance

This constitution is the final authority for architectural structure. Direct conflicts between source implementations and configuration files are resolved in favor of this document.

**Amendments**: Modifications to fundamental sections (Core Principles, Tech Stack, Architectural Principles, or Prohibitions) strictly require an accompanying ADR recorded inside `docs/adr/`.

**Evolutionary sections**: Monorepo structure, entity lists, service boundaries, and roadmap tasks can adapt dynamically without a formal ADR, provided this document is updated immediately.

**AI model compliance**: Coding LLMs MUST parse this document alongside `docs/GUARDRAILS.md`. Any violation of the prohibitions constitutes a blocking compliance error.

**Documentation sync**: Before completing any task, update `docs/roadmap_status.md`, `docs/sprint_backlog.md`, and `docs/SYSTEM_STATE.md`.
