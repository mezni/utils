# Infrastructure Kickoff — MVP-1

## Overview

Set up the BorneMap development environment: monorepo structure, empty service shells with health endpoints, PostGIS databases, initial driver apps with base maps, and a blank dashboard shell. This establishes the foundation all subsequent MVPs build upon.

## User Stories

- As a **developer**, I want a scaffolded monorepo with consistent build tooling so that I can add services, packages, and apps without manual configuration.
- As a **developer**, I want all three backend services (Auth, Driver, Admin) to start and respond to health checks so that I can verify the infrastructure is operational.
- As a **developer**, I want PostGIS, Postgres, and Redis databases running in Docker so that services have their required data stores available.
- As a **driver**, I want to open the mobile or web app and see a base map of Tunisia so that I know the app loads correctly.
- As a **partner/admin**, I want to open the dashboard and see a blank logged-out page so that the routing shell is in place.

## Functional Requirements

### Monorepo Scaffold
- FR1: Project root contains a Cargo workspace for Rust crates and a pnpm workspace for TypeScript packages/apps
- FR2: Directory structure includes `services/`, `apps/`, `packages/`, and `crates/` at the project root
- FR3: Service shells exist under `services/auth-service`, `services/driver-service`, `services/admin-service`
- FR4: App shells exist under `apps/mobile-driver`, `apps/web-driver`, `apps/dashboard`
- FR5: Shared packages exist under `packages/shared-types`, `packages/shared-ui`, `packages/shared-hooks`, `packages/api-client`
- FR6: Shared Rust crates exist under `crates/db-models`, `crates/validation`

### Service Shells
- FR7: Each service exposes `GET /api/v1/health` returning `{ "status": "ok", "service": "<name>", "version": "0.1.0" }`
- FR8: Each service exposes `GET /api/v1/health/ready` that verifies database connectivity and returns 200 or 503
- FR9: Services accept `HOST`, `PORT`, `DATABASE_URL`, and `LOG_LEVEL` environment variables
- FR10: Services log structured output at the configured log level

### Databases & Infrastructure
- FR11: A `platform_db` container runs PostGIS 15 with schemas `gis`, `inventory`, `users` created on first start
- FR12: A `keycloak_db` container runs Postgres 16 for Keycloak identity data (empty schema, MVP-3 activates)
- FR13: An `analytics_db` container runs Postgres 16 for analytics data (empty, MVP-5 activates)
- FR14: All three databases are defined in a single `docker-compose.yml` at `infra/docker-compose.yml`
- FR15: Database credentials are configurable via `.env` file (not hardcoded)
- FR16: Services wait for `platform_db` to be healthy before starting

### Base Map — Driver Apps
- FR17: Mobile driver app (Expo SDK 54) displays a map centered on Tunisia with `react-native-maps`
- FR18: Web driver app (React) displays a map centered on Tunisia with Leaflet
- FR19: Both maps show standard OSM/Mapbox tiles with no station markers (data added in MVP-2)

### Dashboard Shell
- FR20: Dashboard app (React) renders a blank page with routing scaffold (React Router)
- FR21: Dashboard displays a branded login-required state when no session is present

## Non-functional Requirements

- NFR1: All services start within 10 seconds of Docker Compose up (cold start)
- NFR2: All three client apps (mobile, web, dashboard) load without JavaScript errors
- NFR3: `.env.example` documents all configurable variables with sensible defaults
- NFR4: `infra/docker-compose.yml` is the single source of truth for container topology

## Out of Scope

- API logic beyond health checks (register, nearby, CRUD — future MVPs)
- Station markers or data on maps (MVP-2)
- Keycloak authentication (MVP-3)
- Dashboard pages beyond the shell (MVP-4)
- Analytics data or endpoints (MVP-5)
- Traefik or production routing (MVP-6)
- Any code or files under `source/` — project lives at root level

## Success Criteria

| Criterion | Measure |
|-----------|---------|
| All 3 services respond `200 OK` on `/api/v1/health` within 10s of `docker compose up` | Verified via curl or health check |
| All 3 services respond `200 OK` on `/api/v1/health/ready` after DB is up | Verified via curl |
| `platform_db` has `gis`, `inventory`, `users` schemas on first startup | Verified via `psql \dn` |
| Mobile driver app shows a map centered on Tunisia | Visual inspection |
| Web driver app shows a map centered on Tunisia | Visual inspection |
| Dashboard app loads a blank page without errors | Browser console check |
| `docker compose -f infra/docker-compose.yml --profile infra up` starts all DB containers | Verified via `docker compose ps` |
| `.env.example` contains all variables referenced by compose and services | Audited against env-vars.md |

## Dependencies

- Docker Engine 24+ with Compose v2 plugin
- Node.js 20+ with pnpm 9+
- Rust 1.80+ with Cargo
- Expo CLI for mobile development
- npm/node available for web/dashboard builds

## Assumptions

- Development environment is Linux (macOS also works with Docker Desktop)
- All developers have the toolchain installed (Docker, Node, Rust, pnpm)
- Default dev credentials (`bornemap`/`bornemap_dev`) are acceptable for local development
- Services are built from source (no pre-built images)
- Map tiles come from a free tier (OSM default tiles, configurable later)
