# Tasks: MVP-1 Infra Kickoff

## Phase 1: Setup

- [ ] T001 Create root `Cargo.toml` with workspace members for crates/db-models, crates/validation, services/auth-service, services/driver-service, services/admin-service
- [ ] T002 Create root `pnpm-workspace.yaml` with packages/apps/mobile-driver, apps/web-driver, apps/dashboard, packages/*
- [ ] T003 Create root `tsconfig.base.json` with shared TypeScript configuration for all TS packages/apps
- [ ] T004 [P] Create `.env` from `.env.example` (copy, no overwrite if exists)

## Phase 2: Foundational

- [ ] T005 [P] Create `infra/docker-compose.yml` with platform_db (PostGIS), keycloak_db (Postgres 16), analytics_db (Postgres 16), redis (Redis 7), healthchecks, env vars via .env
- [ ] T006 [P] Create `infra/db/init-platform-db.sql` with enums, gis/inventory/users schemas, tables, indexes per `docs/spec/db-schema.md`
- [ ] T007 [P] Create `.env.example` with all MVP-1 vars: DB credentials, service ports, Redis port, log level per `docs/spec/env-vars.md`
- [ ] T008 Add healthcheck blocks to all three DB containers in `infra/docker-compose.yml`

## Phase 3: [US1] Monorepo Scaffold

- [ ] T009 [P] [US1] Scaffold `services/auth-service/` — create `Cargo.toml` with actix-web dependency, empty `src/main.rs`
- [ ] T010 [P] [US1] Scaffold `services/driver-service/` — same structure as auth-service
- [ ] T011 [P] [US1] Scaffold `services/admin-service/` — same structure as auth-service
- [ ] T012 [P] [US1] Scaffold `apps/mobile-driver/` — Expo SDK 54 blank app with `package.json`, `app.json`, `tsconfig.json`
- [ ] T013 [P] [US1] Scaffold `apps/web-driver/` — Vite + React + TypeScript blank app with `package.json`, `vite.config.ts`, `tsconfig.json`
- [ ] T014 [P] [US1] Scaffold `apps/dashboard/` — Vite + React + TypeScript + shadcn/ui blank app with `package.json`, `vite.config.ts`, `tsconfig.json`
- [ ] T015 [P] [US1] Scaffold `packages/shared-types/`, `packages/shared-ui/`, `packages/shared-hooks/`, `packages/api-client/` — each with `package.json` and `tsconfig.json`
- [ ] T016 [P] [US1] Scaffold `crates/db-models/`, `crates/validation/` — each with `Cargo.toml` and `src/lib.rs`
- [ ] T017 [US1] Verify `cargo build` compiles all Rust crates and services without errors
- [ ] T018 [US1] Verify `pnpm install` resolves all workspace dependencies without errors

## Phase 4: [US2] Service Shells

- [ ] T019 [P] [US2] Implement `services/auth-service/src/main.rs` — Actix-web server on port 3000, load config from env, init DB pool
- [ ] T020 [P] [US2] Implement `services/auth-service/src/routes/health.rs` — `GET /api/v1/health` returning `{"status":"ok","service":"auth-service","version":"0.1.0"}`
- [ ] T021 [P] [US2] Implement `services/auth-service/src/routes/ready.rs` — `GET /api/v1/health/ready` pinging DB, returns 200 or 503
- [ ] T022 [P] [US2] Implement `services/auth-service/src/config.rs` — load HOST, PORT, DATABASE_URL, LOG_LEVEL from env with defaults
- [ ] T023 [P] [US2] Implement `services/auth-service/src/db.rs` — sqlx PgPool connection pool from DATABASE_URL
- [ ] T024 [P] [US2] Repeat T019-T023 identically for `services/driver-service` (port 3001)
- [ ] T025 [P] [US2] Repeat T019-T023 identically for `services/admin-service` (port 3002)
- [ ] T026 [US2] Verify `cargo build` succeeds for all three services

## Phase 5: [US3] Databases & Infrastructure

- [ ] T027 [P] [US3] Add depends_on with condition: service_healthy to auth-service → platform_db in `infra/docker-compose.yml`
- [ ] T028 [P] [US3] Add depends_on with condition: service_healthy to driver-service → platform_db in `infra/docker-compose.yml`
- [ ] T029 [P] [US3] Add depends_on with condition: service_healthy to admin-service → platform_db in `infra/docker-compose.yml`
- [ ] T030 [P] [US3] Add profiles: ["services"] to auth-service, driver-service, admin-service in `infra/docker-compose.yml`
- [ ] T031 [US3] Verify `docker compose up` starts all DB containers with init SQL applied
- [ ] T032 [US3] Verify `docker compose --profile services up` starts all services and `/health/ready` returns 200 for each

## Phase 6: [US4] Base Maps — Driver Apps

- [ ] T033 [US4] Implement `apps/mobile-driver/App.tsx` — MapView from react-native-maps centered on Tunisia (lat: 34.0, lon: 9.0), no markers
- [ ] T034 [US4] Implement `apps/web-driver/src/App.tsx` — Leaflet MapContainer centered on Tunisia (lat: 34.0, lon: 9.0), standard OSM tiles, no markers
- [ ] T035 [US4] Add `packages/shared-ui/src/MapContainer.tsx` — Base map wrapper component used by both driver apps
- [ ] T036 [US4] Verify mobile driver app renders map without errors via `npx expo start`
- [ ] T037 [US4] Verify web driver app renders map without errors via `pnpm dev` (no console errors)

## Phase 7: [US5] Dashboard Shell

- [ ] T038 [US5] Implement `apps/dashboard/src/App.tsx` — React Router with routes: `/` (home), `*` (not-found redirect)
- [ ] T039 [US5] Create `apps/dashboard/src/pages/Login.tsx` — branded logged-out state with login prompt, placeholders for email/password form
- [ ] T040 [US5] Create `apps/dashboard/src/pages/Home.tsx` — empty dashboard home with sidebar/nav skeleton
- [ ] T041 [US5] Create `apps/dashboard/src/pages/NotFound.tsx` — "Page not found" with link to home
- [ ] T042 [US5] Verify dashboard loads at `/` and redirects to login when unauthenticated (mock state)

## Phase 8: Polish & Verification

- [ ] T043 Update `docs/mvp-1/STATUS.md` — check off completed Sprint 1.1 tasks
- [ ] T044 Update `docs/mvp-1/STATUS.md` — check off completed Sprint 1.2 tasks
- [ ] T045 Update `docs/mvp-1/STATUS.md` — check off completed Sprint 1.3 tasks, set Status to Complete
- [ ] T046 Final verification: run `make up-all`, verify all services respond to health checks, all apps load

---

## Dependencies

```
Phase 1 (Setup)
  └── Phase 2 (Foundational)
       ├── Phase 3 (US1: Monorepo) ──→ Phase 4 (US2: Services)
       │                              └── Phase 5 (US3: Databases)
       └── Phase 3 (US1: Monorepo) ──→ Phase 6 (US4: Maps)
                                      └── Phase 7 (US5: Dashboard)
                                           └── Phase 8 (Polish)
```

Phases 4 and 5 can run in parallel after Phase 3 completes.
Phases 6 and 7 can run in parallel after Phase 3 completes.

## Parallel Execution

| Tasks | Why parallel |
|-------|-------------|
| T005 + T006 + T007 | Different files, no shared state |
| T009–T016 | All independent file scaffold operations |
| T019–T025 | Three services are independent copies of same pattern |
| T033 + T034 | Mobile and web apps are independent codebases |
| T038–T041 | Dashboard pages are independent components |

## Independent Test Criteria

| Story | Test |
|-------|------|
| US1 | `cargo build` succeeds, `pnpm install` resolves |
| US2 | `curl localhost:3000/api/v1/health` returns 200, `/health/ready` returns 200 after DB up |
| US3 | `docker compose ps` shows all 4 DB containers healthy |
| US4 | Mobile + web render map centered on Tunisia (visual) |
| US5 | Dashboard loads at `/`, `/login` shows branded prompt |

## Implementation Strategy

Build in order: monorepo first (unlocks everything), then services + databases in parallel, then client apps in parallel. Each phase is independently testable.
