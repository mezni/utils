# BorneMap — System State

**Last updated:** 2026-06-19  **Session:** Sprint 1 — Auth Service specification, plan, tasks, contracts, and analysis complete. Branch `002-auth-service` ready for implementation.

## Built and verified

- [x] `docs/GUARDRAILS.md` — master entry-point guardrails file
- [x] `docs/guardrails/rust.md` — Rust clean architecture rules
- [x] `docs/guardrails/testing.md` — testing patterns (unit, integration, E2E)
- [x] `docs/guardrails/postgres.md` — PostgreSQL migration and query rules
- [x] `docs/guardrails/documentation.md` — doc comment and ADR standards
- [x] `docs/guardrails/ux-ui.md` — base UX/UI four-state contract and map rules
- [x] `docs/guardrails/ux-ui-promax.md` — extended design system and animation rules
- [x] `docs/guardrails/code-review.md` — Tier 1/2/3 review workflow
- [x] `docs/guardrails/agents.md` — builder session orchestration rules
- [x] `.specify/memory/constitution.md` — BorneMap Architecture Constitution Rev 2
- [x] `docs/SYSTEM_STATE.md` — this file (living state tracker)
- [x] `docs/roadmap_status.md` — MVP milestones tracker
- [x] `docs/sprint_backlog.md` — granular task backlog
- [x] `docs/bug_tracker.md` — active bug tracking
- [x] `docs/specs/mvp-1-admin-flow.md` — MVP-1 spec written, reviewed, delta-patches applied. Ready for execution.
- [x] `docs/adr/` — directory created (empty, awaiting first ADR)
- [x] `source/infra/docker-compose.yml` — 8-service stack with health checks, volumes, network (Sprint 0)
- [x] `source/infra/traefik/` — static + dynamic config, `rewrite-to-root` middleware (Sprint 0)
- [x] `source/infra/postgres/init/` — 3 init SQL scripts: databases, schemas+roles, initial tables (Sprint 0)
- [x] `source/infra/keycloak/` — realm export (bornemap-realm.json, KC 25.0.6) + Docker config (Sprint 0)
- [x] `source/infra/stubs/` — 4 stubs (auth, admin, driver, catch-all) for routing validation (Sprint 0)
- [x] `specs/002-auth-service/` — Sprint 1 spec, plan, research, data-model, contracts, 30 tasks, analysis (Sprint 1)

## Skills installed (global — `npx skills add -g`)

- [x] `nexu-io/open-design@ui-ux-pro-max`
- [x] `wshobson/agents@rust-async-patterns`
- [x] `apollographql/skills@rust-best-practices`
- [x] `wshobson/agents@postgresql-table-design`
- [x] `wshobson/agents@typescript-advanced-types`
- [x] `mindrally/skills@expo-react-native-typescript`
- [x] `adobe/skills@appbuilder-e2e-testing`
- [x] `dauquangthanh/hanoi-rainbow@keycloak-administration`

## Built in Sprint 0 but not yet wired to app code

- `source/infra/docker-compose.yml` — 6 backend services running (Postgres 16+PostGIS, Redis 7, Keycloak 25, Traefik 3, 3 stubs)
- `source/infra/keycloak/realm-export/bornemap-realm.json` — bornemap realm, 3 clients (auth-service, admin-dashboard, driver-app), 3 roles, audience mapper
- `source/infra/postgres/init/` — 3 init SQL scripts creating 3 databases, schemas, roles, initial tables
- `source/infra/traefik/dynamic/routing.yml` — 4 routers with `rewrite-to-root` middleware, StripPrefix

## Not yet built

- `source/apps/mobile-driver` — mobile driver app
- `source/apps/web-driver` — web driver app
- `source/apps/dashboard` — partner/admin dashboard
- `source/services/auth-service` — Auth Service (:3000) — Sprint 1 in progress
- `source/services/driver-service` — Driver Service (:3001)
- `source/services/admin-service` — Admin Service (:3002)
- `source/packages/shared-types` — shared TypeScript types
- `source/packages/shared-hooks` — shared React hooks
- `source/packages/shared-ui` — shared Tailwind/component library
- `source/crates/db-models` — shared Rust database models
- `source/crates/validation` — shared Rust validation
- `source/infra/migrations/` — database migration files (4 planned: init, audit, seed, materialized views)
- `source/infra/osm-importer/` — OSM data pipeline
- `docs/adr/` — no ADRs yet

## Sprint plan

| Sprint | Focus | Tickets | Status |
|--------|-------|---------|--------|
| 0 | Platform bootstrap (Docker, Keycloak, DB schemas, Traefik routing) | INF-1–4 | 🟢 Complete |
| 1 | Auth Service (login, refresh, USR- upsert, logout, audience propagation) | 30 tasks in `specs/002-auth-service/tasks.md` | 🟡 Spec/plan/tasks complete — branch `002-auth-service` |
| 2 | Admin Service CRUD (partners, stations, chargers, transaction orchestrator, DB role) | ADM-1–5 | ⬜ Not started |
| 3 | Gateway security (JWKS, audience, header injection, Keycloak isolation) | SEC-1–5 | ⬜ Not started |
| 4 | Redis + MV refresh (post-commit bust, failure policy, MV refresh, driver read) | REDIS-1–5 | ⬜ Not started |
| 5 | Analytics + audit (analytics_db, BEFORE/AFTER diff, mutation hooks, indexes) | AUD-1–4 | ⬜ Not started |
| 6 | Idempotency + hardening (Idempotency-Key, duplicate detection, validation, error contracts) | SAFE-1–4 | ⬜ Not started |
| 7 | E2E integration (full flow tests, gateway security, audit verification) | E2E-1–6 | ⬜ Not started |

Total: 36 tickets across 8 sprints. See `docs/sprint_backlog.md` for details.

## Environment (Sprint 0 infra)
- platform_db: provisioned via init SQL (01-create-dbs, 02-schemas-and-roles, 03-initial-tables)
- keycloak_db: provisioned (Keycloak manages its own schema)
- analytics_db: provisioned via init SQL
- Keycloak: configured with bornemap realm, 3 clients, 3 roles, audience mapper (KC 25.0.6)
- Redis: provisioned (port 6379)
- Traefik: configured with 4 routers, `rewrite-to-root` middleware, StripPrefix
- Stubs: 3 HTTP stubs + 1 catch-all for routing verification

## Known issues

- MV + Redis dual consistency lag: DB commit → MV refresh → Redis bust. If MV refresh is slow, Redis may be invalidated before MV is updated, causing brief Driver read inconsistency. Not fatal for MVP — documented for future concurrency optimization.
