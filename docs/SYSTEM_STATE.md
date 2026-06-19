# BorneMap — System State

**Last updated:** 2026-06-18  **Session:** Scaffolding — docs/ + guardrails + skills + MVP-1 spec

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
- [x] `docs/adr/` — directory created (empty, awaiting first ADR)
- [x] `docs/specs/mvp-1-admin-flow.md` — MVP-1 spec written, not started

## Skills installed (global — `npx skills add -g`)

- [x] `nexu-io/open-design@ui-ux-pro-max`
- [x] `wshobson/agents@rust-async-patterns`
- [x] `apollographql/skills@rust-best-practices`
- [x] `wshobson/agents@postgresql-table-design`
- [x] `wshobson/agents@typescript-advanced-types`
- [x] `mindrally/skills@expo-react-native-typescript`
- [x] `adobe/skills@appbuilder-e2e-testing`
- [x] `dauquangthanh/hanoi-rainbow@keycloak-administration`

## Not yet built

- `source/` — no application code exists yet (empty monorepo root)
- `source/apps/mobile-driver` — mobile driver app
- `source/apps/web-driver` — web driver app
- `source/apps/dashboard` — partner/admin dashboard
- `source/services/auth-service` — Auth Service (:3000)
- `source/services/driver-service` — Driver Service (:3001)
- `source/services/admin-service` — Admin Service (:3002)
- `source/packages/shared-types` — shared TypeScript types
- `source/packages/shared-hooks` — shared React hooks
- `source/packages/shared-ui` — shared Tailwind/component library
- `source/crates/db-models` — shared Rust database models
- `source/crates/validation` — shared Rust validation
- `source/infra/docker-compose.yml` — infrastructure orchestration
- `source/infra/keycloak/` — Keycloak configuration
- `source/infra/migrations/` — database migration files
- `infra/osm-importer/` — OSM data pipeline
- `docs/adr/` — no ADRs yet

## Environment

- platform_db: not provisioned
- keycloak_db: not provisioned
- analytics_db: not provisioned
- Keycloak: not configured
- Redis: not provisioned
- Traefik: not configured

## Known issues

- None yet.
