# BorneMap — Sprint Backlog

> Granular tasks assigned to builder sessions. One task = one LLM session.
> For high-level tracking, see `roadmap_status.md`.

---

## Current sprint

**Sprint:** N/A (scaffolding)  **Started:** 2026-06-18  **Target:** TBD

| Task | Status | Assigned to | Notes |
|------|--------|-----------|-------|
| Project scaffolding complete | 🟢 Done | — | docs, guardrails, constitution, skills |
| Write MVP-1 spec | 🟢 Done | — | `docs/specs/mvp-1-admin-flow.md` |

---

## Backlog

| MVP | Task | Priority | Dependencies | Est. effort |
|-----|------|----------|-------------|-----------|
| MVP-1 | Phase 1 — Infra: Docker Compose (Postgres + Redis + Keycloak + Traefik routing) | High | MVP-0 | 3h |
| MVP-1 | Phase 1 — Infra: Keycloak realm export + client config (bornemap, clients: mobile-driver-app, web-driver-app, admin-dashboard) | High | MVP-1 (Docker) | 2h |
| MVP-1 | Phase 1 — Infra: platform_db + analytics_db migrations (schemas, PostGIS, audit columns, updated_at trigger) | High | MVP-1 (Docker) | 2h |
| MVP-1 | Phase 1 — Infra A5: DB role provisioning (auth_service_role, admin_service_role, driver_service_role) | High | MVP-1 (migrations) | 1h |
| MVP-1 | Phase 1 — Infra A6: Network segmentation (Keycloak internal-only, service network isolation) | High | MVP-1 (Docker) | 1h |
| MVP-1 | Phase 1 — Infra A7: Redis namespace contract (Admin invalidates, Driver reads) | High | MVP-1 (Redis) | 0.5h |
| MVP-1 | Phase 1 — Infra: Traefik JWKS validation middleware + header injection + audience check | High | MVP-1 (Keycloak) | 2h |
| MVP-1 | Phase 1 — Infra: Block external Keycloak token endpoint access in Traefik | High | MVP-1 (Traefik) | 0.5h |
| MVP-1 | Phase 2 — Auth Service: Cargo project, config, errors, domain types | High | MVP-1 (infra) | 2h |
| MVP-1 | Phase 2 — Auth Service: login handler + Keycloak proxy + USR- upsert | High | MVP-1 (auth-types) | 3h |
| MVP-1 | Phase 2 — Auth Service: refresh handler | High | MVP-1 (login) | 1h |
| MVP-1 | Phase 2 — Auth Service: integration tests (login, refresh, 401, 403) | High | MVP-1 (handlers) | 2h |
| MVP-1 | Phase 3 — Admin Service: Cargo project, AppError, domain types, repo traits | High | MVP-1 (infra) | 3h |
| MVP-1 | Phase 3 — Admin Service: Partner CRUD handlers + transactions + idempotency | High | MVP-1 (admin-types) | 3h |
| MVP-1 | Phase 3 — Admin Service: Station/charger CRUD handlers + transactions | High | MVP-1 (partner-CRUD) | 3h |
| MVP-1 | Phase 3 — Admin Service: Redis cache bust (sync, in service layer) + analytics_db diff-based audit | High | MVP-1 (station-CRUD) | 2h |
| MVP-1 | Phase 3 — Admin Service: integration tests (incl. idempotency, audit diff, DB role enforcement) | High | MVP-1 (all handlers) | 3h |
| MVP-1 | Phase 5 — Dashboard: login page, token storage (memory), React Router auth guard | High | MVP-1 (auth-service) | 3h |
| MVP-1 | Phase 5 — Dashboard: partner/station CRUD pages + React Query mutations | High | MVP-1 (admin-service) | 4h |
| MVP-2 | Create inventory schema migrations (partners, stations, chargers, materialized views) | Medium | MVP-1 | 2h |
| MVP-2 | Implement driver-service: nearby query + materialized views + Redis reads | Medium | MVP-2 (migrations) | 4h |
| MVP-5 | Create OSM importer script + gis schema migration | Low | MVP-2 | 3h |

---

## Task template

When assigning a new task to a builder session, copy this template:

```markdown
## Task: [MVP-N] [Short description]

**Inputs:**
- Constitution: `.specify/memory/constitution.md`
- Guardrails: `docs/guardrails/[domain].md`
- System state: `docs/SYSTEM_STATE.md`
- Prior spec: `docs/specs/mvp-[N-1].md` (omit for MVP-1 as MVP-0 was scaffolding with no spec)

**Acceptance criteria:**
1. [Criterion 1]
2. [Criterion 2]
3. Tests pass (`cargo test`, `vitest run`)
4. `docs/SYSTEM_STATE.md` updated

**Files to create/modify:**
- `source/services/[name]/...`
- `source/infra/migrations/...`
```

---

## Legend

| Icon | Meaning |
|------|---------|
| 🟢 Done | Built, tested, verified |
| 🟡 In progress | Being worked on this session |
| ⬜ Not started | Not yet begun |
| 🔴 Blocked | Waiting on dependency or decision |
