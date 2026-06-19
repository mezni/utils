# BorneMap — Sprint Backlog

> Granular tasks assigned to builder sessions. One task = one LLM session.
> For high-level tracking, see `roadmap_status.md`.

---

## Current sprint

**Sprint:** N/A (scaffolding)  **Started:** 2026-06-18  **Target:** TBD

| Task | Status | Assigned to | Notes |
|------|--------|-----------|-------|
| Project scaffolding complete | 🟢 Done | — | docs, guardrails, constitution, skills |

---

## Backlog

| MVP | Task | Priority | Dependencies | Est. effort |
|-----|------|----------|-------------|-----------|
| MVP-1 | Create Keycloak realm + clients (docker-compose + realm export) | High | MVP-0 | 2h |
| MVP-1 | Implement auth-service: config, errors, domain types | High | MVP-1 (Keycloak) | 3h |
| MVP-1 | Implement auth-service: login handler + Keycloak proxy | High | MVP-1 (types) | 3h |
| MVP-1 | Implement auth-service: refresh handler | High | MVP-1 (login) | 1h |
| MVP-1 | Create users schema migration + USR_ profile upsert | High | MVP-1 (login) | 1h |
| MVP-1 | Integration tests: login, refresh, 401, 403 | High | MVP-1 (handlers) | 2h |
| MVP-2 | Create OSM importer script + gis schema migration | Medium | MVP-1 | 3h |
| MVP-2 | Create inventory schema migrations (partners, stations, chargers) | Medium | MVP-1 | 2h |
| MVP-2 | Implement driver-service: nearby query + materialized views | Medium | MVP-2 (migrations) | 4h |
| MVP-2 | Create materialized view migrations + CONCURRENTLY refresh | Medium | MVP-2 (inventory) | 2h |

---

## Task template

When assigning a new task to a builder session, copy this template:

```markdown
## Task: [MVP-N] [Short description]

**Inputs:**
- Constitution: `.specify/memory/constitution.md`
- Guardrails: `docs/guardrails/[domain].md`
- System state: `docs/SYSTEM_STATE.md`
- Prior spec: `docs/specs/mvp-[N-1].md` (if applicable)

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
