# BorneMap — Master Guardrails

> This file is the single source of truth for all code quality, architecture, testing, documentation, UX, and review standards across the BorneMap monorepo. Every builder LLM session MUST read this file before writing any code.
>
> Individual domain files in `docs/guardrails/` expand each section with detailed rules and examples. When a conflict exists between this file and a domain file, this file wins.

---

## How to use these guardrails

1. Read this file completely before starting any task.
2. Read the domain-specific guardrail file(s) relevant to your task.
3. At the end of every file you produce, run the **self-check** listed in the relevant domain file.
4. Never skip a guardrail because the task seems small. Small violations compound.

---

## 1. Non-negotiable blockers

These are hard stops. If any of the following is true of your output, do not submit it — fix it first.

**Rust**
- Any `unwrap()` or `expect()` outside of test code → use `?` with typed errors
- Any raw SQL string concatenation → use `sqlx::query!` macros only
- Any `clone()` that could be avoided with a borrow
- Any `pub` on a field or function that does not need to be public
- Missing error variant for any new failure mode introduced

**TypeScript**
- Any use of `any` keyword anywhere in non-generated code
- Any `// @ts-ignore` or `// @ts-nocheck` comment
- Any API response consumed without runtime validation (use Zod)
- Any `console.log` left in production code paths

**PostgreSQL**
- Any query that touches `inventory.station` or `inventory.charger` outside a transaction
- Any new column added without a migration file
- Any index dropped without an explicit ADR comment explaining why
- Any direct write to `users` schema from a service other than Auth Service
- Any direct connection to `keycloak_db` from application code

**Auth & Security**
- Any client-side code calling Keycloak endpoints directly
- Any JWT stored in localStorage
- Any secret, credential, or API key hardcoded in source
- Any endpoint reachable without role validation when it requires one

**UX**
- Any screen that renders a blank state or raw spinner instead of a shimmer skeleton
- Any map interaction without a 300ms debounce
- Any form that submits without client-side validation feedback

---

## 2. Architecture rules (always enforced)

- Three services only: `auth-service`, `driver-service`, `admin-service`. No new services without an ADR.
- All endpoints under `/api/v1/`. No exceptions.
- Auth Service is the sole caller of Keycloak. No other service or client touches Keycloak APIs.
- Traefik performs JWT validation via cached JWKS only. It does not call Keycloak token endpoints.
- Redis spatial cache is owned by Driver Service. Admin Service busts it synchronously on station/charger writes.
- `analytics_db` is written to exclusively by Admin Service.
- `keycloak_db` is owned exclusively by Keycloak. No application code connects to it.

---

## 3. Domain guardrail index

| Domain | File | Applies to |
|--------|------|-----------|
| Rust clean architecture | `guardrails/rust.md` | `services/`, `crates/` |
| Testing | `guardrails/testing.md` | All services and packages |
| Documentation | `guardrails/documentation.md` | All code and docs/ |
| PostgreSQL | `guardrails/postgres.md` | All DB migrations and queries |
| UX/UI base | `guardrails/ux-ui.md` | `apps/mobile-driver`, `apps/web-driver`, `apps/dashboard` |
| UX/UI pro max | `guardrails/ux-ui-promax.md` | `apps/mobile-driver`, `apps/dashboard` — extends ux-ui.md |
| Agent orchestration | `guardrails/agents.md` | All Claude Code / Cursor builder sessions |
| Code review | `guardrails/code-review.md` | All PRs and LLM output review |

---

## 4. Session discipline

- Read `docs/SYSTEM_STATE.md` before starting. Do not re-implement what is already built.
- Read the relevant MVP spec in `docs/specs/` before writing any code.
- Update `docs/SYSTEM_STATE.md` at the end of every session.
- Never leave a TODO comment without a linked issue reference.
- Never commit directly to `main`. All output targets a feature branch.
