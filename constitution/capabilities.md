# BorneMap — Capability Model (Skills)
**Version:** 2.0
**Date:** June 2026
**Supersedes:** v1.0

> These seven skills are always active. They are not optional modules.
> They form a single pipeline. If any layer breaks, the sprint is invalid.
>
> Pipeline: **UX/UI → API Contract → Rust Backend → Postgres/PostGIS → Keycloak/Security → Testing → Documentation**

---

## Skill 1 — UX/UI Professional Standard

**Applies to:** `apps/web`, `apps/dashboard`, `apps/mobile`

### Philosophy
Design as the lead at a small studio: deliberate, opinionated, specific to this product. Reject templated defaults. Every screen for BorneMap must feel like it was made for EV drivers and operators in Tunisia — not a generic SaaS template.

### Mandatory Rules
- Component-driven design — no ad-hoc UI logic anywhere
- All shared UI components in `packages/shared-ui`
- TypeScript strict mode — no untyped props, no `any`
- State-driven UI — no imperative DOM manipulation
- Strict layer separation: `presentation component → domain hook → API client`
- WCAG 2.1 AA accessibility baseline
- Framer Motion for route transitions only in dashboard
- React Query as transport/cache layer only

### Design Process for New Screens
1. Name the screen's single job and its audience
2. Build a compact token system: 4–6 named colors, 2–3 typeface roles, layout concept
3. Define the signature element — what makes this screen memorable
4. Self-critique: does any part read as a generic default? Revise before coding
5. Write copy from the end user's side: plain verbs, active voice, sentence case
6. Errors and empty states are directional — never vague, never apologetic

### Prohibited
- Business logic inside components
- Duplicated UI patterns across apps
- Tailwind classes that conflict or cancel out
- `any` typed props

### CI Gate
UI lint + TypeScript strict + visual regression (Phase 6 TESTING)

---

## Skill 2 — Rust Clean Architecture

**Applies to:** `services/auth-service`, `services/driver-service`, `services/admin-service`

### Mandatory Layer Structure (Every Service)
```
src/
├── api/            # DTOs, Actix-web route handlers, middleware — NO business logic
├── domain/         # Pure Rust business logic — no DB, no HTTP, no external deps
├── application/    # Use-case orchestration — coordinates domain + infrastructure via traits
├── infrastructure/ # SQLx DB, Redis, Keycloak client, external HTTP calls
└── main.rs         # Wiring only
```

### Rules
- `domain/` depends only on `std` and internal domain types
- `application/` defines traits; `infrastructure/` implements them (dependency inversion)
- `api/` maps DTOs; no business logic leaks into handlers
- All DB queries via SQLx compile-time macros — no string construction
- Shared logic in `shared/` Cargo crates — never copy-pasted
- Services communicate only via HTTP — no cross-crate imports between services
- `unwrap()` forbidden in production paths — use `?` and typed errors
- `panic!` forbidden outside initialization

### Prohibited
- Fat controllers (handlers containing business logic)
- DB access in `api/` layer
- Shared mutable state across service boundaries
- `unwrap()` in production code

### CI Gate
`cargo check`, `cargo test`, `sqlx prepare --check`, dependency boundary validation

---

## Skill 3 — Postgres + PostGIS Data Discipline

### Database Rules
- Single `platform_db` — no additions
- Schema isolation strictly enforced (see Skill 2 for ownership)
- PostGIS extensions used only in `driver-service` queries
- All migrations append-only — no destructive migrations
- No raw SQL outside SQLx macros
- `deleted_at TIMESTAMPTZ` required on: `partner_profiles`, `stations`, `chargers`
- Every production query against `stations` MUST include `WHERE s.is_test = FALSE`

### Materialized Views
Owned by `admin-service` (refresh trigger). Read by `driver-service` via dedicated DB read role:
- `mv_stations_geo`
- `mv_stations_summary`
- `mv_stations_reviews`

### Migration Naming
```
migrations/<timestamp>_<service>_<description>.sql
```

### CI Gate
Migration diff validation, schema ownership check, SQLx compile-time verification

---

## Skill 4 — Keycloak Identity & Security

### Keycloak Rules
- Single realm: `bornemap` — no realm additions
- `auth-service` is the ONLY caller of the Keycloak Admin REST API
- No service stores passwords, credentials, or raw tokens in the DB
- JWT validation: Traefik forward auth + per-service middleware (two layers)
- Tokens never persisted in `platform_db`
- Role enforcement is per-endpoint — explicit, never inherited implicitly

### Endpoint Auth Matrix
| Endpoint | Auth | Roles |
|---|---|---|
| `GET /api/v1/driver/stations` (browse) | None | public |
| `GET /api/v1/driver/stations/:id` | None | public |
| `POST /api/v1/driver/favorites` | JWT | `role:driver` |
| `GET /api/v1/auth/me` | JWT | any authenticated |
| `POST /api/v1/admin/stations` | JWT | `role:partner`, `role:admin` |
| `PUT /api/v1/admin/stations/:id` | JWT | `role:partner`, `role:admin` |
| `DELETE /api/v1/admin/stations/:id` | JWT | `role:admin` |
| `GET /api/v1/admin/audit` | JWT | `role:admin` |

### CI Gate
JWT middleware coverage test, endpoint auth matrix validation, role check per route

---

## Skill 5 — Testing Strategy

### Coverage Requirements
| Layer | Minimum |
|---|---|
| `domain/` | 100% |
| `api/` handlers | ≥ 90% |
| Integration flows | Required for all services |
| Critical e2e flows | auth, station browse, partner CRUD |

### Test Types
- **Unit tests** (`testing/unit/`) — domain layer, no mocks of domain logic, real domain code only
- **Integration tests** (`testing/integration/`) — service + real Postgres + real Redis via testcontainers
- **Contract tests** — OpenAPI spec validated against actual Actix-web routes
- **Regression tests** — one per resolved bug in `bugs/resolved.md`

### Rules
- No untested production code
- No in-memory DB fakes for integration tests — testcontainers only
- Test isolation: each test cleans up its own DB state
- Every resolved bug gets a regression test before closing

### CI Gate
`tools/test_runner.sh` — coverage below threshold = HARD BLOCK

---

## Skill 6 — Documentation System

### Living Docs (Always in Sync with Implementation)
| Doc | Path | Purpose |
|---|---|---|
| System architecture | `docs/architecture.md` | Single architectural truth |
| API contracts index | `docs/api-contracts.md` | Links to all OpenAPI specs |
| System state | `docs/SYSTEM_STATE.md` | Runtime state snapshot |
| Roadmap status | `docs/roadmap_status.md` | Sprint delivery tracking |
| ADRs | `docs/adr/` | One per architectural decision |

### Per-Sprint Mandatory Docs
```
sprints/<id>/spec/spec.md
sprints/<id>/spec/scope.md
sprints/<id>/spec/non_scope.md
sprints/<id>/spec/assumptions.md
sprints/<id>/review/sprint_review.md
sprints/<id>/review/validation_report.md
sprints/<id>/review/retro.md
sprints/<id>/backlog/follow_up.md
```

### Rules
- Docs ship with the sprint — no "document later"
- Stale docs = CI failure
- ADR required for every architectural decision made during a sprint
- OpenAPI ↔ implementation parity enforced on every commit

### CI Gate
Doc drift detection in `ci_guard.sh`, OpenAPI ↔ route consistency check

---

## Skill 7 — Security Review

Applied during every sprint's REVIEW phase. Output is a security delta section in `review/sprint_review.md`.

### Threat Model Checklist (Per Sprint)
| Threat | Mitigation |
|---|---|
| API abuse | Traefik rate limiting middleware |
| Unauthorized role escalation | Role matrix enforcement + tests |
| Schema leakage | Schema isolation + CI Gate 3 |
| Cross-service access violation | Service boundary enforcement |
| JWT forgery / replay | Short-lived tokens, no DB persistence |
| SQL injection | SQLx compile-time macros only |
| CORS misconfiguration | Explicit allow-lists per service |
| Error information disclosure | Error catalog — no stack traces to clients |
| Test station leakage (KNOWN-001) | `WHERE s.is_test = FALSE` audit |

### Sprint Security Review Output (Required)
- List of endpoints added this sprint + auth status for each
- List of schema changes + isolation confirmation
- Any new external data sources + trust level assessment
- Threat model delta from previous sprint

---

## Meta-Rule

All seven skills form a single delivery pipeline. No skill can be deferred to a future sprint. No output is valid unless all relevant skills were applied.
