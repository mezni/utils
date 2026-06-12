<!--
  Sync Impact Report
  ==================
  Version change: 0.0.0 (template) → 1.0.0
  Modified principles: (all new — initial template population)
    - [PRINCIPLE_1_NAME] → "I. UX-First"
    - [PRINCIPLE_2_NAME] → "II. Domain-Driven Services"
    - [PRINCIPLE_3_NAME] → "III. Test-First (NON-NEGOTIABLE)"
    - [PRINCIPLE_4_NAME] → "IV. Source-Rooted Codebase"
    - [PRINCIPLE_5_NAME] → "V. Immutable Data & Append-Only Analytics"
  Added sections:
    - Stack & Tooling Mandate
    - Security & Governance
    - Governance (amendment procedure, versioning, compliance)
  Removed sections: (none — first population)
  Templates requiring updates:
    - .specify/templates/plan-template.md — ✅ No changes needed (Constitution Check section is generic)
    - .specify/templates/spec-template.md — ✅ No changes needed
    - .specify/templates/tasks-template.md — ✅ No changes needed
    - .specify/templates/checklist-template.md — ✅ No changes needed
    - .specify/templates/constitution-template.md — ✅ Template unchanged (source of truth)
  Follow-up TODOs: none — all placeholders resolved.
-->

# BorneMap Constitution

## Core Principles

### I. UX-First

UX quality MUST supersede system complexity. Perceived speed MUST
supersede backend sophistication. Map interaction latency is a primary KPI.
Non-negotiable rules:

- Skeleton screens over spinners — everywhere, no exceptions
- Optimistic UI on all user actions that touch the backend
- Haptic feedback on all primary CTAs (expo-haptics)
- Gesture-first design — bottom sheets, swipe-to-dismiss, pull-to-refresh
- Empty states MUST be fully designed — never a blank screen
- Error states MUST include recovery actions — never raw error strings
- Dark mode MUST work on every screen from day one
- Map interaction MUST NOT cause marker jitter or unnecessary re-renders
- Animations MUST use react-native-reanimated v3 only
- Route transitions via expo-router layout animations only
- No Platform.OS checks outside MapContainer.tsx
- All design tokens defined in tokens.ts — no hardcoded colors/spacing

### II. Domain-Driven Services

Each service owns a bounded context. Services MUST NOT query outside
their domain.

| Domain | Owner | Responsibility |
|--------|-------|----------------|
| Discovery | Driver service | Station search, geospatial queries |
| Management + Events | Admin service | Station CRUD, partner management, event ingestion |
| Identity | Keycloak | Authentication, realm management |

Rules:

- Two services only: driver-service (:8080), admin-service (:8081)
- No dedicated clickstream service — events live in admin-service
- Services must not overlap domain boundaries
- Keycloak is internal — never exposed directly to clients
- Keycloak_db is never accessed by services

### III. Test-First (NON-NEGOTIABLE)

TDD mandatory for all backend code. Tests MUST be written first, proposed
for user approval, confirmed failing, then implementation follows.
Red-Green-Refactor cycle strictly enforced.

Coverage targets:
- 80%+ unit test coverage on backend services
- 100% contract test coverage on all API endpoints
- Integration tests for new contracts, contract changes, inter-service
  communication, and shared schemas
- E2E tests for critical user flows (discovery, search, detail)

### IV. Source-Rooted Codebase

All runtime code MUST live under `source/`. Everything outside `source/`
is non-runtime. Never mix runtime and non-runtime in the same directory.

```
source/           ← ALL runtime code
├── shared/        ← Shared Rust crates
│   ├── ev-core/   ← Core domain types, traits
│   ├── ev-auth/   ← Authentication helpers
│   └── ev-db/     ← Database access layer
├── services/      ← Rust microservices
│   ├── driver-service/ ← Rust/Actix :8080
│   └── admin-service/  ← Rust/Actix :8081
├── front/         ← Mobile and web apps
│   ├── packages/   ← Shared design system, UI kit
│   ├── mobile-driver/ ← Expo SDK 54 app
│   ├── web-driver/    ← React + Leaflet
│   └── dashboard/     ← React + shadcn/ui

docs/  ← Documentation only
infra/ ← Docker, migrations, configs
scripts/ ← Build tools, seed scripts
```

### V. Immutable Data & Append-Only Analytics

- platform_db is the single source of truth
- gis schema is READ-ONLY — driver-service only, never written by services
- analytics_db is APPEND-ONLY — no UPDATE, no DELETE ever
- Soft delete on infrastructure entities (station, charger, partner)
- Hard delete on user-generated content only
- All IDs use entity-prefixed nanoids: STA-, CHR-, PRT-, USR-, OPR-
- All timestamps ISO 8601 UTC
- All endpoints prefixed /api/v1/

## Stack & Tooling Mandate

| Technology | Requirement | Enforcement |
|------------|-------------|-------------|
| Backend | Rust + Actix-web | Two services only |
| Mobile | Expo SDK 54 | Locked — no upgrades without ADR |
| Web driver | React + Leaflet | Secondary product surface |
| Dashboard | React + shadcn/ui | Ops only |
| Package manager | pnpm only | No npm, no yarn |
| Database | PostgreSQL 16 + PostGIS | Spatial indexes required |
| API gateway | Traefik | All client traffic through Traefik |
| Tunnel | Direct IP or Cloudflare | ngrok is PROHIBITED |
| Implementation | Claude Code | Sole code execution tool |

Monorepo structure enforced: runtime code under `source/`, config/docs
outside. Dependencies pinned to exact versions.

## Security & Governance

- Zero trust — all requests validated at the gateway boundary
- Least privilege — each service accesses only its own schema
- No cross-domain DB access between services
- JWT validation on all admin endpoints (MVP-3+)
- Partner scoping enforced server-side: WHERE partner_id = JWT.partner_id
- Keycloak realms: bm-drivers (public), bm-control (partners/admins)
- Roles: public_driver, registered_driver, partner, admin
- Partners cannot self-register — admin creates all partners
- No frontend-to-Keycloak direct access

## Governance

Constitution MUST supersede all other practices. Amendments require:
1. ADR documenting the change and rationale
2. Version bump per semantic versioning rules
3. Migration plan for affected artifacts
4. Compliance review against all principles

Versioning:
- MAJOR: Backward incompatible governance/principle removals or redefinitions
- MINOR: New principle/section added or materially expanded guidance
- PATCH: Clarifications, wording, typo fixes, non-semantic refinements

MVP progression (each ends with mandatory stabilization sprint):
MVP-1 (Discovery UX) → MVP-2 (Operational Control) → MVP-3 (Identity & RBAC)
→ MVP-4 (Analytics Intelligence) → MVP-5 (Performance Hardening)
→ MVP-6 (Production)

Use AGENTS.md for runtime development guidance and EXECUTION-LOG.md for
session tracking.

**Version**: 1.0.1 | **Ratified**: 2026-06-10 | **Last Amended**: 2026-06-11
