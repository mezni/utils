# Implementation Plan: Mock API and Design System Foundation

**Branch**: `001-mock-api-and-design-system-foundation` | **Date**: 2026-06-09 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from Sprint 1.1 — Mock API and Design System Foundation

## Summary

Set up the foundation for all frontend development: a json-server mock API serving 4 resources (partners, stations, chargers, availability) under the /api prefix with seeded Tunisian EV data, plus a shared design token package (colors, typography, spacing, radius, shadows) consumable by all three apps, and a pnpm workspace orchestrating everything.

## Technical Context

**Language/Version**: Node.js 18+, json-server 0.17.x

**Primary Dependencies**: json-server, pnpm (workspace manager), TypeScript 5.x, Tailwind CSS 3.x for web apps

**Storage**: source/mock/db.json (file-based, no database for MVP-1)

**Testing**: Manual verification via curl/httpie against json-server endpoints; TypeScript compilation for token files

**Target Platform**: Local development (localhost:3001 for API, separate dev servers for each app)

**Project Type**: mock-api + design-system-library + workspace-orchestration

**Performance Goals**: json-server startup within 5 seconds; all token compilation within 3 seconds

**Constraints**: All API endpoints under /api prefix via routes.json rewrite; no authentication

**Scale/Scope**: 3 partners, 15 stations, 24 chargers, 15 availability records

## Constitution Check

*GATE: Pre-research evaluation against project constitution. Re-checked after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| P1 (MVP-first delivery) | ✅ PASS | json-server is the minimum viable API |
| P10 (API prefix consistency) | ✅ PASS | routes.json maps /api/* → /$1 |
| P9 (Visual consistency) | ✅ PASS | packages/ui shared token package |
| P6 (Domain separation) | ✅ PASS | Mock data organized by resource type |
| P4 (Single source of truth) | ✅ PASS | db.json is the single mock data file |
| P5 (Simple operations) | ✅ PASS | Single pnpm mock command starts everything |

**No constitutional violations. Complexity tracking not needed.**

## Project Structure

### Documentation (this feature)

```text
specs/001-Mock-API-and-Design-System-Foundation/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Not needed (json-server auto-exposes resources)
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
source/
├── mock/
│   ├── db.json           # MVP-1 mock data — 4 resources
│   └── routes.json       # /api/* prefix rewrite
├── packages/
│   └── ui/
│       ├── src/
│       │   └── tokens/
│       │       ├── colors.ts
│       │       ├── typography.ts
│       │       ├── spacing.ts
│       │       ├── radius.ts
│       │       ├── shadows.ts
│       │       ├── native.ts
│       │       └── index.ts
│       └── tailwind.config.base.js
├── apps/                  # Created in later sprints
├── services/              # Created in MVP-2+
└── database/              # Created in MVP-2+
```

**Structure Decision**: Monorepo with `source/` as root. This sprint creates only `source/mock/` and `source/packages/ui/`. All other directories remain empty until their respective sprints.

## Complexity Tracking

Not needed — no constitutional violations.

## Phase 0 — Research

All technical decisions are well-defined by the constitution and implementation plan. No NEEDS CLARIFICATION markers. See [research.md](research.md) for consolidated findings.

## Phase 1 — Design & Contracts

### Data Model

See [data-model.md](data-model.md) for complete entity definitions, fields, validation rules, and seed data strategy.

### Contracts

No external interfaces defined in this sprint. json-server auto-exposes REST endpoints for all resources in db.json. Routes are mapped by [routes.json](../../source/mock/routes.json). Frontend apps consume these endpoints directly.

### Quickstart

See [quickstart.md](quickstart.md) for setup instructions, mock server usage, and verification steps.
