# Implementation Plan: MVP-1 Foundation Setup

**Branch**: `main` | **Date**: 2026-06-08 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-mvp1-mock-api-tokens/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Set up the MVP-1 foundation: json-server mock API with seeded Tunisian EV station data under `/api` prefix, a shared design token package for visual consistency across all three apps, and a pnpm monorepo workspace with root-level dev scripts.

## Technical Context

**Language/Version**: Node.js (json-server runtime), TypeScript 5.x (design tokens)

**Primary Dependencies**: json-server 0.x, pnpm 9.x, TypeScript, concurrently

**Storage**: `source/mock/db.json` (JSON file-based, no database)

**Testing**: Manual verification via curl / browser — json-server requests are verified by inspection

**Target Platform**: All three applications (Dashboard web, Driver web, Driver mobile)

**Project Type**: Monorepo (json-server mock + shared package + 3 app stubs)

**Performance Goals**: N/A — mock API, no performance targets

**Constraints**: `/api` prefix via routes.json, port 3001, no custom backend logic, client-side nearby filtering only

**Scale/Scope**: 3 partners, 15 Tunisian stations, 24 chargers; ~15 token files in packages/ui

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Principle I (MVP-First)**: This sprint is the exact minimum to prove the core loop — mock API + tokens. No over-engineering. ✅
- **Principle III (Dashboard First)**: Dashboard app stub is created; Dashboard CRUD screens are deferred to Sprint 1.2 per spec scope. ✅
- **Principle IX (Visual Consistency)**: Design token package is the focus — no hardcoded values. ✅
- **Principle X (API Prefix)**: All endpoints under `/api` via routes.json. ✅
- **Principle VII (Public Access)**: API has no auth — anonymous browsing works. ✅

**No violations found. Gate passes.**

## Project Structure

### Documentation (this feature)

```text
specs/001-mvp1-mock-api-tokens/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
source/
├── mock/
│   ├── db.json          # Seeded data: 3 partners, 15 stations, 24 chargers
│   └── routes.json      # /api/* → /$1 prefix mapping
└── packages/
    └── ui/
        ├── src/
        │   └── tokens/
        │       ├── colors.ts
        │       ├── typography.ts
        │       ├── spacing.ts
        │       ├── radius.ts
        │       ├── shadows.ts
        │       ├── native.ts
        │       └── index.ts
        ├── tailwind.config.base.js
        └── package.json

pnpm-workspace.yaml
package.json              # Root scripts: mock, dev:dashboard, dev:web, dev:mobile, dev
```

**Structure Decision**: The monorepo structure follows the constitution's prescribed layout. Sprint 1.1 creates only `source/mock/` and `source/packages/ui/`. The three app directories (`source/apps/driver-web`, `source/apps/driver-mobile`, `source/apps/dashboard`) will be scaffolded in subsequent sprints.

## Complexity Tracking

> *No constitution violations — this section is intentionally left empty.*
