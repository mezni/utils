# Implementation Plan: Project Scaffolding & CI/CD

**Branch**: `001-project-scaffolding-cicd` | **Date**: 2026-05-25 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-project-scaffolding-cicd/spec.md`

## Summary

Scaffold the BorneMap monorepo with workspace directories, build
configuration, Docker Compose local stack, and GitHub Actions CI
pipelines. Deliver a working development environment where all three
frontend apps compile, the backend starts with health check, and code
quality gates run automatically on every push.

## Technical Context

**Language/Version**: Rust 1.78+, Node.js 20, TypeScript 5.x

**Primary Dependencies**:
- Backend: Actix-web, SQLx, Tokio, PostGIS
- Frontend: React 18, Vite, Tailwind CSS, Leaflet, Expo SDK 51
- Tooling: pnpm 9, Cargo, Docker Compose, sqlx-cli

**Storage**: PostgreSQL 16+ with PostGIS extension (local dev only)

**Testing**: `cargo test` (Rust), `pnpm test` (frontend — placeholder)

**Target Platform**: Linux (Docker), web browsers (Chrome, Firefox,
Safari), iOS/Android via Expo Go

**Project Type**: Monorepo — single backend binary + three frontend
applications (web admin, web partner, mobile driver)

**Performance Goals**: N/A at scaffolding stage. CI backend checks
complete in under 10 minutes.

**Constraints**: None at this stage. Constitutional constraints
(cf. constitution Principle I) will apply when services are built.

**Scale/Scope**: 1 backend binary, 3 frontend apps, 1 shared UI
package, 3 CI workflows covering the full workspace.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Gate | Assessment |
|-----------|------|------------|
| I. Modular Monorepo Architecture | All code under `sources/`; `/api/v1/*` namespace | ✅ Compiled — directory tree enforces `sources/` layout. No API routes yet (Phase 1+). |
| II. Semantic Identity & Data Isolation | `USR-`/`PRT-`/`STN-` prefixes; `is_test` filter; soft delete | ✅ N/A at scaffolding stage — no schema or data. Gates verified when migrations are written (Phase 1 of larger plan). |
| III. Administrative UX Discipline | Design tokens; ScrollableTable; destructive confirmation | ✅ N/A at scaffolding stage — no UI built yet. Gates verified when admin portal phases begin. |
| IV. Mobile & Discovery Constraints | Expo Go; 20km radius; LIMIT 50 | ✅ Expo SDK 51 scaffold uses managed workflow. Discovery constraints verified in later phases. |
| V. Deterministic Implementation | Modular domain layers; seed script; sandbox indicator | ✅ Domain module structure is fixed in the directory tree. Seeds and indicator verified in later phases. |

**Result**: PASS — no violations. All gates that apply to scaffolding
are satisfied. Remaining gates are deferred to the phases where the
relevant code is written.

## Project Structure

### Documentation (this feature)

```
specs/001-project-scaffolding-cicd/
├── plan.md              # This file
├── research.md          # Phase 0 output — tooling decisions
├── data-model.md        # Phase 1 output — project structure entities
├── quickstart.md        # Phase 1 output — setup guide
├── contracts/           # Phase 1 output — file layout conventions
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```
bornemap-monorepo/
├── Cargo.toml                        # Rust workspace virtual manifest
├── docker-compose.dev.yml            # PostGIS + backend services
├── .gitignore
├── .github/
│   └── workflows/
│       ├── backend.yml               # Rust: fmt → clippy → test → build
│       ├── frontend.yml              # pnpm: lint → type-check → build
│       └── docker.yml                # Docker Compose smoke test
├── sources/
│   ├── backend/
│   │   ├── Cargo.toml
│   │   ├── Dockerfile.dev            # Rust dev container
│   │   ├── migrations/               # SQLx migration scripts (Phase 1+)
│   │   ├── src/
│   │   │   └── main.rs               # Actix-web hello-world on :8080
│   │   └── sqlx-data.json            # SQLx offline query cache
│   └── frontend/
│       ├── package.json              # pnpm workspace root
│       ├── pnpm-workspace.yaml       # Workspace definition
│       ├── packages/
│       │   └── ui/
│       │       ├── package.json
│       │       ├── tsconfig.json
│       │       └── src/
│       │           └── components/
│       │               └── ui/
│       │                   └── scrollable-table.tsx  # Placeholder
│       └── apps/
│           ├── admin-portal/
│           │   ├── package.json
│           │   └── src/
│           ├── partner-dashboard/
│           │   ├── package.json
│           │   └── src/
│           └── mobile-driver/
│               ├── app.json
│               ├── package.json
│               └── src/
```

**Structure Decision**: Monorepo with `sources/backend/` for the single
Rust binary and `sources/frontend/` for all client applications sharing
a common `packages/ui/` design system. This matches the constitution's
fixed repository structure.

## Complexity Tracking

No constitutional violations identified. Complexity tracking is not
required for this phase.
