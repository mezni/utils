# Implementation Plan: Dev Environment + CI/CD + Runnable Skeleton

**Branch**: `001-dev-env-skeleton` | **Date**: 2026-05-27 | **Spec**: specs/001-dev-env-skeleton/spec.md

**Input**: Feature specification from `specs/001-dev-env-skeleton/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Build a fully reproducible monorepo developer environment spanning a Rust/Actix
Web backend (health endpoints only), an Expo Go mobile app connected to the
backend, a shared types package, Docker infrastructure, and CI/CD pipelines.
Phase 1 explicitly excludes all product logic — the goal is a runnable skeleton
with zero ambiguity in the clone-to-verify loop.

## Technical Context

**Language/Version**: Rust (stable toolchain), TypeScript with Node.js 18+

**Primary Dependencies**:
- Rust: actix-web, tokio, serde, serde_json, nanoid, env_logger
- Mobile: React Native, Expo Go
- Build: pnpm, turbo, cargo (workspaces)

**Storage**: N/A — no database in Phase 1

**Testing**: cargo test (Rust backend), vitest (frontend, future)

**Target Platform**: Linux/macOS development hosts; Docker for CI

**Project Type**: Multi-package monorepo — backend web-service + mobile app +
shared library

**Performance Goals**: Health endpoints respond in <100ms; CI pipeline
completes in <5 minutes

**Constraints**: No database, no authentication, no product logic; zero
external service dependencies for local development

**Scale/Scope**: 1-3 developers; single-machine dev environment; GitHub CI

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*
*Re-checked after Phase 1 design: ALL PASS — no design changes introduced constitution violations.*

| Principle | Assessment | Status |
|-----------|-----------|--------|
| I. Validation-First | Phase 1 is pre-validation infra — no product hypothesis to validate yet. Justified as foundational prerequisite. | PASS |
| II. Rust-First Backend | Actix Web + Rust backend per spec. | PASS |
| III. Access Isolation | No auth in Phase 1. Deferred to later phases. | PASS (deferred) |
| IV. Spatial UX Excellence | No spatial UX in Phase 1. Begins Phase 2+. | PASS (deferred) |
| V. Controlled Complexity | Minimal skeleton with no business logic. YAGNI applied. | PASS |

No violations. All gates pass.

## Project Structure

### Documentation (this feature)

```text
specs/001-dev-env-skeleton/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   └── health-api.md
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
bornemap/
├── Cargo.toml                  # Workspace root
├── package.json                # pnpm root
├── pnpm-workspace.yaml
├── turbo.json
├── .env.example
├── .gitignore
│
├── services/
│   └── core-service/           # Actix Web backend
│       ├── Cargo.toml
│       └── src/
│           └── main.rs
│
├── frontends/
│   └── apps/
│       └── mobile-driver/     # Expo Go app
│           ├── package.json
│           ├── app.json
│           └── App.tsx
│
├── shared/
│   └── bornemap-types/        # Shared Rust types
│       ├── Cargo.toml
│       └── src/
│           └── lib.rs
│
├── infrastructure/
│   ├── docker/
│   │   └── Dockerfile         # core-service Docker image
│   ├── docker-compose.dev.yml
│   └── docker-compose.test.yml
│
└── .github/workflows/
    ├── lint.yml
    ├── test.yml
    └── build.yml
```

**Structure Decision**: Monorepo with `services/`, `frontends/`, `shared/`, and
`infrastructure/` — matching the user-specified layout from the Phase 1 spec.
This isolates backend, mobile, and shared concerns cleanly while keeping them
in a single repository.

## Complexity Tracking

> No constitution violations requiring justification.
