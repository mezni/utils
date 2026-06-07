# Implementation Plan: Monorepo and CI/CD Setup

**Branch**: `001-monorepo-ci-cd` | **Date**: 2026-06-07 | **Spec**: specs/001-monorepo-ci-cd/spec.md

**Input**: Feature specification from `specs/001-monorepo-ci-cd/spec.md`

## Summary

Set up the BorneMap monorepo with Cargo workspace (Rust 1.95) and npm workspace (Node 20.20), create shared crates (ev-core, ev-db), configure 6 GitHub Actions CI workflows, and provide Docker Compose for local development.

## Technical Context

**Language/Version**: Rust 1.95, Node.js 20.20, npm 10.8

**Primary Dependencies**:
- Rust: actix-web 4, sqlx 0.8 (postgres + runtime-tokio + tls-native-tls), tokio 1, serde 1, tracing 0.1, chrono 0.4, nanoid 0.4, thiserror 1, dotenvy 0.15
- JS/TS: React 18, Vite 5, Tailwind CSS 3.4, Leaflet 1.9, Expo SDK 54, React Native 0.76.5, react-native-maps 1.18

**Storage**: PostgreSQL 16 + PostGIS 3.4 (defined in Docker Compose, not wired to services yet in this sprint)

**Testing**: cargo test (unit + integration via sqlx::test), cargo clippy --all-targets -- -D warnings, cargo fmt --all -- --check, npm test (vitest), npm lint (eslint)

**Target Platform**: Linux (CI: GitHub Actions ubuntu-latest), Docker containers (debian:bookworm-slim for Rust services)

**Project Type**: Monorepo with multiple services (web-service), frontend apps (web-app + mobile-app), and shared libraries (crates + packages)

**Performance Goals**: CI complete in under 10 minutes per push; npm install under 2 minutes with caching; cargo build under 5 minutes

**Constraints**: Operable by one person; no additional Docker containers without ADR; all endpoints under /api/v1 prefix

**Scale/Scope**: 2 Rust services, 3 frontend apps, 2 shared crates, 4 shared packages. Single developer operating everything.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Principle I (Pragmatic Architecture)**: Compliant. 6 CI workflows match existing service count. No new services introduced.
- **Principle III (Simple Operations)**: Compliant. CI is standard GitHub Actions. Compose file for local dev is simple Docker Compose.
- **Principle V (Build for Current Scale)**: Compliant. 6 path-scoped workflows are proportional to project size. No complex CI matrix needed.
- **Principle IX (API Prefix)**: Not applicable to this sprint (no live endpoints).

**Gate status**: ✅ PASS — no violations. Complexity tracking not required.

## Project Structure

### Documentation (this feature)

```text
specs/001-monorepo-ci-cd/
├── plan.md              # This file
├── spec.md              # Feature specification
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
ev-platform/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       ├── ci-driver-service.yml
│       ├── ci-admin-service.yml
│       ├── ci-driver-web.yml
│       ├── ci-driver-mobile.yml
│       └── ci-dashboard.yml
├── apps/
│   ├── driver-web/          # React + Vite scaffold
│   ├── driver-mobile/       # Expo SDK 54 scaffold
│   └── dashboard/           # React + Vite scaffold
├── services/
│   ├── driver-service/      # Rust service scaffold
│   └── admin-service/       # Rust service scaffold
├── crates/
│   ├── ev-core/             # NanoIDs, shared enums
│   └── ev-db/               # PgPool, pagination
├── packages/
│   ├── ui/
│   ├── api-client-driver/
│   ├── api-client-admin/
│   └── api-client-events/
├── db/
│   ├── migrations/
│   └── seeds/
├── infra/
│   ├── compose/
│   │   ├── docker-compose.yml       # Dev (includes pgadmin)
│   │   └── docker-compose.prod.yml  # Prod (no pgadmin)
│   └── env/
│       ├── .env.example
│       ├── driver-service.env.example
│       └── admin-service.env.example
├── docs/
│   ├── planning/
│   ├── architecture/
│   ├── api/
│   ├── adr/
│   └── ...
├── Cargo.toml                   # Workspace root
├── Cargo.lock
├── package.json                 # npm workspace root
├── tsconfig.base.json
├── .eslintrc.base.js
├── .prettierrc
├── .gitignore
└── .dockerignore
```

**Structure Decision**: Monorepo with Cargo workspaces for Rust (services + crates) and npm workspaces for JS/TS (apps + packages). Clear separation by language tooling.

## Complexity Tracking

> Not needed — Constitution Check passed without violations.
