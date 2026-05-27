# Implementation Plan: BorneMap Platform Scaffold

**Branch**: `002-initial-scaffold` | **Date**: 2026-05-27 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-initial-scaffold/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Scaffold the full BorneMap platform: Rust backend workspace with mock api-service exposing station discovery via `/api/v1/stations/nearby`, React Native mobile driver app with map-based station browsing, Docker Compose local dev stack with PostGIS, GitHub Actions CI/CD, and Makefile operational tooling. All mock data uses Tunisian coordinates and nanouuid identifiers per constitution standards.

## Technical Context

**Language/Version**: Rust 2021 edition (backend), JavaScript/Node.js 18 (mobile)

**Primary Dependencies**: Actix-web 4.4, React Native via Expo SDK, react-native-maps, axios, serde, chrono, parking_lot, postgis/postgis:15-3.3

**Storage**: PostgreSQL 15 with PostGIS 3.3 extension

**Testing**: cargo test --workspace (backend), npx expo export --platform web (frontend build verification)

**Target Platform**: Linux server (api-service), iOS/Android via Expo Go (mobile driver app)

**Project Type**: web-service + mobile-app

**Performance Goals**: API response <100ms on local hardware, mobile map display <5s on standard device

**Constraints**: IDs MUST follow `^[a-z]{3}-[a-f0-9]{8}$` pattern; spatial coordinates MUST use SRID 4326; default map center MUST be Tunis, Tunisia; Docker Compose environment parity required

**Scale/Scope**: Tunis metro area only; mock data (2+ stations) for MVP; auth-service out of scope

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Compliance | Notes |
|-----------|------------|-------|
| I. Validation Before Optimization | ✅ Pass | Mock data scaffold — no scaling infrastructure deployed |
| II. Technical Stack Governance | ✅ Pass | Rust/Actix-web, PostGIS, React Native/Expo, Docker Compose all match locked stack |
| III. Architecture & Service Taxonomy | ✅ Pass | Gateway named `api-service`; `auth-service` separated; no `api/` references |
| IV. Data Architecture Standards | ✅ Pass | nanouuid IDs (`stn-`, `chg-`, `prv-`), SRID 4326, Tunis default map center |
| V. Operational Workflows & Deployment | ✅ Pass | CI/CD integrated, Docker Compose under `/deployments`, specs under `/spec` |
| Service Isolation & Security | ✅ Pass | Auth deferred (explicitly out-of-scope); no shared mutable state across services |
| Context Governance | ✅ Pass | Spec in `specs/001-initial-scaffold/`; plan, data model, contracts follow |

## Project Structure

### Documentation (this feature)

```text
specs/001-initial-scaffold/
├── spec.md              # Feature specification (/speckit.specify output)
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
borne-map/
├── .github/
│   └── workflows/
│       └── ci.yml
├── apps/
│   ├── mobile-driver/
│   │   ├── src/
│   │   │   ├── components/    # StationCard.js, etc.
│   │   │   ├── services/      # api.js
│   │   │   └── screens/       # MapScreen.js
│   │   └── App.js
│   └── web-admin/
├── backend/
│   ├── Cargo.toml
│   ├── api-service/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs
│   │       └── handlers/
│   │           └── locate.rs
│   ├── auth-service/
│   ├── core/
│   └── infra/
├── db/
│   ├── migrations/
│   └── seeds/
├── deployments/
│   ├── docker-compose.yml
│   ├── docker-compose.prod.yml
│   └── nginx/
├── docs/
│   ├── architecture.md
│   └── onboarding.md
├── .env.example
├── docker-compose.yml
└── Makefile
```

**Structure Decision**: Multi-crate Rust workspace (api-service, auth-service, core, infra) + mobile app under `apps/` + admin portal under `apps/web-admin/`, as specified in the constitution. Docker Compose at repo root for local dev, production overlay under `deployments/`.

## Complexity Tracking

> No Constitution Check violations — this section is intentionally empty.
