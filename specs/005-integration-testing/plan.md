# Implementation Plan: Integration & Testing

**Branch**: `005-integration-testing` | **Date**: 2026-06-12 | **Spec**: `specs/005-integration-testing/spec.md`

**Input**: Feature specification from `/specs/005-integration-testing/spec.md`

## Summary

Wire up Traefik API gateway, connect mobile and web apps through it, and run end-to-end tests covering the complete station discovery flow, event logging, dark mode, error handling, contract compliance, and performance benchmarks under 50 concurrent requests.

## Technical Context

**Language/Version**: TypeScript (mobile/web apps), Rust 1.75+ (backend services), Docker Compose (infrastructure)

**Primary Dependencies**: Traefik v2.10+ (API gateway), existing driver-service and admin-service Rust binaries, existing Expo mobile app and React web app, @tanstack/react-query (data fetching)

**Storage**: PostgreSQL 16 + PostGIS (platform_db), analytics_db (append-only raw_events table) — both pre-existing from Phase 1-2

**Testing**: Jest (unit tests), Pact or similar (contract tests), Maestro or Detox (mobile E2E), k6 or autocannon (load/performance tests)

**Target Platform**: Android emulator / iOS simulator (mobile E2E), Chrome/Firefox/Safari (web), Docker Compose (Traefik + backend services)

**Project Type**: Integration & E2E testing for existing microservice + mobile + web architecture

**Performance Goals**: Nearby search <100ms p95 (50 concurrent requests), map markers appear within 5s, station detail loads within 2s

**Constraints**: All client traffic through Traefik (per constitution), no direct service-to-client access, events are append-only, services must not query outside their domain

**Scale/Scope**: 5 user stories (3 P1, 2 P2), 16 functional requirements, mobile app + web app + 2 backend services + Traefik gateway

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Assessment | Status |
|-----------|------------|--------|
| I. UX-First | E2E tests verify skeleton screens, dark mode, error recovery actions (retry + back nav), and haptic feedback | ✅ Pass |
| II. Domain-Driven Services | Traefik routes to correct service per domain (driver-service: discovery, admin-service: management+events) | ✅ Pass |
| III. Test-First | This phase IS dedicated to testing. 100% contract test coverage mandated. E2E tests cover critical user flows | ✅ Pass |
| IV. Source-Rooted Codebase | No new source code — tests live in existing `source/front/*/` test directories and `source/services/*/tests/` | ✅ Pass |
| V. Immutable Data & Append-Only Analytics | Events verified as append-only in E2E tests. Soft delete tested on station deactivation | ✅ Pass |
| Stack & Tooling | Traefik as API gateway (mandated). No new technology introduced beyond what stack mandates | ✅ Pass |
| Security & Governance | Auth testing limited to graceful 401/403 rejection — full JWT flow deferred to MVP-3 per constitution | ✅ Pass |

**All gates pass. No violations requiring Complexity Tracking.**

## Project Structure

### Documentation (this feature)

```text
specs/005-integration-testing/
├── plan.md              # This file
├── research.md          # Phase 0: Technical decisions
├── data-model.md        # Phase 1: Test event schema, test data
├── quickstart.md        # Phase 1: Setup & run tests
├── contracts/           # Phase 1: Event contract, Traefik routing contract
└── tasks.md             # (created by /speckit.tasks)
```

### Source Code (repository root)

No new source code directories. This phase adds tests to existing project structure:

```text
source/
├── services/
│   ├── driver-service/tests/        ← Contract tests, unit tests
│   └── admin-service/tests/         ← Contract tests, unit tests
├── front/
│   ├── mobile-driver/               ← E2E tests added (e.g., __e2e__/)
│   └── web-driver/                  ← E2E tests added (e.g., __e2e__/)
infra/
├── docker-compose.yml               ← Traefik service added
└── traefik/                         ← Traefik configuration (dynamic.yml)
```

**Structure Decision**: Tests are co-located with their respective projects following the existing repository conventions. Traefik configuration is added to `infra/` alongside existing Docker Compose and migration files.

## Complexity Tracking

No violations — all constitution gates pass without exception.

## Phases

### Phase 0: Research & Decisions

Research tasks (all resolved via spec + clarification session, documented in `research.md`):
- R-001: Test framework selection for mobile E2E (Maestro vs Detox)
- R-002: Contract testing tool selection (Pact vs alternatives)
- R-003: Traefik routing configuration for local development
- R-004: Load testing tool selection (k6 vs autocannon)
- R-005: CI pipeline integration strategy for integration tests

### Phase 1: Design & Contracts

Outputs:
- `data-model.md`: Event record schema, test data specification, test station requirements
- `contracts/`: Event API contract, Traefik routing contract reference
- `quickstart.md`: Setup instructions for running integration tests
- Agent context update (AGENTS.md)
