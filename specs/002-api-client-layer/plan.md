# Implementation Plan: API Client Layer

**Branch**: `002-api-client-layer` | **Date**: 2026-06-13 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-api-client-layer/spec.md`

## Summary

Create the `@bm/api-client` shared TypeScript package providing three typed functions (`getStations`, `getStationById`, `getNearbyStations`) that serve as the single HTTP layer for both web and mobile frontends. All frontend traffic must go through this layer — no direct `fetch`/`axios` in apps.

## Technical Context

**Language/Version**: TypeScript (target ES2022, strict mode)

**Primary Dependencies**: None at package level — uses platform `fetch` internally; types depend on `@bm/types`

**Storage**: N/A (no persistence — pure HTTP client)

**Testing**: Vitest (aligned with monorepo toolchain) for unit + integration; msw for HTTP mocking

**Target Platform**: Web (React/Leaflet, browser `fetch`) + Mobile (React Native/Expo, RN `fetch`)

**Project Type**: Shared library/package (workspace package under `source/front/packages/@bm/api-client`)

**Performance Goals**: Sub-50ms overhead per call (client-side processing only; network latency excluded)

**Constraints**: No `fetch`/`axios` importable from app code; fully typed responses via `@bm/types`; single package consumed by both web and mobile

**Scale/Scope**: 3 functions wrapping 3 backend endpoints; consumed by 2 apps

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Gate | Status | Notes |
|------|--------|-------|
| I. Documentation-First | ✅ PASS | Spec exists at `specs/002-api-client-layer/spec.md` |
| II. LLM-Driven Execution | ✅ PASS | Spec-driven, constitution-guided |
| III. MVP Isolation | ✅ PASS | Sprint 2 of MVP-1 — no auth, admin, or analytics |
| IV. Complete Testing | ⚠️ Must verify | Tests required: unit + integration for all 3 functions |
| V. Backend Architecture | ✅ N/A | Pure frontend package |
| VI. Frontend Architecture | ✅ PASS | Follows `@bm/api-client` mandatory dependency rule |
| VII. Data Ownership | ✅ N/A | Data passes through; no storage |
| VIII. Skill System | ✅ PASS | No skills violated |

**No violations to justify.**

## Project Structure

### Documentation (this feature)

```text
specs/002-api-client-layer/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── README.md
│   └── api-client.ts
└── tasks.md             # Phase 2 output (NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
source/front/packages/@bm/api-client/
├── src/
│   ├── index.ts               # Public exports
│   ├── client.ts              # ApiClient class/factory
│   ├── errors.ts              # Typed error classes
│   ├── transport.ts           # HTTP transport abstraction
│   └── types.ts               # Internal request/response types
├── tests/
│   ├── client.test.ts         # Unit tests for client methods
│   └── integration.test.ts    # Integration tests (mocked HTTP)
├── package.json
└── tsconfig.json
```

**Structure Decision**: Monorepo workspace package under `source/front/packages/@bm/api-client`. Follows existing `@bm/types`, `@bm/utils`, `@bm/design-tokens` layout.

## Complexity Tracking

No constitution violations to justify.
