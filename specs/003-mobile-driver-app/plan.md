# Implementation Plan: Web Driver Client

**Branch**: `004-web-driver-client` | **Date**: 2026-06-18 | **Spec**: [`spec.md`](./spec.md)

**Input**: Feature specification from `/specs/004-web-driver-client/spec.md`

## Summary

Build a web driver client application (`source/apps/web-driver/`) using React + Leaflet for map rendering, constrained to Tunisia, displaying charging station markers fetched from `/api/v1/nearby`. Features 300ms viewport debouncing, shimmer loading states, localStorage offline cache with banner, error boundary with retry, and Zoom-out overlay at zoom < 4. Zero build-step native modules — runs entirely in browser with Tailwind styling.

## Technical Context

**Language/Version**: TypeScript strict mode (React 19 / Web)

**Primary Dependencies**: `react@19.2.7`, `react-dom@19.2.7`, `leaflet@1.9.4`, `@tanstack/react-query`, `tailwindcss`

**Storage**: localStorage (offline viewport cache only — no server-side storage for this sprint)

**Testing**: Manual via Chrome/Edge browser during validation; `jest` + `@testing-library/react` for unit tests

**Target Platform**: Modern browsers (Chrome 90+, Edge 90+, Safari 15+, Firefox 88+)

**Project Type**: Web application (React)

**Performance Goals**: Page load <2s, shimmer appears <200ms, cache write <100ms, offline fallback <500ms

**Constraints**: Must run in default browser (no build step), offline-capable via localStorage fallback, no crash reporting in v1

**Scale/Scope**: Validation phase — up to 1000 concurrent drivers via existing backend; single Tunisia region

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Gate | Status | Notes |
|------|--------|-------|
| I. No new microservice | ✅ PASS | Web client, not a service — topology unchanged |
| III. TypeScript strict mode, no `any` | ✅ PASS | Constitution mandates strict mode; must be enforced in tsconfig and lint |
| §5a. State-driven checklist (loading/success/empty/error) | ✅ PASS | US3 + FR-004 cover all four states |
| §5a. Viewport debounce ≥ 300ms | ✅ PASS | FR-002 specifies 300ms debounce |
| §5a. localStorage offline fallback | ✅ PASS | US4 + FR-005/FR-006 cover cache write and offline read |
| §5a. Zero input mutation | ✅ PASS | Coordinates validated via shared types (FR-002 uses device GPS) |
| Web-specific: Leaflet asset optimization | ✅ PASS | Custom markers bundled locally; Tailwind styling consistency enforced |

**No violations found.** Plan proceeds without exemptions.

## Project Structure

### Documentation (this feature)

```text
specs/003-mobile-driver-app/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
source/apps/web-driver/
├── index.html                   # Entry point with Leaflet CSS/JS loading
├── src/
│   ├── components/
│   │   ├── MapContainer.tsx     # Leaflet map wrapper, Tunisia bounds, markers
│   │   ├── StationMarker.tsx    # Leaflet marker with custom icon
│   │   ├── ShimmerSkeleton.tsx  # Loading shimmer for map area
│   │   ├── ErrorBoundary.tsx    # Visual error boundary with Retry Connection
│   │   ├── EmptyState.tsx       # Guidance message for no stations
│   │   ├── OfflineBanner.tsx    # "Viewing cached data" banner
│   │   └── ZoomOutOverlay.tsx   # Overlay at zoom < 4
│   ├── hooks/
│   │   ├── useDebounce.ts       # 300ms viewport debounce
│   │   └── useNearbyStations.ts # React Query hook for /api/v1/nearby
│   ├── services/
│   │   └── api.ts               # API client with configurable base URL
│   ├── cache/
│   │   └── localStorage.ts      # localStorage read/write for station cache
│   ├── types/
│   │   └── index.ts             # Station, Viewport, FetchState types
│   └── utils/
│       ├── coordinates.ts       # Tunisia bounds check, coordinate rounding
│       └── network.ts           # Network connectivity detection
├── assets/
│   └── markers/                 # Charging pin SVG/PNG assets for Leaflet
├── public/
│   ├── leaflet.css              # Leaflet styles (bundled)
│   └── leaflet.js               # Leaflet library (bundled)
├── package.json
├── tsconfig.json
└── .env                          # API_BASE_URL (gitignored)
```

**Structure Decision**: Single React web application under `source/apps/web-driver/` following standard React project layout. Custom markers bundled locally; Tailwind CSS for all styling. Shared business logic (types, API hooks, coordinate utils) collocated locally; extraction to `packages/shared-*` deferred until needed by both apps.

## Complexity Tracking

*No constitution violations — no justification needed.*
