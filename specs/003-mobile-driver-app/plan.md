# Implementation Plan: Mobile Driver App

**Branch**: `003-mobile-driver-app` | **Date**: 2026-06-18 | **Spec**: [`spec.md`](./spec.md)

**Input**: Feature specification from `/specs/003-mobile-driver-app/spec.md`

## Summary

Build an Expo SDK 54 mobile driver app (`source/apps/mobile-driver/`) with a full-screen `react-native-maps` map constrained to Tunisia, displaying charging station markers fetched from `/api/v1/nearby`. Features 300ms viewport debouncing, shimmer loading states, AsyncStorage offline cache with banner, error boundary with retry, and macro-zoom overlay at zoom < 8. Zero custom native modules — runs entirely in Expo Go.

## Technical Context

**Language/Version**: TypeScript strict mode (Expo SDK 54 / React Native 0.76+)

**Primary Dependencies**: `expo@54`, `react-native-maps`, `@react-native-async-storage/async-storage`, `expo-location`, `expo-constants`, `@tanstack/react-query`

**Storage**: AsyncStorage (offline viewport cache only — no server-side storage for this sprint)

**Testing**: Manual via Expo Go on physical devices during validation; `jest` + `@testing-library/react-native` for unit tests

**Target Platform**: iOS 15+ and Android 8+ via Expo Go

**Project Type**: Mobile application (Expo / React Native)

**Performance Goals**: Map renders in <4s on 4G, shimmer appears <200ms, cache write <100ms, offline fallback <500ms

**Constraints**: Must run in default Expo Go (zero native modules), offline-capable via AsyncStorage fallback, no crash reporting in v1

**Scale/Scope**: Validation phase — up to 1000 concurrent drivers via existing backend; single Tunisia region

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Gate | Status | Notes |
|------|--------|-------|
| I. No new microservice | ✅ PASS | Mobile app, not a service — topology unchanged |
| I. No native modules outside Expo Go | ✅ PASS | FR-012 explicitly bans custom native modules; SC-008 mandates Expo Go compatibility |
| III. TypeScript strict mode, no `any` | ✅ PASS | Constitution mandates strict mode; must be enforced in tsconfig and lint |
| §5a. State-driven checklist (loading/success/empty/error) | ✅ PASS | US3 + FR-004 cover all four states |
| §5a. Viewport debounce ≥ 300ms | ✅ PASS | FR-002 specifies 300ms debounce |
| §5a. AsyncStorage offline fallback | ✅ PASS | US4 + FR-005/FR-006 cover cache write and offline read |
| §5a. Zero input mutation | ✅ PASS | Coordinates validated via shared types (FR-002 uses device GPS) |

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
source/apps/mobile-driver/
├── app.json                     # Expo config with API_BASE_URL extra
├── App.tsx                      # Root component (providers + map)
├── src/
│   ├── components/
│   │   ├── MapContainer.tsx     # react-native-maps wrapper, Tunisia bounds, markers
│   │   ├── StationCallout.tsx   # Marker callout with name/distance/partner
│   │   ├── ShimmerSkeleton.tsx  # Loading shimmer for map area
│   │   ├── ErrorBoundary.tsx    # Visual error boundary with Retry Connection
│   │   ├── EmptyState.tsx       # Guidance message for no stations
│   │   ├── OfflineBanner.tsx    # "Viewing cached data" banner
│   │   └── MacroZoomOverlay.tsx # Overlay at zoom < 8
│   ├── hooks/
│   │   ├── useDebounce.ts       # 300ms viewport debounce
│   │   └── useNearbyStations.ts # React Query hook for /api/v1/nearby
│   ├── services/
│   │   └── api.ts               # API client with configurable base URL
│   ├── cache/
│   │   └── asyncStorage.ts      # AsyncStorage read/write for station cache
│   ├── types/
│   │   └── index.ts             # Station, Viewport, FetchState types
│   └── utils/
│       ├── coordinates.ts       # Tunisia bounds check, coordinate rounding
│       └── network.ts           # Network connectivity detection
├── assets/
│   └── markers/                 # Charging pin SVG/PNG assets
├── package.json
├── tsconfig.json
└── .env                          # API_BASE_URL (gitignored)
```

**Structure Decision**: Single Expo application under `source/apps/mobile-driver/` following standard Expo project layout. Shared business logic (types, API hooks, coordinate utils) collocated locally; extraction to `packages/shared-*` deferred until the web driver app also needs them.

## Complexity Tracking

*No constitution violations — no justification needed.*
