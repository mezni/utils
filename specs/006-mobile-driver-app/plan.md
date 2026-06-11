# Implementation Plan: Mobile Driver App (Core UX)

**Branch**: `006-mobile-driver-app` | **Date**: 2026-06-11 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/006-mobile-driver-app/spec.md`

## Summary

Build the primary Borne driver mobile app — an Expo SDK 54 React Native application with a full-screen map, station markers with nearby search integration, a station detail bottom sheet, skeleton-first loading, and clickstream event tracking. This is the core UX screen that ties together the Driver Service, Clickstream Service, and Design System.

## Technical Context

**Language/Version**: TypeScript 5.x

**Primary Dependencies**: expo (SDK 54), react-native, react-native-maps, expo-router, expo-location, react-native-reanimated v3, react-native-safe-area-context, @borne/design-system (internal workspace package)

**Storage**: N/A — stateless mobile client (no on-device persistence for MVP-1)

**Testing**: Jest + React Native Testing Library (component tests), manual device/simulator testing (map interactions, gestures, animations)

**Target Platform**: iOS 15+ / Android 12+ (via Expo managed workflow)

**Project Type**: mobile-app (React Native / Expo)

**Performance Goals**: Map renders within 3 seconds, bottom sheet opens within 1 second of marker tap, skeleton appears within 100ms of mount

**Constraints**: Reanimated v3 only (no Animated API), skeleton-first loading (no blank screens), dark mode mandatory, fire-and-forget event tracking (never blocks UX)

**Scale/Scope**: 1 screen (Map Screen), 1 bottom sheet (Station Detail), ~15 TypeScript source files

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution is a template-only file (`.specify/memory/constitution.md` is unpopulated). No constitution gates to evaluate.

## Project Structure

### Documentation (this feature)

```text
specs/006-mobile-driver-app/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
source/front/
├── app/                          # Expo Router file-based routing
│   ├── _layout.tsx               # Root layout (ThemeProvider, gesture handler)
│   └── index.tsx                 # Map Screen (default route)
├── src/
│   ├── components/
│   │   ├── MapScreen.tsx         # Full-screen map with markers
│   │   ├── StationMarker.tsx     # Single marker annotation
│   │   ├── StationBottomSheet.tsx # Bottom sheet with station details
│   │   ├── ChargerList.tsx       # Charger entries inside sheet
│   │   └── MapErrorState.tsx     # Error state wrapper for map
│   ├── hooks/
│   │   ├── useNearbyStations.ts  # Fetches stations for map region
│   │   ├── useStationDetail.ts   # Fetches single station detail
│   │   ├── useLocation.ts        # GPS permission + location tracking
│   │   └── useClickstream.ts     # Fire-and-forget event sender
│   ├── services/
│   │   ├── api.ts                # Shared HTTP client (configurable base URL)
│   │   └── config.ts             # Environment config (service URLs)
│   └── types/
│       ├── station.ts            # Station, Charger, Marker types
│       └── events.ts             # Clickstream event types
├── packages/
│   └── design-system/            # Existing design system package
├── app.json
├── package.json
└── tsconfig.json
```

**Structure Decision**: Expo Router file-based routing under `source/front/app/` (standard Expo SDK 54 convention). Shared logic under `src/` organized by concern (components, hooks, services, types). The design system remains a separate workspace package under `packages/`. This keeps the app lean while allowing the design system to evolve independently.

## Complexity Tracking

> No Constitution violations — complexity justification not required.
