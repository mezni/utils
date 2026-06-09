# Implementation Plan: Driver Mobile App

**Branch**: `005-driver-mobile-app` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/005-driver-mobile-app/spec.md`

## Summary

Build a standalone Driver Mobile App — React Native + Expo SDK 54 app with a full-screen map (react-native-maps) showing station markers from json-server, color-coded by availability (green/red), with location permission handling and navigation to Station Detail. Two screens (Map, Station Detail) following the same patterns as the Driver Web App (Sprint 1.4).

## Technical Context

**Language/Version**: TypeScript (React Native via Expo SDK 54)

**Primary Dependencies**: expo (SDK 54), react-native-maps, expo-location, @react-navigation/native + @react-navigation/native-stack, react-native-safe-area-context, react-native-screens, @tanstack/react-query

**Storage**: N/A — data from json-server at host machine IP

**Testing**: Manual verification on iOS Simulator + Android Emulator (no test framework in MVP-1)

**Target Platform**: iOS 15+ / Android 8+ (Expo managed workflow)

**Project Type**: Mobile app (React Native) — 2 screens (Map, Station Detail) with shared types

**Performance Goals**: Map loads station markers within 5 seconds under standard mobile connection (SC-001)

**Constraints**: Location permission graceful denial (Tunisia fallback); no search/filter/list view; no authentication; partner visibility computed client-side; API accessed via host machine IP (not localhost)

**Scale/Scope**: 1 new app, 2 screens, 0-2 shared components (MarkerCallout, ChargerRow)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| # | Principle | Check | Result |
|---|-----------|-------|--------|
| 1 | Dashboard-first delivery | Dashboard was Sprint 1.2/1.3, Driver Web Sprint 1.4 — Mobile is Sprint 1.5 | PASS |
| 2 | MVP-first delivery | Minimal scope: map + detail only, no search/filters/list | PASS |
| 3 | Single source of truth | json-server remains sole data source | PASS |
| 4 | Visual consistency | No shared design tokens needed (native maps, no Tailwind) — but color conventions match Driver Web (#00E676 green, #EF4444 red) | PASS |
| 5 | API prefix consistency | App connects to json-server directly via host IP — no proxy needed | PASS |
| 6 | No authentication | No auth in MVP-1 — mobile app is public | PASS |
| 7 | Partner visibility | Client-side filter — same approach as Driver Web | PASS |

**Note**: Constitution (`constitution.md`) is still a template with `[PLACEHOLDER]` values — not yet ratified. No enforceable gates beyond above project conventions.

**Gate verdict**: ALL PASS — proceed to Phase 0

## Project Structure

### Documentation (this feature)

```text
specs/005-driver-mobile-app/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (separate command)
```

### Source Code (repository root)

```text
source/apps/driver-mobile/
├── app.json
├── App.tsx
├── package.json
├── tsconfig.json
├── babel.config.js
├── src/
│   ├── api/
│   │   └── client.ts        # fetch wrappers (same pattern as driver-web)
│   ├── screens/
│   │   ├── MapScreen.tsx     # Full-screen map with markers
│   │   └── StationDetailScreen.tsx  # Station info + charger list
│   ├── components/
│   │   └── ChargerRow.tsx    # Charger list item (reused from driver-web pattern)
│   ├── navigation/
│   │   └── AppNavigator.tsx  # React Navigation stack
│   └── types/
│       └── index.ts          # Partner, Station, Charger interfaces
```

**Structure Decision**: New Expo project under `source/apps/driver-mobile/`. Follows Expo managed workflow conventions. Screen-based structure (not page-based like the web apps). Navigation is handled by React Navigation's native-stack navigator. Types are shared across screens via a dedicated types module.

## Complexity Tracking

No constitution violations — complexity tracking not required.

---

## Phase 0: Research

### Unknowns & Research Tasks

| # | Unknown | Research Task |
|---|---------|---------------|
| R01 | Map library choice | react-native-maps vs expo-maps — which provides the best platform-native map experience? |
| R02 | Location permission flow | How to request foreground location permission with expo-location and handle grant/deny gracefully |
| R03 | Navigation pattern | How to set up @react-navigation/native-stack with TypeScript, pass route params for station detail |
| R04 | Data fetching strategy | Should we use @tanstack/react-query for caching/refetching or plain fetch + useState like Driver Web? |
| R05 | API connection from simulator | How to reach json-server running on host machine from iOS Simulator and Android Emulator |

---

## Phase 1: Research Output

### R01 — Map Library Choice

**Decision**: Use `react-native-maps` (Google Maps on Android, Apple Maps on iOS via `PROVIDER_GOOGLE` or Apple Maps provider).

**Rationale**: react-native-maps is the most mature and widely used map library for React Native. It provides platform-native map rendering (not WebView-based), supports custom markers with colors, callouts, and region control. Expo SDK 54 includes react-native-maps as a compatible dependency.

**Alternatives considered**: `expo-maps` (newer, less mature, fewer customization options for marker colors/callouts), WebView-based Leaflet (loses native feel, no platform gesture integration).

### R02 — Location Permission Flow

**Decision**: Use `expo-location` to request `LOCATION_FOREGROUND` permission on mount. If granted, use `getCurrentPositionAsync` to center the map. If denied, fall back to Tunisia center (33.8869, 9.5375).

**Rationale**: expo-location is the standard Expo module for location. It handles the iOS/Android permission dialogs consistently and provides a simple async API. No need for `requestForegroundPermissionsAsync` wrapper — expo-location handles this natively.

**Alternatives considered**: `react-native-permissions` (more flexible but unnecessary complexity), `navigator.geolocation` (deprecated in React Native).

### R03 — Navigation Pattern

**Decision**: Use `@react-navigation/native-stack` with typed route params. The stack has two screens: `Map` and `StationDetail`. The `StationDetail` screen receives `{ stationId: string }` as a route param.

**Rationale**: React Navigation is the de facto standard for React Native navigation. Native stack provides platform-native transitions. Typed route params via TypeScript generics provide compile-time safety (same approach as the web's typed route params).

**Alternatives considered**: Expo Router (file-based routing — newer, less battle-tested), plain React Navigation stack (native-stack is more performant).

### R04 — Data Fetching Strategy

**Decision**: Use `@tanstack/react-query` for data fetching with automatic caching and refetch-on-focus.

**Rationale**: React Query handles loading/error states, caching, refetch on app foreground, and deduplication — all valuable on mobile where network conditions vary and users switch between apps. It's a lightweight addition that prevents boilerplate compared to plain fetch + useState.

**Alternatives considered**: Plain fetch + useState (used in Driver Web, but mobile benefits from caching), Redux Toolkit Query (overkill for 2 screens), SWR (similar but less mature in RN ecosystem).

### R05 — API Connection from Simulator

**Decision**: Use the host machine's local IP address (e.g., `http://192.168.x.x:3001/api`) for simulator connections. Detect platform at build time via `Platform.OS` and expose the base URL through a config constant.

**Rationale**: iOS Simulator can reach `localhost` on the host machine directly, but Android Emulator maps `localhost` to itself. Using the machine's LAN IP works for both. This matches the assumption stated in the spec.

**Alternatives considered**: `localhost` with proxy (Android-specific workaround), ngrok (adds external dependency), `10.0.2.2` for Android (only works for Android Emulator, not iOS Simulator).
