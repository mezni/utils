# Implementation Plan: Driver Mobile App with Mock Data

**Branch**: `003-driver-mobile-mock` | **Date**: 2026-06-06 | **Spec**: [spec.md](./spec.md)

**Input**: Sprint specification from `/specs/003-driver-mobile-mock/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Scaffold and populate the Driver Mobile App (`apps/driver-mobile`) with all 7 screens (Map/Home, Station List, Station Detail, Search, Favorites, Profile, Login/Register), 12 mobile-specific components, and realistic mock data (15 stations, 2–4 chargers each, 3–5 reviews each with Arabic/French content). The app uses Expo + React Native + TypeScript with native tokens from `packages/ui/src/tokens/native.ts`, React Navigation (bottom tabs + stack), react-i18next for Arabic/French with `I18nManager.forceRTL()`, and `react-native-safe-area-context` for safe area insets. The map is a placeholder (`#EAF0E6` background View with positioned pin markers). No backend calls — all data from local mock files.

## Technical Context

**Language/Version**: TypeScript 5.x with strict mode, React Native 0.76+, Expo SDK 52+

**Primary Dependencies**:
- Expo (~52+) — managed workflow for iOS + Android
- React Navigation 6 (bottom tabs: `@react-navigation/bottom-tabs`, stack: `@react-navigation/native-stack`, native: `@react-navigation/native`)
- react-i18next + i18next + expo-localization
- react-native-safe-area-context
- `packages/ui` (workspace dependency, native tokens)
- Jest + @testing-library/react-native (testing)
- expo-font (Plus Jakarta Sans font loading)

**Storage**: N/A — all data from local mock TypeScript files (same shape as Sprint 1.2 web mock data)

**Testing**: Jest (via Expo preset) + @testing-library/react-native. Testing is OPTIONAL per constitution (not explicitly requested in spec) but recommended for component variants.

**Target Platform**: iOS 15+ and Android 8+ via Expo managed workflow

**Project Type**: Expo + React Native mobile app (frontend only, no backend)

**Performance Goals**: FlatList renders 15 items without stutter, skeleton placeholders render on first mount (<100ms), debounced search fires <300ms after last keystroke

**Constraints**:
- No backend calls — all data from local mock files
- Map is a placeholder (`#EAF0E6` background View with absolutely positioned pin Views) — no real map library
- All visual values from `packages/ui/src/tokens/native.ts` (colors, spacing, typography, radius, shadows)
- Arabic RTL via `I18nManager.forceRTL()` on language switch — must work on every screen
- Safe area insets on MobileTopBar (top), BottomTabBar (bottom), via `react-native-safe-area-context`
- Social login buttons are visual-only (no OAuth)
- Profile form is static (no submission or validation)
- Favorites are mock data only (no persistence across restarts)
- All static strings translated in `ar.json` and `fr.json` (reuse web translations where possible)
- App targets both iOS and Android from a single Expo managed codebase
- Lock to portrait orientation is acceptable

**Scale/Scope**: 7 screens, 12 mobile-specific components, ~15 stations mock data, ~50 chargers, ~60 reviews

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Principle I: Pragmatic Architecture
✅ **PASS** — Single Expo + React Native application with minimal structure. No services, no databases. Architecture is minimal for the sprint's scope. Same approach as Sprint 1.2.

### Principle II: Single Source of Truth
✅ **PASS** — Visual values from `packages/ui/src/tokens/native.ts` (Sprint 1.1). Mock data is placeholder only, to be replaced by real API calls in Phase 5. Mock data shape matches web app.

### Principle III: Simple Operations
✅ **PASS** — Standard Expo dev workflow (`pnpm dev` or `npx expo start`). No complex operations.

### Principle IV: Domain Separation by Schema
N/A — No database schemas involved at this sprint stage.

### Principle V: Build for Current Scale
✅ **PASS** — Mock data approach is the simplest approach for mobile UI development. FlatList at 15 items needs no virtualization optimization. No premature architecture.

### Principle VI: Public Access First
✅ **PASS** — Map/Home, Station List, Station Detail, and Search screens are accessible without login. Authentication is only needed for Favorites and Profile — matching the spec's P3 prioritization.

### Principle VII: RTL & Arabic Built-In
✅ **PASS** — Arabic RTL required on every screen. Uses `I18nManager.forceRTL()` with `useEffect` on language change. Translations in `ar.json` and `fr.json`. RTL verification is a success criterion (SC-003). This is a non-negotiable rule per constitution.

### Principle VIII: Visual Consistency
✅ **PASS** — All visual values from `packages/ui/src/tokens/native.ts`. No hardcoded visual values. Token consumption pattern matches web app approach.

### Non-Negotiable Rules
- ✅ **inventory.station** — N/A (mock data, no database)
- ✅ **Public access** — Map/Home, Station List, Station Detail, and Search are public
- ✅ **Tokens not stored** — N/A (no auth tokens in this sprint)
- ✅ **Arabic RTL** — Required on every screen
- ✅ **Only Traefik** — N/A (mobile app, no network ports exposed)
- ✅ **Keycloak owns auth** — N/A (mock auth screens only, no real auth)
- ✅ **No additional services without ADR** — No services added
- ✅ **Cross-schema access** — N/A

**Overall Result**: ✅ **ALL GATES PASSED**

## Project Structure

### Documentation (this feature)

```text
specs/003-driver-mobile-mock/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
# Driver Mobile Application (new)
apps/driver-mobile/
├── src/
│   ├── components/
│   │   ├── MobileTopBar.tsx       — header with safe area top inset
│   │   ├── SearchBar.tsx          — TextInput with search icon
│   │   ├── FilterPills.tsx        — horizontal ScrollView pill row
│   │   ├── MapPinMarker.tsx       — positioned View with glow shadow
│   │   ├── ZoomControls.tsx       — floating +/- button group
│   │   ├── StationCard.tsx        — name, address, distance, charger info
│   │   ├── ChargerRow.tsx         — connector type, power, status badge
│   │   ├── ReviewCard.tsx         — author, rating, date, text
│   │   ├── BottomStationCard.tsx  — absolute-positioned bottom card
│   │   ├── SpecRow.tsx            — detail row with label and value
│   │   ├── CenterActionButton.tsx — raised circular button above tab bar
│   │   └── BottomTabBar.tsx       — custom tab bar with safe area bottom inset
│   ├── screens/
│   │   ├── HomeMapScreen.tsx      — full-bleed map placeholder + BottomStationCard
│   │   ├── StationListScreen.tsx  — FlatList of StationCard, pull-to-refresh
│   │   ├── StationDetailScreen.tsx — ScrollView + Charger/Review FlatLists
│   │   ├── SearchScreen.tsx       — TextInput + FilterPills + results
│   │   ├── FavoritesScreen.tsx    — FlatList of favorited stations
│   │   ├── ProfileScreen.tsx      — static form fields with mock data
│   │   └── LoginRegisterScreen.tsx — full-screen login/register form
│   ├── mocks/
│   │   ├── stations.ts            — 15 stations (same shape as web)
│   │   ├── chargers.ts            — 2-4 chargers per station
│   │   ├── reviews.ts             — 3-5 reviews per station
│   │   └── users.ts               — mock driver user
│   ├── i18n/
│   │   ├── ar.json                — Arabic translations
│   │   ├── fr.json                — French translations
│   │   └── index.ts               — i18next config with RN backend
│   ├── hooks/
│   │   ├── useStations.ts         — returns all mock stations
│   │   ├── useFavorites.ts        — favorite station IDs with toggle
│   │   └── useMockFilter.ts       — debounced search + filter logic
│   ├── navigation/
│   │   ├── RootNavigator.tsx      — bottom tabs + stack navigator
│   │   └── types.ts               — navigation param types
│   ├── types/
│   │   └── index.ts               — Station, Charger, Review, DriverUser, FilterState
│   ├── App.tsx                    — entry point, providers, navigation container
│   └── index.css                  — (minimal, Expo handles CSS)
├── app.json                       — Expo config
├── babel.config.js
├── tsconfig.json
├── package.json
├── metro.config.js
└── .gitignore

# UI Package (dependency, already exists from Sprint 1.1)
packages/ui/
├── src/
│   ├── tokens/
│   │   ├── colors.ts
│   │   ├── typography.ts
│   │   ├── spacing.ts
│   │   ├── radius.ts
│   │   ├── shadows.ts
│   │   ├── index.ts              — web re-exports
│   │   └── native.ts             — RN-compatible re-exports
│   ├── components/               — shared web components
│   └── index.ts
└── tailwind.config.base.js
```

**Structure Decision**: Standard Expo + React Native project structure. Screens are top-level route components in `screens/`. Mobile-specific components in `components/`. Mock data in `mocks/`. Navigation config in `navigation/`. i18n translations in `i18n/`. Hooks in `hooks/`. This mirrors the web app pattern from Sprint 1.2 but adapted for React Native conventions (no CSS files per component, StyleSheet.create for styling, etc.).

## Complexity Tracking

N/A — No constitution violations that require justification.
