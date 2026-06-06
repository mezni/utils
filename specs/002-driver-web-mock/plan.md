# Implementation Plan: Driver Web App with Mock Data

**Branch**: `002-driver-web-mock` | **Date**: 2026-06-05 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-driver-web-mock/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command.

## Summary

Scaffold and populate the Driver Web App (`apps/driver-web`) with all 6 public screens, 9 driver-specific components, and realistic mock data (15 stations, 2-4 chargers each, 3-5 reviews each with Arabic/French content). The app uses Vite + React + TypeScript with Tailwind extending `packages/ui` tokens, React Router for navigation, and react-i18next for Arabic/French i18n with RTL layout. No backend calls — all data is local mock files.

## Technical Context

**Language/Version**: TypeScript 5.x with strict mode, React 18+

**Primary Dependencies**: Vite 5, React Router 6, react-i18next, i18next, Tailwind CSS 3, `packages/ui` (workspace dependency), Vitest, @testing-library/react, @testing-library/jest-dom, @testing-library/user-event

**Storage**: N/A — all data from local mock TypeScript files

**Testing**: Vitest + @testing-library/react (unit + component tests), @testing-library/jest-dom (DOM matchers), @testing-library/user-event (interaction tests)

**Target Platform**: Web browser (modern Chrome, Firefox, Safari)

**Project Type**: Vite + React SPA (frontend only, no backend)

**Performance Goals**: Bundle build <30s, dev server startup <5s, component renders <50ms

**Constraints**:
- No backend calls — all data from local mock files
- Map is a placeholder (#EAF0E6 background with positioned div markers) — no real map library
- All visual values from `packages/ui` design tokens via Tailwind config extension
- WCAG 2.1 AA accessibility compliance on all screens
- Arabic RTL must work correctly on every screen
- All static strings translated in ar.json and fr.json
- Social login buttons are visual-only (no OAuth)
- Profile form is static (no submission or validation)
- Favorites are mock data only (no persistence)

**Scale/Scope**: 6 screens, 9 driver-specific components, ~15 stations mock data, ~50 chargers, ~60 reviews

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Principle I: Pragmatic Architecture
✅ **PASS** — Single Vite + React application with minimal structure. No services, no databases. Architecture is minimal for the sprint's scope.

### Principle II: Single Source of Truth
✅ **PASS** — Visual values from `packages/ui` tokens (Sprint 1.1). Mock data is placeholder only, to be replaced by real API calls in Phase 5.

### Principle III: Simple Operations
✅ **PASS** — Standard Vite dev workflow (`pnpm dev`). No complex operations.

### Principle IV: Domain Separation by Schema
N/A — No database schemas involved.

### Principle V: Build for Current Scale
✅ **PASS** — Mock data approach is the simplest approach for UI development. No premature optimization.

### Principle VI: Public Access First
✅ **PASS** — Home/Map, Station Detail, and Search Results are accessible without login. Authentication is only needed for Favorites (P2) and Profile (P3) — these are clearly identified as authentication-gated features per the spec.

### Principle VII: RTL & Arabic Built-In
✅ **PASS** — Arabic RTL required on every screen. Translations in `ar.json` and `fr.json`. RTL verification is a success criterion.

### Principle VIII: Visual Consistency
✅ **PASS** — Tailwind config extends `packages/ui/tailwind.config.base.js`. All visual values from design tokens. No hardcoded visual values.

### Non-Negotiable Rules
- ✅ **inventory.station** — N/A (mock data, no database)
- ✅ **Public access** — Home/Map and Station Detail are public
- ✅ **Tokens not stored** — N/A (no auth tokens in this sprint)
- ✅ **Arabic RTL** — Required on every screen
- ✅ **Only Traefik** — N/A (frontend only)
- ✅ **Keycloak owns auth** — N/A (mock auth screens only)
- ✅ **No additional services without ADR** — No services added
- ✅ **Cross-schema access** — N/A

**Overall Result**: ✅ **ALL GATES PASSED**

## Project Structure

### Documentation (this feature)

```text
specs/002-driver-web-mock/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
# Driver Web Application
apps/driver-web/
├── src/
│   ├── components/
│   │   ├── MobileTopBar.tsx
│   │   ├── SearchBar.tsx
│   │   ├── FilterPills.tsx
│   │   ├── MapPinMarker.tsx
│   │   ├── ZoomControls.tsx
│   │   ├── StationCard.tsx
│   │   ├── ChargerRow.tsx
│   │   ├── ReviewCard.tsx
│   │   └── BottomStationCard.tsx
│   ├── screens/
│   │   ├── HomeMapScreen.tsx
│   │   ├── StationDetailScreen.tsx
│   │   ├── SearchResultsScreen.tsx
│   │   ├── FavoritesScreen.tsx
│   │   ├── ProfileScreen.tsx
│   │   └── LoginRegisterScreen.tsx
│   ├── mocks/
│   │   ├── stations.ts
│   │   ├── chargers.ts
│   │   ├── reviews.ts
│   │   └── users.ts
│   ├── i18n/
│   │   ├── ar.json
│   │   ├── fr.json
│   │   └── index.ts
│   ├── hooks/
│   │   ├── useStations.ts
│   │   ├── useFavorites.ts
│   │   └── useMockFilter.ts
│   ├── types/
│   │   └── index.ts
│   ├── App.tsx
│   ├── main.tsx
│   └── index.css
├── public/
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
├── tailwind.config.ts
└── postcss.config.js

# UI Package (dependency, already exists from Sprint 1.1)
packages/ui/
├── src/
│   ├── tokens/       # Design tokens (colors, typography, spacing, radius, shadows)
│   ├── components/   # Shared components (Button, Input, Badge, StatusBadge, etc.)
│   └── index.ts
└── tailwind.config.base.js
```

**Structure Decision**: Standard Vite + React SPA structure. Screens are top-level route components in `screens/`. Driver-specific components in `components/`. Mock data in `mocks/`. i18n translations in `i18n/`. This mirrors the typical React project layout and keeps the app boundary clearly separated from the shared `packages/ui` package.

## Complexity Tracking

N/A — No constitution violations that require justification.
