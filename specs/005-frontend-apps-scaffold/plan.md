# Implementation Plan: Frontend Apps Scaffold

**Branch**: `005-frontend-apps-scaffold` (currently on `004-admin-service`) | **Date**: 2026-06-07 | **Spec**: `specs/005-frontend-apps-scaffold/spec.md`

**Input**: Feature specification from `/specs/005-frontend-apps-scaffold/spec.md`

## Summary

Scaffold three frontend applications — Driver Web (Vite+React+Leaflet), Driver Mobile (Expo+react-native-maps), Dashboard (Vite+React+AppShell) — with map displays, station markers from the real driver-service API, location handling, and sidebar navigation. No authentication in this sprint.

## Technical Context

**Language/Version**: TypeScript 5.6+ (all three apps), JavaScript (ESM modules)

**Primary Dependencies**:
- Driver Web: React 18.3, Vite 5.4, react-router-dom 6.28, Leaflet, react-leaflet, Tailwind CSS v3
- Driver Mobile: Expo SDK 54, react-native-maps 1.18, expo-location 18, expo-router 4
- Dashboard: React 18.3, Vite 5.4, react-router-dom 6.28, Tailwind CSS v3, recharts (for Overview stat cards)

**Storage**: N/A (all data fetched from live APIs)

**Testing**: vitest (web apps), No dedicated test framework for mobile in this sprint

**Target Platform**: 
- Driver Web: Modern browsers (Chrome, Firefox, Safari, Edge)
- Driver Mobile: iOS 15+, Android 8+ (via Expo SDK 54)
- Dashboard: Modern browsers

**Project Type**: Web application (Driver Web, Dashboard) + Mobile application (Driver Mobile)

**Performance Goals**: 
- Map renders in <3s on broadband
- Dashboard nav responds in <100ms

**Constraints**: 
- Constitution compliance (no auth, no localStorage tokens, OpenStreetMap tiles only, Expo SDK 54 locked)
- Vite proxy needed for Driver Web API calls to avoid CORS issues
- Location permission denial must not crash the mobile app
- Dashboard active nav must use #EAF0E6 bg / #007943 text

**Scale/Scope**: Three frontend apps, 9 tasks (TASK-43 through TASK-51), 2-week sprint

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Check | Status |
|-----------|-------|--------|
| I. Pragmatic Architecture | Three apps are justified by distinct surfaces (web driver, mobile driver, admin dashboard). No new service introduced. | ✅ PASS |
| VII. English & French | UI text in English. French strings deferred (not required for scaffold). | ⚠️ Note |
| VIII. Visual Consistency | Design tokens not yet defined in packages/ui. Hardcoded colors exist (sidebar active color specified in CSS). | ⚠️ Needs ADR or justification |
| IX. API Prefix Consistency | Apps consume /api/v1/* endpoints — compliant | ✅ PASS |
| VI. Public Access | Driver apps must work without auth — compliant | ✅ PASS |
| X. No Auth in Sprint 1.5 | Explicitly deferred — compliant | ✅ PASS |

**Violations requiring justification**:
- **VIII**: Dashboard active nav color (#EAF0E6/#007943) is hardcoded per user requirement. Until packages/ui provides design tokens, this is accepted as interim. Create ADR or add token extraction to Sprint 1.6 backlog.

## Project Structure

### Documentation (this feature)

```text
specs/005-frontend-apps-scaffold/
├── spec.md              # This file (/speckit.specify output)
├── plan.md              # This file (/speckit.plan output)
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
apps/
├── driver-web/                      # Vite + React + Tailwind + Leaflet
│   ├── src/
│   │   ├── components/
│   │   │   └── StationMap.tsx       # Leaflet map with markers
│   │   ├── hooks/
│   │   │   └── useStations.ts       # API fetch hook
│   │   ├── services/
│   │   │   └── api.ts              # API client (fetch wrapper)
│   │   ├── App.tsx                  # Root component
│   │   └── main.tsx                 # Entry point
│   ├── index.html
│   ├── vite.config.ts               # + proxy config for /api/v1
│   ├── tailwind.config.js
│   └── postcss.config.js
│
├── driver-mobile/                   # Expo SDK 54 + react-native-maps
│   ├── app/
│   │   └── index.tsx               # Map screen (expo-router file-based)
│   ├── components/
│   │   └── StationMarker.tsx        # Marker callout component
│   ├── hooks/
│   │   └── useLocation.ts           # Location permission + fallback
│   ├── services/
│   │   └── api.ts                  # API client (fetch wrapper)
│   ├── app.json                     # Expo config
│   └── package.json                 # + expo-location, react-native-maps
│
├── dashboard/                       # Vite + React + Tailwind + AppShell
│   ├── src/
│   │   ├── components/
│   │   │   ├── AppShell.tsx         # Layout with sidebar + content area
│   │   │   └── Sidebar.tsx          # Left sidebar navigation
│   │   ├── pages/
│   │   │   ├── OverviewPage.tsx     # Stat cards overview
│   │   │   ├── PartnersPage.tsx     # Placeholder
│   │   │   ├── StationsPage.tsx     # Placeholder
│   │   │   └── ChargersPage.tsx     # Placeholder
│   │   ├── App.tsx                  # Router + AppShell wrapper
│   │   └── main.tsx                 # Entry point
│   ├── index.html
│   ├── vite.config.ts
│   ├── tailwind.config.js
│   └── postcss.config.js

packages/
├── ui/                              # Design token foundation (placeholder in Sprint 1.5)
│   └── package.json
├── api-client-driver/               # Shared driver API client (existing, enhanced)
│   └── ...
└── api-client-admin/                # Shared admin API client (existing, enhanced)
    └── ...
```

**Structure Decision**: Three independent frontend apps under `apps/`, organized by user-facing surface. Shared code lives in `packages/`. No monorepo build orchestration — each app builds independently.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| VIII — Hardcoded colors in Dashboard | User-specified exact colors (#EAF0E6/#007943) for active nav. Token extraction blocked by packages/ui being placeholder. | Deferred token extraction to Sprint 1.6 hardening post-ADR. |
