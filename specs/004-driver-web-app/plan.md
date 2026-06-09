# Implementation Plan: Driver Web App

**Branch**: `004-driver-web-app` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/004-driver-web-app/spec.md`

## Summary

Build a standalone Driver Web App — a Leaflet map showing station markers from json-server with color-coded availability and navigation to station detail. Vite + React + TypeScript + Tailwind project that consumes the existing API and shared design tokens.

## Technical Context

**Language/Version**: TypeScript 5.7 (same as Dashboard)

**Primary Dependencies**: React 18, React Router 6+, Leaflet + react-leaflet, Tailwind CSS

**Storage**: N/A — data from json-server at `http://localhost:3001/api/*`

**Testing**: Manual verification against json-server (no test framework)

**Target Platform**: Web browser — modern Chrome, Firefox, Safari (desktop + mobile)

**Project Type**: New Vite + React SPA — 2 screens (Map, Station Detail) with shared components

**Performance Goals**: Map loads station markers within 3 seconds on broadband; marker colors reflect real-time charger status

**Constraints**: All API calls use `/api` prefix via Vite proxy; all visual values from shared design tokens; partner visibility rule computed client-side; no geolocation; no search/filter/list view

**Scale/Scope**: 1 new app, 2 screens, 3 new components (StationCard, ChargerRow, ZoomControls)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| # | Principle | Check | Result |
|---|-----------|-------|--------|
| 1 | MVP-first delivery | New standalone app — minimal scope (map + detail only) | PASS |
| 2 | Layered complexity | Driver Web is read-only, no CRUD, no auth — simplest possible | PASS |
| 3 | Dashboard first | Dashboard was Sprint 1.2/1.3 — Driver Web is the third product | PASS |
| 4 | Single source of truth | json-server remains sole data source | PASS |
| 5 | Simple operations | Add `pnpm dev:web` script | PASS |
| 9 | Visual consistency | All tokens from shared design system | PASS |
| 10 | API prefix consistency | All calls use `/api` prefix via Vite proxy | PASS |
| — | Partner visibility | Client-side filter — full server-side enforcement in MVP-2 | PASS |
| — | No authentication | No auth in MVP-1 — driver app is public | PASS |

**Gate verdict**: ALL PASS — proceed to Phase 0

## Project Structure

### Documentation (this feature)

```text
specs/004-driver-web-app/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (separate command)
```

### Source Code (repository root)

```text
source/apps/driver-web/
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
├── tailwind.config.js
├── src/
│   ├── main.tsx
│   ├── App.tsx
│   ├── api/
│   │   └── client.ts
│   ├── components/
│   │   ├── StationCard.tsx
│   │   ├── ChargerRow.tsx
│   │   └── ZoomControls.tsx
│   └── pages/
│       ├── MapPage.tsx
│       └── StationDetailPage.tsx
```

**Structure Decision**: New Vite project under `source/apps/driver-web/`. Follows the same conventions as the Dashboard app (same tech stack, same token base). Two page components for the two routes.

## Complexity Tracking

No constitution violations — complexity tracking not required.

---

## Phase 0: Research

### Unknowns & Research Tasks

| # | Unknown | Research Task |
|---|---------|---------------|
| R01 | Leaflet + React + Vite integration | How to set up react-leaflet with Vite, handle tile layers, markers, popups |
| R02 | Partner visibility filtering | How to fetch partners, stations, chargers and compute visible stations client-side |
| R03 | Marker color logic | How to compute available_count per station and assign green/red marker fill |
| R04 | Map position persistence | How to preserve map center/zoom when navigating to Station Detail and back |

---

## Phase 1: Research Output

### R01 — Leaflet + React + Vite Integration

**Decision**: Use `react-leaflet` with Leaflet CSS imported in main.tsx. Configure Vite to handle Leaflet's marker icon path issue via `import.meta.url` or a custom fix.

**Rationale**: react-leaflet is the standard React wrapper. Leaflet CSS must be imported globally. The marker icon issue (broken default icon path in bundled apps) is a known Vite problem with a standard workaround.

**Alternatives considered**: Vanilla Leaflet in React (loses component model), MapLibre GL (heavier, over-engineered for MVP-1).

### R02 — Partner Visibility Filtering

**Decision**: Fetch all partners, stations, and chargers on mount. Build a set of visible partner IDs (where is_verified && is_live && is_active). Filter stations to those belonging to visible partners.

**Rationale**: json-server supports fetching all resources without pagination. The data set is small (< 20 stations). Client-side filtering is simple and avoids needing server-side join logic.

**Alternatives considered**: Query stations first, then batch-filter by partner (two round trips), server-side middleware (out of scope for MVP-1).

### R03 — Marker Color Logic

**Decision**: Compute `available_count` per station by counting chargers with status === 'available'. Green marker if available_count > 0, red marker if 0.

**Rationale**: Simple boolean classification. Matches the FR-005/FR-006 requirements. Green means "some working chargers", red means "none working".

**Alternatives considered**: Three-state colors (green/yellow/red for high/medium/low), numeric labels in the marker (cluttered on the map).

### R04 — Map Position Persistence

**Decision**: Store the map center (lat, lng) and zoom level in React state within the map page component. Use React Router's state or a simple context to pass initial position from Station Detail back to Map.

**Rationale**: The map page owns the position state. Navigating to detail and back should restore position. React Router's `location.state` is the simplest mechanism.

**Alternatives considered**: URL query params (pollutes URL for every pan), Redux/context (over-engineered for a single value), localStorage (unnecessary persistence).
