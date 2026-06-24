# Sprint 03 — Task Breakdown

**Status**: TASKS DEFINED
**Date**: 2026-06-24

---

## Task 1: Scaffold Monorepo Workspace

| Field | Value |
|-------|-------|
| **Input** | Architecture spec (3 packages + 1 app) |
| **Output** | Root package.json, pnpm-workspace.yaml, tsconfig base |
| **Module** | `/source/` |
| **Validation** | `pnpm install` succeeds |
| **Security** | No env vars, no secrets |
| **Tests** | — |

**Steps:**
- Create `/source/package.json` with pnpm workspace config
- Create `/source/pnpm-workspace.yaml` listing `packages/*` and `apps/*`
- Create `/source/tsconfig.base.json` with strict TypeScript config

---

## Task 2: Create `domain-types` Package

| Field | Value |
|-------|-------|
| **Input** | Rust `NearbyStationResponse` DTO, Sprint 02 API contract |
| **Output** | `/source/packages/domain-types/` with station DTOs and Zod schemas |
| **Module** | `packages/domain-types` |
| **Validation** | `tsc` compiles, Zod validates sample payloads |
| **Security** | Zod schema enforces shape of untrusted API responses |
| **Tests** | Schema accepts valid payload, rejects invalid |

**Files:**
- `package.json` (name: `@bornemap/domain-types`, dep: `zod`)
- `tsconfig.json`
- `src/index.ts` (re-exports)
- `src/station.ts` (`StationDto`, `NearbyResponse`, `StationSchema`, `NearbyResponseSchema`)
- `src/__tests__/station.test.ts`

---

## Task 3: Create `client-core` Package

| Field | Value |
|-------|-------|
| **Input** | `domain-types` types, driver-service API URL |
| **Output** | `/source/packages/client-core/` with API client + React hook |
| **Module** | `packages/client-core` |
| **Validation** | `tsc` compiles, tests with mocked fetch |
| **Security** | API response validated through Zod schema |
| **Tests** | fetchNearbyStations calls correct URL, validates response, handles 4xx/5xx |

**Files:**
- `package.json` (name: `@bornemap/client-core`, dep: `@bornemap/domain-types`, `zod`)
- `tsconfig.json`
- `src/index.ts`
- `src/stationApi.ts` (`fetchNearbyStations`, `useNearbyStations` hook)
- `src/__tests__/stationApi.test.ts`

---

## Task 4: Create `ui-kit` Package

| Field | Value |
|-------|-------|
| **Input** | Design requirements from spec (map, spinner, banner, empty state) |
| **Output** | `/source/packages/ui-kit/` with reusable components |
| **Module** | `packages/ui-kit` |
| **Validation** | `tsc` compiles, components render in tests |
| **Security** | No dangerouslySetInnerHTML, no inline event handlers |
| **Tests** | Each component renders, responds to props |

**Components:**
- `MapProvider.tsx` — Leaflet map wrapper with viewport callback
- `StationMarkerLayer.tsx` — Marker + cluster layer
- `LoadingSpinner.tsx` — Loading indicator
- `ErrorBanner.tsx` — Error display with retry button
- `EmptyState.tsx` — Empty results message

**Files:**
- `package.json` (name: `@bornemap/ui-kit`, deps: `leaflet`, `react-leaflet`, `react-leaflet-cluster`)
- `tsconfig.json`
- `src/index.ts`
- `src/map/MapProvider.tsx` + `MapProvider.module.css`
- `src/map/StationMarkerLayer.tsx` + `StationMarkerLayer.module.css`
- `src/map/clusterConfig.ts`
- `src/feedback/LoadingSpinner.tsx` + `LoadingSpinner.module.css`
- `src/feedback/ErrorBanner.tsx` + `ErrorBanner.module.css`
- `src/feedback/EmptyState.tsx` + `EmptyState.module.css`
- `src/__tests__/MapProvider.test.tsx`
- `src/__tests__/StationMarkerLayer.test.tsx`
- `src/__tests__/LoadingSpinner.test.tsx`
- `src/__tests__/ErrorBanner.test.tsx`
- `src/__tests__/EmptyState.test.tsx`

---

## Task 5: Create `web-driver` App Shell

| Field | Value |
|-------|-------|
| **Input** | Vite + React + TypeScript scaffold |
| **Output** | `/source/apps/web-driver/` with App.tsx, main.tsx, MapPage.tsx |
| **Module** | `apps/web-driver` |
| **Validation** | `pnpm dev` starts on :5173, blank page loads |
| **Security** | — |
| **Tests** | App renders without crash |

**Files:**
- `package.json` (name: `web-driver`, deps: `@bornemap/ui-kit`, `@bornemap/client-core`, `@bornemap/domain-types`)
- `tsconfig.json`
- `vite.config.ts`
- `index.html`
- `src/main.tsx`
- `src/App.tsx`
- `src/pages/MapPage.tsx` + `MapPage.module.css`

---

## Task 6: Implement MapPage with State Management

| Field | Value |
|-------|-------|
| **Input** | ui-kit components, client-core hooks |
| **Output** | Full MapPage with loading/error/empty/success states |
| **Module** | `apps/web-driver/src/pages/` |
| **Validation** | All 4 states render correctly |
| **Security** | API response validated through client-core → domain-types Zod |
| **Tests** | Each state renders expected UI elements |

**Implementation:**
- `MapPage` uses `useStationsNearViewport` hook
- Maps states to ui-kit components:
  - Loading → `<LoadingSpinner />`
  - Error → `<ErrorBanner message={...} onRetry={refetch} />`
  - Empty → `<EmptyState />`
  - Success → `<StationMarkerLayer stations={stations} />` inside `<MapProvider>`
- API_BASE_URL from `import.meta.env.VITE_API_BASE_URL` (default `http://localhost:3001`)

---

## Task 7: Implement Viewport Debounce & Clustering

| Field | Value |
|-------|-------|
| **Input** | useStationsNearViewport hook, clusterConfig |
| **Output** | Debounced re-fetch on map drag/zoom, clustered markers |
| **Module** | `apps/web-driver/src/hooks/`, `packages/ui-kit/src/map/` |
| **Validation** | Console log shows debounced calls, clusters appear at low zoom |
| **Security** | — |
| **Tests** | Debounce delays API call, clustering renders for >10 stations |

**Implementation:**
- `useStationsNearViewport.ts`:
  - State: `center`, `zoom`, `stations`, `isLoading`, `error`
  - `onViewportChange`: updates center/zoom, calls debounced fetch
  - Debounce: 300ms via `setTimeout`/`clearTimeout`
  - Fetch: calls `fetchNearbyStations` from client-core
- `clusterConfig.ts`:
  - `maxClusterRadius: 50`, `spiderfyOnMaxZoom: true`, `disableClusteringAtZoom: 10`

---

## Task 8: Write Tests

| Field | Value |
|-------|-------|
| **Input** | All source files |
| **Output** | Test files for all packages + app |
| **Validation** | `pnpm test` passes across workspace |
| **Security** | — |
| **Tests** | See Testing Strategy in plan.md |

---

## Task 9: Validation & Final Checks

| Field | Value |
|-------|-------|
| **Input** | All implemented code |
| **Output** | `pnpm check` (tsc) passes, `pnpm test` passes, `pnpm lint` passes |
| **Module** | All packages + app |
| **Validation** | All scripts pass |
| **Tests** | — |

---

## Task 10: Delivery Artifacts

| Field | Value |
|-------|-------|
| **Input** | Sprint 03 deliverables |
| **Output** | SYSTEM_STATE.md, sprint_state.json, validation_report.md, sprint_review.md, follow_up.md |
| **Module** | `docs/speckit/sprints/sprint-03/` |
| **Validation** | All 9 artifacts exist |
| **Tests** | — |
