# Sprint 03 — Web Driver Map UI (Tunisia Map + EV Stations Visualization)

**Status**: SPEC WRITTEN (Phase 0)
**Date**: 2026-06-24
**Constitution Version**: v1.15.2

---

## Scope Lock (HARD CONSTRAINT)

| Domain | Included | Excluded |
|--------|----------|----------|
| **Frontend** | ✅ `web-driver` app at `/source/apps/web-driver/` | ❌ Any other apps |
| **Packages** | ✅ `ui-kit`, `domain-types`, `client-core` prerequisites | ❌ New packages beyond the 3 defined ones |
| **Map Engine** | ✅ Leaflet (free, no API key) | ❌ Mapbox, Google Maps |
| **Data Source** | ✅ `GET /api/v1/stations/nearby` from driver-service | ❌ Any other data source |
| **Backend** | ❌ None | ❌ Changes to driver-service |
| **DB** | ❌ None | ❌ Schema changes |
| **Auth** | ❌ None | ❌ Auth redesign or implementation |

---

## System Behavior

### Objective

Production-grade map-based UI allowing users to:
1. View Tunisia map as default viewport
2. Visualize EV charging stations as markers
3. Fetch nearby stations dynamically from driver-service
4. Interact with map markers (hover → tooltip, click → detail panel)
5. Handle all UI states deterministically

### Core Feature — Map Rendering

| Requirement | Spec |
|-------------|------|
| Default viewport | Tunisia center (lat: 34.0, lon: 9.5, zoom: 6) |
| Zoom | Standard Leaflet zoom (1–18) |
| Pan | Free pan, no bounds restriction |
| Clustering | MarkerClusterGroup at zoom < 10 |
| Marker detail | Popup on click with station_id, name, distance_km |
| Hover tooltip | Tooltip on hover with station name |

### Station Markers

Each marker displays from `NearbyStationResponse`:
- `station_id` (visible in detail popup)
- `name` (visible in tooltip + popup)
- `lat`, `lon` (marker position)
- `distance_km` (visible in popup)

### Data Source

```
GET /api/v1/stations/nearby?lat={lat}&lon={lon}&radius={radius}&limit={limit}
```

Base URL: `http://localhost:3001` (configurable via env)

### User Flow

1. User opens web app
2. Map loads centered on Tunisia with loading indicator
3. System fetches nearby stations (default: Tunisia center, 50km, limit 50)
4. On success → markers rendered with clustering
5. On error → error state with retry
6. Empty results → "No stations found" message
7. User clicks marker → popup with station details
8. User drags map → 300ms debounce → re-fetch nearby stations for new viewport center
9. API in flight → loading indicator overlay

---

## UX/UI PRO MAX Discipline (STRICT)

### UX Principles

| Principle | Implementation |
|-----------|----------------|
| Immediate visual feedback | Map tiles load immediately; skeleton/spinner during API |
| No blank map states | Fallback message rendered if tiles fail |
| Clear loading indicators | Spinner overlay during API fetch |
| Graceful API failure | Error banner with retry button |
| "No stations" state | Informational overlay on empty results |

### UI Rules

| Rule | Enforcement |
|------|-------------|
| ui-kit only | All UI components from `/source/packages/ui-kit/` |
| Consistent spacing | Design token spacing scale |
| Reusable components | Map/station components in `map/` directory |
| No inline styling | All styles via ui-kit or CSS modules |
| Responsive | Mobile-first, full-width on desktop |

### Interaction Rules

| Interaction | Behavior |
|-------------|----------|
| Marker hover | Tooltip with station name (0.2s delay) |
| Marker click | Popup with full station details |
| Map drag | 300ms debounce → re-fetch from new center |
| Zoom change | Re-fetch after zoom stabilized (500ms) |

### State Rules (Every Surface)

| State | Visual |
|-------|--------|
| Loading | Spinner overlay + "Loading stations..." text |
| Success | Map with markers rendered |
| Error | Error banner + "Retry" button |
| Empty | "No charging stations found in this area" message |

---

## Architecture (Frontend Only)

### Location

```
/source/apps/web-driver/
```

### Structure

```
web-driver/
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
├── src/
│   ├── main.tsx
│   ├── App.tsx
│   ├── components/
│   │   └── ... (app-specific components)
│   ├── pages/
│   │   ├── MapPage.tsx
│   │   └── MapPage.css
│   ├── services/
│   │   └── stationService.ts (thin wrapper calling client-core)
│   ├── hooks/
│   │   └── useStationsNearViewport.ts
│   ├── state/
│   │   └── stationStore.ts
│   └── map/
│       ├── MapProvider.tsx
│       ├── StationMarkerLayer.tsx
│       └── clusterConfig.ts
```

### Integration Layers

```
web-driver
  └── imports from client-core (API calls)
  └── imports from domain-types (DTOs)
  └── imports from ui-kit (UI components)

client-core
  └── imports from domain-types
  └── no UI imports
  └── API client for driver-service

domain-types
  └── pure types only
  └── zero dependencies

ui-kit
  └── zero dependencies
  └── MapProvider, StationMarkerLayer, LoadingSpinner, ErrorBanner, EmptyState
```

### Package Dependency Chain

```
ui-kit  (UI components, design tokens)
   ↓
domain-types  (DTOs, API contracts, entity types)
   ↓
client-core  (API client, React Query hooks)
   ↓
web-driver  (application shell, pages)
```

---

## Map Engine Requirements

| Requirement | Implementation |
|-------------|----------------|
| Marker rendering | Leaflet `L.marker` via React-Leaflet |
| Clustering | Leaflet.markercluster via react-leaflet-cluster |
| Viewport-based updates | `useMapEvents` → debounced callback |
| Performance | Clustering at zoom < 10, max 100 markers per fetch |
| Tile layer | OpenStreetMap (free, no API key) |

---

## Security Rules

| Rule | Implementation |
|------|----------------|
| Never trust API payload | Zod schema validation on API response |
| Validate station schema | Parse `NearbyStationResponse[]` through Zod |
| Sanitize UI rendering | React default escaping (no dangerouslySetInnerHTML) |
| Prevent XSS in metadata | All string fields rendered as React text nodes |

---

## Testing Strategy

### Unit Tests

| Test | Scope |
|------|-------|
| Map component renders Tunisia viewport | `MapProvider.test.tsx` |
| StationMarkerLayer renders correct markers | `StationMarkerLayer.test.tsx` |
| Loading state shows spinner | `MapPage.test.tsx` |
| Error state shows error banner | `MapPage.test.tsx` |
| Empty state shows message | `MapPage.test.tsx` |
| DTO validation (Zod schema) | `domain-types` tests |

### Integration Tests

| Test | Scope |
|------|-------|
| API → map marker pipeline | `web-driver` |
| Error fallback behavior | `web-driver` |
| Empty dataset rendering | `web-driver` |

### UX Tests

| Test | Scope |
|------|-------|
| Loading indicator visible during fetch | `web-driver` |
| Marker click shows popup | `web-driver` |
| Hover shows tooltip | `web-driver` |

---

## Hard Stop Conditions

| Condition | Action |
|-----------|--------|
| Backend changes introduced | HALT |
| New service added | HALT |
| Map data sourced outside driver-service | HALT |
| UI bypasses client-core for API calls | HALT |
| UX states incomplete (loading/error/empty/success) | HALT |
| ui-kit not used | HALT |
| Inline styling used instead of ui-kit | HALT |
| Architecture layer violation | HALT |

---

## Implementation Flow

| Step | Description |
|------|-------------|
| STEP 1 | Branch: `sprint/03-web-driver-map` |
| STEP 2 | Scaffold monorepo (root package.json, pnpm-workspace), create `domain-types`, `client-core`, `ui-kit` packages |
| STEP 3 | Scaffold `web-driver` app with Vite + React + TypeScript |
| STEP 4 | Implement `domain-types` — Station DTO, NearbyResponse, Zod schemas |
| STEP 5 | Implement `client-core` — API client, `useNearbyStations` hook |
| STEP 6 | Implement `ui-kit` — MapProvider, StationMarkerLayer, LoadingSpinner, ErrorBanner, EmptyState |
| STEP 7 | Implement `web-driver` — MapPage, stationService, useStationsNearViewport |
| STEP 8 | Performance optimization — debounce, clustering |
| STEP 9 | Tests — unit, integration, UX |
| STEP 10 | Validation — lint, typecheck, test |
| STEP 11 | Delivery artifacts |
