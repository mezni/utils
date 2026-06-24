# Sprint 03 — Implementation Plan

**Status**: PLANNED
**Date**: 2026-06-24

---

## 1. System Architecture

### Context Diagram

```
  Browser
     │
     ├── web-driver :5173 (Vite dev server)
     │     ├── MapPage ──→ useStationsNearViewport
     │     │                    │
     │     │              [300ms debounce]
     │     │                    │
     │     │              client-core/stationApi
     │     │                    │
     │     │              HTTP GET ──→ driver-service :3001
     │     │                              └── /api/v1/stations/nearby
     │     │                                      │
     │     │                              PostgreSQL (gis.find_nearby_stations)
     │     │
     │     └── ui-kit components
     │           ├── MapProvider (Leaflet map shell)
     │           ├── StationMarkerLayer (markers + clustering)
     │           ├── LoadingSpinner
     │           ├── ErrorBanner
     │           └── EmptyState
     │
     └── OpenStreetMap tile server (CDN)
```

### Service Impact Map

| Component | Port | Impact | Reason |
|-----------|------|--------|--------|
| driver-service | 3001 | Consumed (read-only) | Data source for nearby stations |
| auth-service | 3000 | None | Not in scope |
| admin-service | 3002 | None | Not in scope |
| web-driver (new) | 5173 | Created | This sprint's target |
| ui-kit (new) | — | Created | Package dependency |
| domain-types (new) | — | Created | Package dependency |
| client-core (new) | — | Created | Package dependency |

### Dependency Graph

```
pnpm workspace root (source/)
  │
  ├── packages/ui-kit/
  │   ├── package.json  (react, react-dom, leaflet, react-leaflet)
  │   ├── tsconfig.json
  │   └── src/
  │       ├── index.ts
  │       ├── map/MapProvider.tsx
  │       ├── map/StationMarkerLayer.tsx
  │       ├── feedback/LoadingSpinner.tsx
  │       ├── feedback/ErrorBanner.tsx
  │       └── feedback/EmptyState.tsx
  │
  ├── packages/domain-types/
  │   ├── package.json  (zod)
  │   ├── tsconfig.json
  │   └── src/
  │       ├── index.ts
  │       ├── station.ts        (Station dto, NearbyResponse)
  │       └── api-contracts.ts  (NearbyParams)
  │
  ├── packages/client-core/
  │   ├── package.json  (domain-types, zod)
  │   ├── tsconfig.json
  │   └── src/
  │       ├── index.ts
  │       └── stationApi.ts     (fetchNearbyStations, useNearbyStations)
  │
  └── apps/web-driver/
      ├── package.json  (ui-kit, domain-types, client-core)
      ├── tsconfig.json
      ├── vite.config.ts
      ├── index.html
      └── src/
          ├── main.tsx
          ├── App.tsx
          ├── pages/MapPage.tsx
          ├── hooks/useStationsNearViewport.ts
          └── services/stationService.ts
```

---

## 2. Type Definitions (domain-types)

```typescript
// packages/domain-types/src/station.ts
export interface StationDto {
  station_id: string;
  name: string | null;
  lat: number;
  lon: number;
  distance_km: number;
}

export interface NearbyResponse {
  data: StationDto[];
}

export const StationSchema: z.ZodSchema<StationDto>;
export const NearbyResponseSchema: z.ZodSchema<NearbyResponse>;
```

## 3. API Client (client-core)

```typescript
// packages/client-core/src/stationApi.ts
interface NearbyParams {
  lat: number;
  lon: number;
  radius?: number;
  limit?: number;
}

async function fetchNearbyStations(
  baseUrl: string,
  params: NearbyParams
): Promise<StationDto[]>;

// React hook
function useNearbyStations(
  params: NearbyParams
): { stations: StationDto[]; isLoading: boolean; error: Error | null; refetch: () => void };
```

## 4. UI Components (ui-kit)

```typescript
// MapProvider.tsx
interface MapProviderProps {
  center: [number, number];  // [lat, lon]
  zoom: number;
  children: React.ReactNode;
  onViewportChange?: (center: [number, number], zoom: number) => void;
}

// StationMarkerLayer.tsx
interface StationMarkerLayerProps {
  stations: StationDto[];
}

// LoadingSpinner.tsx
// ErrorBanner.tsx  { message: string; onRetry?: () => void }
// EmptyState.tsx   { message?: string }
```

## 5. App Hooks & Services

```typescript
// useStationsNearViewport.ts
function useStationsNearViewport(debounceMs?: number): {
  center: [number, number];
  zoom: number;
  stations: StationDto[];
  isLoading: boolean;
  error: Error | null;
  onViewportChange: (center: [number, number], zoom: number) => void;
  refetch: () => void;
};
```

## 6. Testing Strategy

| Layer | Test | Framework |
|-------|------|-----------|
| domain-types | Schema validation (valid/invalid payloads) | Vitest |
| client-core | fetchNearbyStations parsing | Vitest + MSW (or mocked fetch) |
| ui-kit | MapProvider renders children, LoadingSpinner shows, ErrorBanner click | Vitest + @testing-library/react |
| web-driver | MapPage states (loading/error/empty/success) | Vitest + @testing-library/react |
| web-driver | Marker click shows popup | Vitest + @testing-library/react |

## 7. Risk Assessment

| Risk | Mitigation |
|------|------------|
| driver-service not running | Error state with retry, clear message |
| Map tiles slow/CDN down | Default OpenStreetMap CDN, fallback tiles on error |
| Large number of stations | Clustering at zoom < 10, max 100 limit |
| API rate limiting | 300ms debounce on viewport changes |
| CORS issues | driver-service already has tower-http CORS middleware |

## 8. Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| pnpm | 9+ | Package manager |
| Vite | 6+ | Build tool |
| React | 18+ | UI framework |
| TypeScript | 5.6+ | Type safety |
| Leaflet | 1.9+ | Map engine |
| react-leaflet | 5+ | React bindings for Leaflet |
| react-leaflet-cluster | latest | Marker clustering |
| zod | 3.23+ | Runtime schema validation |
| Vitest | 3+ | Test runner |
| @testing-library/react | 16+ | Component testing |
