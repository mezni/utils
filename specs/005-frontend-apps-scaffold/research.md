# Research: Frontend Apps Scaffold

> Phase 0 output. Resolves all unknowns from the Technical Context and documents technology decisions.

## 1. Technology Choices

### 1.1 Web Map Library — Leaflet + react-leaflet

**Decision**: Leaflet via react-leaflet v4

**Rationale**:
- ADR-014 already ratified Leaflet + OpenStreetMap for web maps
- react-leaflet provides idiomatic React bindings (MapContainer, TileLayer, Marker, Popup)
- OpenStreetMap tiles are free and constitution-mandated
- No paid tile provider needed at this scale

**Alternatives considered**:
- Mapbox GL JS — requires API key, paid beyond free tier, violates ADR-014
- Google Maps — requires API key, violates ADR-014

### 1.2 Vite Proxy Configuration

**Decision**: Configure Vite server proxy to forward `/api/v1` to `http://localhost:3001`

**Rationale**:
- Eliminates CORS issues during development
- No changes needed when deploying (production uses Traefik routing)
- Simple configuration in `vite.config.ts` using the `server.proxy` option

**Reference**:
```ts
// vite.config.ts
server: {
  proxy: {
    '/api/v1': 'http://localhost:3001'
  }
}
```

### 1.3 Mobile Map Library — react-native-maps 1.18

**Decision**: react-native-maps 1.18 with Expo SDK 54

**Rationale**:
- Constitution mandates react-native-maps (ADR-012, non-negotiable rule)
- Expo SDK 54 supports react-native-maps 1.18 natively via `expo install`
- Provides platform-native MapView (Apple Maps on iOS, Google Maps on Android)

**Compatibility**: Confirmed compatible with Expo SDK 54 (react-native-maps 1.18.x).

### 1.4 Location Permission Handling — expo-location

**Decision**: Use `expo-location` `requestForegroundPermissionsAsync` with graceful fallback

**Rationale**:
- expo-location 18 is already in package.json dependencies
- Pattern: request permission → if granted, use `getCurrentPositionAsync` → if denied, set default coords (36.8065, 10.1815 — Tunis)
- No `Location.watchPositionAsync` needed in Sprint 1.5 (static location only)

### 1.5 Dashboard Navigation Sidebar

**Decision**: Custom React component with `react-router-dom` NavLink

**Rationale**:
- No need for an external sidebar library (e.g., `react-pro-sidebar`)
- `NavLink` provides built-in `isActive` prop for styling active items
- Only 4 nav items — custom component is simpler and more maintainable
- Active state colors: `#EAF0E6` background, `#007943` text (hardcoded per user requirement, tracked for token extraction)

### 1.6 Dashboard Stat Cards

**Decision**: Simple card components rendering hardcoded or API-fetched stats

**Rationale**:
- No charting library needed for basic count display (recharts deferred)
- Overview stat cards show counts (total partners, stations, chargers) fetched from admin-service API
- Visual: three cards in a horizontal row with icon, label, and count

## 2. API Contracts

### 2.1 Driver-Service API

**`GET /api/v1/stations/nearby?lat={lat}&lng={lng}&radius_km={radius}`**

Response format (confirmed from Sprint 1.3 integration tests):
```json
{
  "stations": [
    {
      "id": "STN-abc123",
      "name": "Station Name",
      "latitude": 36.8188,
      "longitude": 10.1657,
      "address": " Tunis",
      "available_chargers": 3,
      "total_chargers": 5
    }
  ]
}
```

### 2.2 Admin-Service API

**`GET /api/v1/partners`** — List all partners
**`GET /api/v1/stations`** — List all stations
**`GET /api/v1/chargers`** — List all chargers

Response format (from Sprint 1.4 data model):
```json
// GET /api/v1/partners
[
  { "id": "PRT-abc", "name": "Partner Name", "created_at": "..." }
]

// GET /api/v1/stations
[
  { "id": "STN-abc", "name": "Station", "partner_id": "PRT-abc", "latitude": 36.8, "longitude": 10.1 }
]

// GET /api/v1/chargers
[
  { "id": "CHG-abc", "station_id": "STN-abc", "status": "available" }
]
```

## 3. Edge Case Handling

### 3.1 API Unavailable (all apps)
- **Decision**: Show error state with retry option
- Map still renders with default center
- Station markers area shows "Unable to load stations" message

### 3.2 Location Permission Denied (mobile)
- **Decision**: Default to Tunis coordinates (36.8065, 10.1815)
- No further prompts during session
- User can manually pan the map

### 3.3 Invalid Route (dashboard)
- **Decision**: Redirect to Overview page using react-router-dom's Navigate component

## 4. Design Token Strategy

**Decision**: Color values hardcoded in Sprint 1.5. Token extraction is deferred to Sprint 1.6 hardening.

**Rationale**:
- packages/ui has no design token definitions yet
- Constitution principle VIII requires tokens, but Sprint 1.5 timeline doesn't allow full token system design
- ADR or Sprint 1.6 task will extract these into a shared token set

## 5. CI Workflow Impact

- **driver-web**: Add Tailwind CSS, Leaflet, and react-leaflet to dependencies. CI build must succeed.
- **driver-mobile**: Must scaffold Expo project structure. CI runs `npx tsc --noEmit`.
- **dashboard**: Add Tailwind CSS to dependencies. CI build must succeed.

No changes to CI workflow YAML files — existing workflows run `npm run build` (web) and `npm run tsc` (mobile) which will pick up the new code.
