# Research: Driver Web App

## R01 — Leaflet + React + Vite Integration

**Decision**: Use `react-leaflet` with Leaflet CSS imported in main.tsx. Fix the default marker icon issue by copying icon assets or using the standard URL override.

**Rationale**: react-leaflet is the standard React wrapper. The marker icon issue (broken default icon path in Vite bundled apps) is well-documented with a standard fix — either importing icon PNGs directly or using `L.icon()` with explicit URL.

**Alternatives considered**: Vanilla Leaflet (loses component model, harder to maintain), MapLibre GL (heavier, over-engineered for MVP-1), Google Maps (proprietary, requires API key).

## R02 — Partner Visibility Filtering

**Decision**: Fetch all partners, stations, and chargers on Map mount. Build a set of visible partner IDs (where is_verified && is_live && is_active). Filter stations to those belonging to visible partners.

**Rationale**: json-server supports fetching all resources. Data set is small (< 20 stations, 3 partners, < 30 chargers). Client-side filtering is O(n) and avoids extra round trips.

**Alternatives considered**: Two-step query (stations then partner check — more round trips), server-side filter (out of scope for MVP-1).

## R03 — Marker Color Logic

**Decision**: Compute available_count per station by counting chargers with status === 'available'. Green fill if available_count > 0, red fill if 0.

**Rationale**: Mirrors the spec's FR-005/FR-006 exactly. Simple boolean classification that drivers can understand at a glance.

**Alternatives considered**: Three colors (green/yellow/red) — adds complexity without clear user benefit for MVP-1.

## R04 — Map Position Persistence

**Decision**: Store map center and zoom in React component state. Pass via React Router's location state when navigating to Station Detail. On return, the Map page reads the location state to restore position.

**Rationale**: React Router location.state is available via `useLocation()` and persists through forward/back navigation naturally. No extra state management needed.

**Alternatives considered**: URL query params for lat/lng/zoom (pollutes URL), context/context (extra boilerplate), localStorage (unnecessary for tab-level state).
