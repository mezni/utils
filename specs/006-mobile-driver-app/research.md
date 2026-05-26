# Research: Mobile Driver App — Map Discovery

**Date**: 2026-05-26 | **Plan**: [plan.md](./plan.md)

## Decisions

### Decision 1: Nearby endpoint authentication

**Decision**: Make `/api/v1/stations/nearby` publicly accessible by removing JWT requirement from the infrastructure module. This is read-only data (station locations and statuses) already visible on the admin map.

**Rationale**: Requiring authentication creates a login barrier for drivers who want quick discovery. The nearby endpoint exposes no sensitive data — it returns station coordinates and charger counts that are already publicly available on the admin BaseMap.

**Alternatives considered**:
- Anonymous JWT (rejected: adds complexity with no security benefit for read-only public data)
- Full registration (rejected: blocks the core use case — frictionless discovery)

### Decision 2: Map library choice

**Decision**: Use `react-native-maps` (built-in Expo SDK component) with `PROVIDER_DEFAULT` (Apple Maps on iOS, Google Maps on Android).

**Rationale**: `react-native-maps` is included in Expo SDK 51 and provides native map rendering on both platforms without additional configuration. It supports markers, clustering, and region-based callbacks.

**Alternatives considered**:
- `react-native-leaflet` (rejected: WebView-based, poor performance)
- `mapbox-gl` (rejected: requires API key and native module, violates managed Expo constraint)

### Decision 3: Marker clustering approach

**Decision**: Use `react-native-maps` built-in clustering via `<Marker>` with `clustered` prop or the `@react-native-map/clustering` library if the built-in option is insufficient.

**Rationale**: The spec requires clustering only when >20 results. For MVP0, simple clustering without custom cluster styles is acceptable.

**Alternatives considered**:
- Always show individual markers (rejected: map clutter at 50 markers on small mobile screens)
- Custom clustering algorithm (rejected: reinventing the wheel)

### Decision 4: Station detail data source

**Decision**: Use the nearby API response which already includes station details (name, address, coordinates) and available charger count. For per-charger details, make individual `GET /api/v1/stations/{id}/chargers` calls when the bottom sheet opens.

**Rationale**: The nearby endpoint returns a list summary. Charger-level detail requires a second call, which is acceptable for a bottom sheet that opens on tap.

**Alternatives considered**:
- Embed charger details in nearby response (rejected: would increase payload size for all 50 stations when driver only taps one)
- Pre-fetch all charger details (rejected: wasteful network usage)
