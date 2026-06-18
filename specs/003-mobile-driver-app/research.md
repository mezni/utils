# Research: Mobile Driver App

**Branch**: `003-mobile-driver-app` | **Date**: 2026-06-18

## Decisions

### API Base URL Configuration
- **Decision**: Configurable `API_BASE_URL` via Expo `app.json` extras (`expo.extra.apiBaseUrl`)
- **Rationale**: Physical devices on Expo Go cannot use `localhost`. Each developer sets their machine's LAN IP where Traefik is running. Expo's `extra` field keeps it cleanly out of source code.
- **Alternatives considered**: Hardcoded fallback chain (fragile, fails silently), Ngrok tunnel (adds latency and external dependency during validation)

### Location Privacy in AsyncStorage
- **Decision**: Round viewport coordinates to 2 decimal places before caching; station data cached as-is
- **Rationale**: 2 decimal places (~1.1km precision) prevents precise location recovery while still useful for cache key matching. Station data contains no personal information.
- **Alternatives considered**: Raw coordinate storage (privacy risk), expo-secure-store (overkill for non-sensitive cached station data)

### Crash Reporting
- **Decision**: No crash reporting in v1; rely on Metro logs and device console during validation
- **Rationale**: Validation phase with developer-only testing; Metro bundler provides full error traces. Defer Sentry or similar to a future production-hardening sprint.
- **Alternatives considered**: Sentry integration (unnecessary overhead for validation), custom AsyncStorage error log (low value during active development)

### Map Component Strategy
- **Decision**: Platform-specific map views — do NOT attempt to create a shared map component between mobile and web
- **Rationale**: `react-native-maps` uses native OS map views (Apple Maps / Google Play Services) while web uses Leaflet DOM rendering. A shared abstraction would either break the Expo Go build pipeline or collapse to the lowest-common-denominator API.
- **Alternatives considered**: Shared map wrapper via `expo/vector-icons` abstraction (breaks Leaflet on web), WebView-based map (poor performance, violates platform conventions)

### Offline Cache Strategy
- **Decision**: Cache-on-read pattern — write to AsyncStorage on every successful API response; read on network failure
- **Rationale**: Simplest cache strategy with no TTL complexity during validation. Cache overwrites on each successful fetch, so it always reflects the most recent successful viewport.
- **Alternatives considered**: TTL-based cache (unnecessary complexity for validation), LRU cache (overengineered for <50 stations per viewport)

### Pull-to-Refresh vs Auto-Refresh
- **Decision**: Manual pull-to-refresh only; no auto-refresh on pan/zoom or timer
- **Rationale**: Debounced pan/zoom + pull-to-refresh is the simplest correct behavior. Auto-refresh on pan would amplify API calls; periodic refresh would drain mobile battery. Validated during Sprint 1.3 clarifications.
- **Alternatives considered**: Auto-refresh on pan (excessive API calls), periodic 30s polling (battery drain, unnecessary for static station data)

### State Management
- **Decision**: React built-in hooks only (useState, useEffect, useRef, useCallback) + @tanstack/react-query for API calls
- **Rationale**: The app has a single data dependency (nearby stations API). React Query handles caching, refetch, loading/error states out of the box. No global store needed.
- **Alternatives considered**: Zustand (overkill for single data source), Redux (heavy for validation phase), Context API (React Query already provides global cache)
