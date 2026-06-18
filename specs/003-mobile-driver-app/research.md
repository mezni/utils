# Research: Web Driver Client

**Branch**: `004-web-driver-client` | **Date**: 2026-06-18 | **Spec**: [`spec.md`](../004-web-driver-client/spec.md)

## Decisions

### API Base URL Configuration
- **Decision**: Configurable `API_BASE_URL` via environment variable (`VITE_API_BASE_URL`)
- **Rationale**: Web applications can use `localhost` directly. Each developer sets their local driver-service port (3001). Vite's environment variable system keeps it cleanly out of source code.
- **Alternatives considered**: Hardcoded fallback chain (fragile, fails silently), ngrok tunnel (adds latency and external dependency during validation)

### Location Privacy in localStorage
- **Decision**: Round viewport coordinates to 2 decimal places before caching; station data cached as-is
- **Rationale**: 2 decimal places (~1.1km precision) prevents precise location recovery while still useful for cache key matching. Station data contains no personal information.
- **Alternatives considered**: Raw coordinate storage (privacy risk), secure session storage (browser memory, not persistent enough for offline)

### Crash Reporting
- **Decision**: No crash reporting in v1; rely on browser console and error boundaries during validation
- **Rationale**: Validation phase with developer-only testing; browser dev tools provide full error traces. Defer Sentry or similar to a future production-hardening sprint.
- **Alternatives considered**: Sentry integration (unnecessary overhead for validation), custom localStorage error log (low value during active development)

### Map Component Strategy
- **Decision**: Use Leaflet for web map rendering; separate from mobile (react-native-maps)
- **Rationale**: Leaflet uses DOM-based mapping (tiles, markers, popups) while mobile uses native OS map views. A shared abstraction would either break Leaflet's DOM rendering or collapse to the lowest-common-denominator API.
- **Alternatives considered**: Shared map wrapper via SVG abstraction (complex marker handling), WebGL-based map (overengineered for validation phase)

### Offline Cache Strategy
- **Decision**: Cache-on-read pattern — write to localStorage on every successful API response; read on network failure
- **Rationale**: Simplest cache strategy with no TTL complexity during validation. Cache overwrites on each successful fetch, so it always reflects the most recent successful viewport.
- **Alternatives considered**: TTL-based cache (unnecessary complexity for validation), LRU cache (overengineered for <50 stations per viewport)

### Pull-to-Refresh vs Auto-Refresh
- **Decision**: Manual refresh button only; no auto-refresh on scroll or timer
- **Rationale**: Debounced pan/zoom + manual refresh is the simplest correct behavior. Auto-refresh on scroll would amplify API calls; periodic refresh would drain battery and unnecessary for static station data.
- **Alternatives considered**: Auto-refresh on scroll (excessive API calls), periodic 30s polling (battery drain, unnecessary for static station data)

### State Management
- **Decision**: React built-in hooks only (useState, useEffect, useRef, useCallback) + @tanstack/react-query for API calls
- **Rationale**: The app has a single data dependency (nearby stations API). React Query handles caching, refetch, loading/error states out of the box. No global store needed.
- **Alternatives considered**: Zustand (overkill for single data source), Redux Toolkit (heavy for validation phase), Context API (React Query already provides global cache)

### Styling Strategy
- **Decision**: Use Tailwind CSS via CDN for web driver client
- **Rationale**: Enforces consistent design system across web apps, requires no build step during development, and provides responsive utilities for mobile-friendly layout.
- **Alternatives considered**: CSS modules (more boilerplate), SCSS with preprocessor (adds complexity), custom CSS files (loses design system benefits)
