# Research: Mobile Driver App (Core UX)

**Phase**: Phase 0 | **Date**: 2026-06-11 | **Feature**: [spec.md](spec.md)

## 1. Map Library: react-native-maps

**Decision**: Use `react-native-maps` with platform-native map views (Apple Maps on iOS, Google Maps on Android).

**Rationale**: Standard choice for Expo managed workflow. Provides native map performance, built-in marker annotations, region change callbacks for nearby re-fetch, and clustering support (deferred to post-MVP). No licensing cost for Apple Maps on iOS; Google Maps on Android requires API key but is included in Expo SDK.

**Alternatives considered**:
- **Mapbox (react-native-mapbox-gl)**: Requires separate native module installation, not included in Expo SDK 54 managed workflow.
- **WebView-based maps (Leaflet/MapLibre)**: Slower, no native gesture performance, unnecessarily complex.

## 2. Routing: Expo Router

**Decision**: Use Expo Router (file-based routing) for app navigation.

**Rationale**: Expo SDK 54 ships with Expo Router as the recommended routing solution. File-based routing under `app/` directory eliminates manual navigation configuration. For MVP-1 with a single screen, the overhead is minimal and provides the foundation for multi-screen navigation in later sprints.

**Alternatives considered**:
- **react-navigation (standalone)**: More explicit navigation config, but Expo Router already wraps react-navigation. No benefit to using it directly.
- **No router**: Possible for a single-screen app, but would require migration later.

## 3. GPS / Location: expo-location

**Decision**: Use `expo-location` for GPS permission requests, current location retrieval, and region tracking.

**Rationale**: First‑party Expo module. Covers `requestForegroundPermissionsAsync`, `getCurrentPositionAsync`, and `watchPositionAsync` — all needed for MVP-1. No extra native module linking required.

**Alternatives considered**:
- **react-native-geolocation-service**: Requires native module linking, no advantage over expo-location in the managed workflow.

## 4. API Client Pattern

**Decision**: Simple fetch-based client with `useEffect` + custom hooks (no Axios, no React Query for MVP-1).

**Rationale**: MVP-1 has only 2 API calls (nearby search, station detail). Adding Axios or React Query introduces dependencies for minimal benefit. If caching, retries, or pagination become needed in later sprints, React Query can be added incrementally.

**Alternatives considered**:
- **Axios**: Adds request/response interceptors but no significant benefit for 2 endpoints.
- **React Query / TanStack Query**: Excellent for caching and retries, but overkill for 2 endpoints. Deferred to Sprint 1.6 (integration).

## 5. Clickstream Event Architecture

**Decision**: Fire-and-forget HTTP POST to Clickstream Service using `navigator.sendBeacon`-equivalent pattern (unawaited fetch with no error handling visible to the user).

**Rationale**: The MVP rule states "must never block UX." Using unawaited `fetch()` calls ensures events are best-effort without impacting map rendering, sheet animation, or navigation. Events may be lost if the service is down, but this is acceptable for MVP-1.

**Alternatives considered**:
- **Background queue (AsyncStorage)**: Retries events on failure. Adds persistence complexity. Not needed for MVP-1.
- **Batch sending**: Collects events and sends periodically. Better for battery/bandwidth but adds buffering complexity.

## 6. Error / Loading State Strategy

**Decision**: Every data-fetching hook provides `{ data, loading, error }` return type with 10-second timeout on network requests. Skeleton components from the design system render while `loading === true`. ErrorState renders when `error` is set. EmptyState renders when `data` returns empty.

**Rationale**: Matches the MVP UX rules (skeleton-first, no blank states). The 10-second timeout prevents infinite spinners. The ternary pattern (`loading ? Skeleton : error ? ErrorState : data ? Content : EmptyState`) is consistent across all screen-level components.

**Alternatives considered**:
- **Suspense / React Error Boundary**: More declarative but requires React 18+ concurrent mode configuration. MVP-1 prefers the explicit ternary pattern for clarity.

## 7. Dark Mode

**Decision**: The `@borne/design-system` ThemeProvider already handles dark mode via Appearance API. The app wraps its root layout with ThemeProvider; all components use `useTheme()` to get the active palette.

**Rationale**: Dark mode was already implemented in Sprint 1.4. The app simply consumes the existing ThemeProvider — no new dark mode work needed.

**Alternatives considered**: N/A — already solved by the design system.
