# Research: Mobile & Web Driver Apps

**Feature**: MVP-1 Phase 4 - Mobile & Web Driver Apps
**Branch**: `004-driver-apps`
**Date**: 2026-06-12

## Technical Context Decisions

### 1. Map Provider Strategy (react-native-maps vs react-native-webview for Web)

**Decision**: Use react-native-maps for mobile + react-native-webview for web

**Rationale**:
- react-native-maps is open-source, free, and doesn't require API keys (compliance with constitution's public API usage)
- Supports both iOS and Android natively with excellent performance
- react-native-webview embeds Leaflet, which provides web browser-standard map experience
- Consistent UX across mobile and web (both show OpenStreetMap tiles)
- Alignment with user stories (web users get same experience as mobile)

**Alternatives Considered**:
- Mapbox SDK: Requires API key and payment tier, creates vendor lock-in
- Google Maps SDK: Requires API key and costs money, violates "free and open-source preferred" principle

**Best Practices**:
- Use default OpenStreetMap tiles (no authentication needed)
- Set map viewport constraints to prevent map overscroll on mobile
- Handle map permissions gracefully on Android (Request runtime permissions)

---

### 2. Map Marker Performance Optimization (1000+ Stations)

**Decision**: Use react-native-maps native markers with optimized rendering

**Rationale**:
- react-native-maps uses native components for markers, providing best performance
- Native markers render at 60fps, outperforming custom React-based markers
- Simplified clustering: show count badges for dense areas (50m radius)
- Debounce marker updates during pan/zoom to prevent unnecessary re-renders
- Use `react-native-maps`'s built-in performance features (native markers, region updates)

**Alternatives Considered**:
- Custom SVG markers: Poor performance, many re-renders
- Complex clustering libraries (react-native-maps-clustering): Adds complexity, may not fit MVP scope
- Force render every frame: Terrible performance, causes jitter

**Best Practices**:
- Update markers only when visible region changes (react-native-maps' `onRegionChangeComplete`)
- Use `react-native-maps`'s `coordinate` prop with native gesture handling
- Debounce marker updates by 100-200ms during pan/zoom
- Test on devices with weaker GPUs (older Android, iPhone 12) to ensure smoothness

---

### 3. Geocoding API Strategy (OSM Nominatim Rate Limits)

**Decision**: Exponential backoff with 3 retries (10s, 30s, 60s) + user-facing error

**Rationale**:
- OSM Nominatim has generous free tier (50 requests/minute for legitimate use)
- Exponential backoff prevents API overload and increases success rate
- User-friendly error message explains rate limit and provides recovery
- Aligns with UX-first principle (no raw error strings)

**Alternatives Considered**:
- Ignore rate limits: High risk of IP ban, poor user experience
- No retries: Too many failed requests, users get frustrated
- Linear backoff (10s, 20s, 30s): Less effective than exponential

**Best Practices**:
- Include app identifier in User-Agent header (compliance with OSM ToS)
- Cache geocoding results for common queries (Tunis Central, locations within Tunisia)
- Show loading state during retry attempts
- Limit concurrent geocoding requests to 1 at a time

---

### 4. State Management Architecture (Zustand vs Redux vs Context API)

**Decision**: Zustand with 3 stores (Theme, Station, Map)

**Rationale**:
- Zustand is lightweight (~1KB), no boilerplate, simple API
- Perfect for UI state (theme, selections, map view) - not complex data flow
- React Query handles data fetching, Zustand handles UI state
- Less boilerplate than Redux, no provider nesting like Context API
- Easy to persist (localStorage for web, AsyncStorage for mobile)

**Alternatives Considered**:
- Redux Toolkit: Overkill for UI state, steep learning curve
- Context API alone: Re-renders entire component tree on any state change
- Apollo Client: Overkill, adds unnecessary complexity

**Best Practices**:
- Separate stores by domain (Theme, Station, Map) to avoid circular dependencies
- Use Zustand's immer for immutable updates (prevents bugs)
- Persist theme store only (user preferences)
- Keep stores simple - avoid business logic in stores (delegate to services)

---

### 5. Offline Caching Strategy (50 Stations Limit)

**Decision**: React Query cache + AsyncStorage for recently viewed stations

**Rationale**:
- React Query's built-in cache handles network failure scenarios gracefully
- Cache last 50 stations + details for offline reading
- Mark cache as stale when network returns, fetch fresh data
- Simple to implement, no custom cache logic needed

**Alternatives Considered**:
- Custom cache: Too much boilerplate, risk of bugs
- No caching: Poor offline experience, poor UX
- Cache all stations: Too large, conflicts with 50km search radius assumption

**Best Practices**:
- React Query's stale-while-revalidate strategy: Show cached data while refreshing
- Limit cache size to 50 stations (enough for common use cases)
- Save recently viewed stations to AsyncStorage for persistence across app restarts
- Show offline indicator when using cached data

---

### 6. Navigation Strategy (expo-router vs React Router)

**Decision**: expo-router v3 for mobile, React Router for web

**Rationale**:
- expo-router provides file-based routing out of the box, perfect for Expo
- Native navigation with gestures, bottom sheets, transitions
- No external dependencies needed (built into Expo SDK 54)
- Easy to implement deep links and nested routes

**Alternatives Considered**:
- React Navigation v6: More complex setup, requires navigation tree
- React Router: Not native-first, poor mobile experience

**Best Practices**:
- Use nested routes in expo-router for station detail page (`/stations/[id]`)
- Configure navigation structure in `app/_layout.tsx` for global state
- Use reanimated for screen transitions (exhaustion of "route transitions via expo-router only")

---

### 7. Map Marker Clustering Approach

**Decision**: Simplified clustering - show count badges for dense areas (50m radius)

**Rationale**:
- Complex clustering algorithms add significant complexity
- Badge count provides immediate feedback without user confusion
- Sufficient for MVP - users can always zoom in to see individual markers
- No vendor lock-in, works with any marker provider

**Alternatives Considered**:
- Full clustering (Leaflet clustering plugin): Heavy, overkill for MVP
- No clustering: Poor performance with 1000+ markers
- Auto-zoom to cluster: Confusing UX, unexpected behavior

**Best Practices**:
- Show badge with count when multiple markers within 50m radius
- Badge color indicates status (green=available, red=in_use, yellow=maintenance)
- Badge positioned near marker center
- Badge hidden when user zooms in past clustering threshold

---

### 8. Skeleton Screen Strategy

**Decision**: Per-screen skeleton components (never global spinner)

**Rationale**:
- Skeleton screens show users exactly what will load (station list skeleton, detail skeleton)
- Aligns with "skeleton screens over spinners — everywhere, no exceptions" principle
- Improves perceived performance (user knows content is loading)
- React Query's skeleton mode: `enabled: true` with fallback to Skeleton component

**Alternatives Considered**:
- Global spinner: Poor UX, no feedback on what's loading
- Progressive loading: Too complex for MVP
- No skeletons: Feels slow, users confused

**Best Practices**:
- Create skeleton components for each screen type (StationList, StationDetail, Map)
- Skeletons should match exact layout of real components
- Use animation (pulse or shimmer) to indicate loading state
- Show skeleton immediately on screen enter, not on first data fetch

---

### 9. Image Loading Strategy

**Decision**: Lazy load station images only when station detail is visible

**Rationale**:
- Images can be large, loading all at once hurts performance
- Users only see detail when they tap a station - delay is acceptable
- Reduces initial bundle size, improves first screen load time

**Alternatives Considered**:
- Load all images immediately: Poor performance, wasted bandwidth
- Load images on demand (when visible): Same as lazy load
- Preload images: Too complex, no significant benefit

**Best Practices**:
- Use `expo-image` with lazy loading (reanimated v3 for transitions)
- Show placeholder image while loading (station logo or generic placeholder)
- Lazy load only when detail screen enters view (use expo-router' `useFocusEffect`)
- Resize images on server to max 600px width (reduce bandwidth)

---

### 10. Performance Targets (Latency & Bundle Size)

**Decision**: Define and measure against strict targets (3s load, 200ms fetch, 5MB mobile, 200KB web)

**Rationale**:
- Performance is a primary KPI (constitution: "Map interaction latency is a primary KPI")
- Targets are achievable with optimized code, caching, and bundle analysis
- Metrics enable early detection of performance regressions

**Alternatives Considered**:
- No targets: Performance becomes "good enough", leads to technical debt
- Conservative targets (5s load): Too slow, poor UX
- Aggressive targets (1s load): Unachievable with MVP constraints

**Best Practices**:
- Measure p95 latency (90% of requests under target)
- Bundle size targets: Mobile <5MB (reasonable for mobile app store), Web <200KB (gzip)
- Use bundle analyzer to identify large dependencies
- Test on real devices (iOS 13+, Android 10+)

---

## Integration Decisions

### react-native-maps Configuration

- **Provider**: Default OpenStreetMap tiles (no authentication)
- **Custom Marker**: Use native markers (better performance)
- **Location Permissions**: Request at runtime on Android, prompt once on iOS
- **Clustering**: Simplified badges (50m radius)

### React Query Configuration

- **Cache Duration**: 5 minutes (fresh enough for cache, stale enough to refresh)
- **Stale-While-Revalidate**: Enabled (show cached while fetching)
- **Retry**: 2 retries with exponential backoff (network failure)
- **Enabled**: Always enabled (allow manual refresh)

### OSM Nominatim API Configuration

- **User-Agent**: `BorneMap/1.0 (contact@bornemap.com)`
- **Timeout**: 10s
- **Retries**: 3 with exponential backoff (10s, 30s, 60s)
- **Rate Limit Handling**: Show user-friendly error, implement backoff

### Theme Persistence Strategy

- **Web**: localStorage (synced via useSyncExternalStore)
- **Mobile**: AsyncStorage (via @react-native-async-storage/async-storage)
- **Default**: System preference (light/dark mode based on OS settings)
- **Persistence**: Saved on toggle, loaded on app launch

---

## Best Practices Summary

### Performance
- Skeleton screens over spinners (every screen)
- Lazy load images (only when visible)
- Native markers (react-native-maps)
- Debounce marker updates during pan/zoom
- React Query's stale-while-revalidate

### UX
- Haptic feedback on all primary CTAs
- Pull-to-refresh on list and map
- Bottom sheet with swipe gestures
- Optimistic UI updates
- User-friendly error messages with recovery actions

### Testing
- Unit tests for stores, services, hooks (80%+ coverage)
- Integration tests for critical paths (discovery, search, detail)
- Manual testing on iOS and Android devices
- Performance profiling with 1000+ stations

### Security & Privacy
- No PII in logs (sanitize addresses before logging)
- No authentication in MVP (public-access web app)
- Secure storage for theme preference
- HTTPS for all API calls

---

## Conclusion

All technology decisions align with project principles:
- **UX-First**: Skeleton screens, haptic feedback, optimistic UI
- **Domain-Driven**: No external dependencies outside bounded contexts
- **Test-First**: Unit tests for critical paths, 80%+ coverage target
- **Source-Rooted**: Runtime code under `source/front/`, no mixing with docs/configs
- **Immutable Data**: No write operations to geospatial data (GIS schema READ-ONLY)

All technical choices are pragmatic, maintainable, and aligned with MVP scope.
