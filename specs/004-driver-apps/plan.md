# Implementation Plan: Mobile & Web Driver Apps

**Feature Branch**: `004-driver-apps`

**Created**: 2026-06-12

**Spec**: `specs/004-driver-apps/spec.md`

## Overview

This document outlines the implementation plan for MVP-1 Phase 4: building mobile and web driver apps with station discovery functionality. The apps will consume the design system packages (@bornemap/tokens and @bornemap/ui) built in Phase 3.

## Scope

### In Scope

- **Mobile Driver App** (Expo SDK 54):
  - Station discovery via map and list views
  - Text search with OSM geocoding (10km radius, expand to 25km if <5 results)
  - Station detail screens with charger information
  - Navigation to stations via external mapping apps
  - Dark mode with persistent theme preference
  - Skeleton screens (no global spinners)
  - Pull-to-refresh on list and map
  - Pagination for station lists
  - Bottom sheet for station preview on map
  - Optimistic UI updates
  - Error handling with recovery actions

- **Web Driver App** (React 19 + Leaflet):
  - Responsive mobile-first design
  - Same station discovery flows as mobile app
  - Leaflet map integration
  - Dark mode via CSS variables
  - Pull-to-refresh and pagination

### Out of Scope

- User authentication or account system
- Real-time charger availability updates
- Station reservation/booking system
- Payment processing
- Offline data syncing (cache only for last 50 stations)
- Map marker clustering with complex algorithms
- Station image upload functionality
- Analytics and event tracking
- Push notifications

## Architecture

### Project Structure

```
source/front/
├── mobile-driver/          ← Expo SDK 54 app
│   ├── app/               ← expo-router pages
│   │   ├── _layout.tsx    ← Root layout with ThemeProvider
│   │   ├── index.tsx      ← Home/map screen
│   │   ├── stations.tsx   ← Station list screen
│   │   └── station/[id].tsx← Station detail screen
│   ├── components/        ← Reusable components
│   ├── hooks/             ← Custom React hooks
│   ├── services/          ← API and data layer
│   ├── store/             ← Zustand stores
│   ├── theme/             ← Dark mode config
│   ├── navi.ts            ← Map navigation service
│   └── package.json
│
├── web-driver/            ← React 19 web app
│   ├── src/
│   │   ├── pages/         ← Route pages
│   │   ├── components/    ← Reusable components
│   │   ├── hooks/         ← Custom hooks
│   │   ├── services/      ← API layer
│   │   ├── store/         ← Zustand stores
│   │   ├── App.tsx        ← Root component
│   │   └── main.tsx       ← Entry point
│   ├── public/            ← Static assets
│   ├── package.json
│   └── vite.config.ts     ← Vite configuration
│
└── packages/
    ├── tokens/            ← @bornemap/tokens (Phase 3)
    └── ui/                ← @bornemap/ui (Phase 3)
```

### Technology Stack

**Mobile Driver App:**
- Framework: Expo SDK 54 + React 18
- Navigation: expo-router v3 (file-based routing)
- State Management: Zustand
- Data Fetching: React Query (TanStack Query)
- Map: react-native-maps (open-source, no API key)
- Animations: reanimated v3
- Storage: AsyncStorage
- Styling: NativeWind (Tailwind for React Native)
- UI Components: @bornemap/ui

**Web Driver App:**
- Framework: React 19
- Build Tool: Vite
- Map: Leaflet
- State Management: Zustand
- Data Fetching: React Query
- Styling: @bornemap/ui + CSS variables
- Theme: @bornemap/tokens (CSS variables)

### API Integration

Both apps will consume the existing driver-service API endpoints:

- `GET /api/v1/stations` - Paginated station list
- `GET /api/v1/stations/{id}` - Station details
- `GET /api/v1/stations/nearby?lat={lat}&lng={lng}&radius={km}` - Nearby stations
- `GET /api/v1/health` - Health check

**Geocoding**: Client-side OSM Nominatim API calls
- Endpoint: `https://nominatim.openstreetmap.org/search`
- Parameters: `q={query}&format=json`
- Timeout: 10s, 2 retries, linear backoff
- User-Agent: Must include app identifier for rate limit compliance

### State Management

**Zustand Stores:**

1. **useThemeStore** - Dark mode toggle, persistence
   ```typescript
   interface ThemeState {
     isDarkMode: boolean
     toggleTheme: () => void
     loadTheme: () => void
   }
   ```

2. **useStationStore** - Station data, filters, selection
   ```typescript
   interface StationState {
     stations: Station[]
     selectedStation: Station | null
     searchQuery: string
     setStations: (stations: Station[]) => void
     setSelectedStation: (station: Station | null) => void
     setSearchQuery: (query: string) => void
   }
   ```

3. **useMapStore** - Map view state, markers, cluster info
   ```typescript
   interface MapState {
     userLocation: { lat: number; lng: number } | null
     center: { lat: number; lng: number }
     zoom: number
     markers: Marker[]
     updateMarkers: (stations: Station[]) => void
   }
   ```

### Data Flow

```
User Action → Component Handler → Service/API Call → React Query Cache → UI Update
         ↓
    Error Handler → Show Error UI + Retry
         ↓
    Optimistic Update → Commit Changes
```

## Implementation Phases

### Phase 1: Project Setup (2 days)

**Mobile Driver App:**
- [ ] Initialize Expo project with TypeScript
- [ ] Configure pnpm workspace dependencies
- [ ] Set up expo-router v3 file-based routing
- [ ] Install dependencies (Zustand, React Query, reanimated v3, AsyncStorage, react-native-maps)
- [ ] Configure TypeScript with strict mode
- [ ] Set up ESLint and Prettier configurations
- [ ] Create app directory structure (`app/`, `components/`, `services/`, `store/`, `theme/`)
- [ ] Configure NativeWind (Tailwind) for styling
- [ ] Set up App.tsx root component with ThemeProvider
- [ ] Test build process (pnpm build)

**Web Driver App:**
- [ ] Initialize Vite + React project
- [ ] Configure pnpm workspace dependencies
- [ ] Install dependencies (Zustand, React Query, Leaflet, @bornemap/ui)
- [ ] Configure TypeScript with strict mode
- [ ] Set up ESLint and Prettier configurations
- [ ] Create source directory structure
- [ ] Configure CSS variables from tokens package
- [ ] Set up main.tsx entry point with App.tsx
- [ ] Test build process (pnpm build)

**Deliverables:**
- Both apps compile with zero errors
- TypeScript strict mode typechecking passing
- ESLint and Prettier configurations working

---

### Phase 2: Core Navigation & Routing (2 days)

**Mobile Driver App:**
- [ ] Create root layout with ThemeProvider and dark mode initialization
- [ ] Implement navigation structure:
  - `/` - Map screen (default)
  - `/stations` - Station list screen
  - `/stations/[id]` - Station detail screen
- [ ] Add pull-to-refresh handler to map and list screens
- [ ] Configure reanimated for screen transitions
- [ ] Implement bottom sheet component for map preview
- [ ] Test navigation between screens
- [ ] Verify transitions are smooth

**Web Driver App:**
- [ ] Create App.tsx root component with theme initialization
- [ ] Configure Leaflet map initialization
- [ ] Implement page routing with React Router (or server-side rendering)
- [ ] Create responsive layouts for mobile and desktop
- [ ] Test navigation and transitions

**Deliverables:**
- Navigation structure complete
- Pull-to-refresh functional on all data-fetching screens
- Smooth transitions between screens
- Bottom sheet working on mobile

---

### Phase 3: Map Integration (3 days)

**Mobile Driver App:**
- [ ] Initialize react-native-maps MapView
- [ ] Implement geolocation permission handling
- [ ] Create MapScreen component
- [ ] Add markers for stations within search radius
- [ ] Implement map cluster badges (simplified: show count badge for dense areas)
- [ ] Add station preview bottom sheet when marker tapped
- [ ] Implement map interaction handlers (pan, zoom)
- [ ] Handle map state in Zustand store
- [ ] Add pull-to-refresh to map
- [ ] Test performance with 1000+ markers
- [ ] Verify no marker flickering or jitter

**Web Driver App:**
- [ ] Initialize Leaflet map in MapScreen
- [ ] Add OpenStreetMap tile layer
- [ ] Add markers for stations within search radius
- [ ] Add station preview modal when marker tapped
- [ ] Implement responsive map sizing
- [ ] Add pull-to-refresh to map
- [ ] Test performance with 1000+ markers
- [ ] Verify no marker flickering

**Deliverables:**
- Map rendering working on both platforms
- Markers show correct coordinates
- Pull-to-refresh functional
- Performance acceptable (60fps, no jitter)

---

### Phase 4: Station List & Search (3 days)

**Mobile Driver App:**
- [ ] Create StationListScreen component
- [ ] Implement pagination (page, per_page parameters)
- [ ] Add pull-to-refresh functionality
- [ ] Implement search bar with debouncing (300ms)
- [ ] Connect search to OSM Nominatim geocoding API
- [ ] Display search results with distance information
- [ ] Handle empty search results (show empty state)
- [ ] Show loading skeletons while fetching
- [ ] Add haptic feedback on search button
- [ ] Test search performance (<500ms target)
- [ ] Test edge cases (invalid input, network error)

**Web Driver App:**
- [ ] Create StationList component
- [ ] Implement pagination controls
- [ ] Add pull-to-refresh functionality
- [ ] Implement search input with debouncing
- [ ] Connect search to OSM Nominatim API
- [ ] Display results with distance
- [ ] Handle empty results
- [ ] Test responsive design on different screen sizes

**Deliverables:**
- Station list with pagination working
- Search returns results within 500ms
- Empty states display correctly
- Skeletons shown during loading
- Pull-to-refresh functional

---

### Phase 5: Station Details (3 days)

**Mobile Driver App:**
- [ ] Create StationDetailScreen component
- [ ] Display station name, address, opening hours, amenities
- [ ] Show charger information (type, connector count, availability)
- [ ] Display pricing information (if available)
- [ ] Add navigation button to external mapping app
- [ ] Add map button to show station location on map
- [ ] Load station images lazily (only when screen visible)
- [ ] Handle errors with contextual error UI
- [ ] Add pull-to-refresh
- [ ] Test all UI elements render correctly

**Web Driver App:**
- [ ] Create StationDetail page
- [ ] Implement same UI as mobile (responsive)
- [ ] Add navigation and map buttons
- [ ] Test on different screen sizes

**Deliverables:**
- Station detail screens complete on both platforms
- Charger information displays correctly
- Navigation button functional with error recovery
- Images load when visible (if available)
- All edge cases handled

---

### Phase 6: Offline Support & Persistence (2 days)

**Mobile Driver App:**
- [ ] Implement AsyncStorage for theme persistence
- [ ] Create offline cache for last 50 stations
- [ ] Save recently viewed stations to cache
- [ ] Load cached stations when offline
- [ ] Show cached data with offline indicator
- [ ] Update cache when network available
- [ ] Test offline scenarios (network down, slow network)
- [ ] Verify cache invalidation

**Web Driver App:**
- [ ] Implement localStorage for theme persistence
- [ ] Create offline cache (same logic as mobile)
- [ ] Save recently viewed stations
- [ ] Load cached data when offline
- [ ] Test offline scenarios

**Deliverables:**
- Theme preference persists across app restarts
- Last 50 stations cached
- Offline mode works correctly
- Cache refreshes when network returns

---

### Phase 7: Error Handling & UX (2 days)

**Mobile Driver App:**
- [ ] Create ErrorBoundary component using @bornemap/ui
- [ ] Add error screens for:
  - Network errors (with retry button)
  - Geocoding failures (with fallback)
  - API errors (with retry button)
  - Invalid data errors
- [ ] Implement error messages with copy-to-clipboard for addresses
- [ ] Add haptic feedback on all primary actions
- [ ] Test error recovery paths
- [ ] Verify all error screens use skeleton/empty states
- [ ] Test with network errors, invalid responses

**Web Driver App:**
- [ ] Create error components
- [ ] Implement same error handling patterns
- [ ] Test responsive error screens

**Deliverables:**
- ErrorBoundary catches runtime errors
- All error paths have recovery actions
- Error messages are clear and actionable
- No blank error screens

---

### Phase 8: Theme Implementation (2 days)

**Mobile Driver App:**
- [ ] Verify ThemeProvider works with @bornemap/ui
- [ ] Test dark mode toggle functionality
- [ ] Verify theme persistence (AsyncStorage)
- [ ] Test theme transitions (smooth 300ms)
- [ ] Verify all screens render correctly in both themes
- [ ] Check WCAG AA contrast ratios
- [ ] Test with real devices (iOS and Android)
- [ ] Handle system theme changes

**Web Driver App:**
- [ ] Verify theme implementation with @bornemap/ui
- [ ] Test dark mode toggle functionality
- [ ] Verify theme persistence (localStorage)
- [ ] Test theme transitions
- [ ] Verify all screens render correctly in both themes
- [ ] Check contrast ratios
- [ ] Test responsive design in both themes

**Deliverables:**
- Dark mode works perfectly on all screens
- Theme preference persists
- Smooth theme transitions
- WCAG AA contrast thresholds met

---

### Phase 9: Testing & Quality Assurance (3 days)

**Unit Tests:**
- [ ] Create test files for mobile components
- [ ] Create test files for web components
- [ ] Test Zustand stores (theme, station, map)
- [ ] Test React Query hooks
- [ ] Test service layer (API calls, geocoding)
- [ ] Test error handlers
- [ ] Achieve 80%+ coverage on critical paths

**Integration Tests:**
- [ ] Test station list loading
- [ ] Test search functionality
- [ ] Test navigation flows
- [ ] Test station detail page
- [ ] Test pagination
- [ ] Test pull-to-refresh
- [ ] Test offline scenarios

**Manual Testing:**
- [ ] Test on iOS device (primary: iPhone 13)
- [ ] Test on Android device (primary: Samsung Galaxy)
- [ ] Test on different screen sizes (mobile, tablet, desktop)
- [ ] Test pull-to-refresh gestures
- [ ] Test bottom sheet gestures (swipe to dismiss)
- [ ] Test dark mode on all screens
- [ ] Test haptic feedback on primary actions
- [ ] Test map interactions (pan, zoom, marker tap)
- [ ] Test error recovery flows
- [ ] Test navigation to external apps

**Performance Testing:**
- [ ] Measure first screen load time (<3s target)
- [ ] Measure station list fetch time (<200ms target)
- [ ] Measure search query time (<500ms target)
- [ ] Measure station detail load time (<200ms target)
- [ ] Test with 1000+ stations on map (no jitter)
- [ ] Measure bundle size (target: <5MB for mobile, <200KB for web)
- [ ] Profile memory usage

**Deliverables:**
- Test suite passing
- Manual testing checklist passed
- Performance targets met
- No crashes on real devices
- All critical paths tested

---

### Phase 10: Documentation & Deployment Prep (1 day)

**Documentation:**
- [ ] Create README for mobile-driver app
- [ ] Create README for web-driver app
- [ ] Document build commands
- [ ] Document setup instructions
- [ ] Document API integration
- [ ] Document configuration (env variables)

**Deployment Prep:**
- [ ] Create .env.example for both apps
- [ ] Configure CI/CD workflows (if applicable)
- [ ] Set up build scripts for production
- [ ] Test production builds
- [ ] Verify deployment to staging (if applicable)

**Deliverables:**
- READMEs complete
- Build scripts working
- Production builds tested

---

## Dependencies

### Internal Dependencies
- **Phase 3 Design System**: @bornemap/tokens and @bornemap/ui packages must be available and buildable
- **Phase 2 Backend Services**: Driver-service API must be accessible and all endpoints must work
- **Phase 1 Database**: Platform and analytics databases must be running with migrations applied

### External Dependencies
- **OSM Nominatim API**: Public geocoding API (no authentication required for MVP)
- **react-native-maps**: Open-source mapping library
- **Leaflet**: Open-source mapping library for web
- **Expo**: Mobile development framework
- **Vite**: Build tool for web app

### Blocked By
- No items blocked (all dependencies available from Phase 1-3)

## Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Map performance with 1000+ markers | Medium | High | Implement marker clustering; use virtualization if needed; test early |
| OSM Nominatim API rate limiting | Low | Medium | Add proper User-Agent header; implement request batching; use caching |
| React Native Maps issues on specific devices | Low | Medium | Test on multiple devices; use latest SDK; report issues to maintainers |
| Cross-platform styling consistency | Medium | Medium | Use NativeWind (Tailwind) for mobile; CSS variables for web; @bornemap/ui components |
| Offline cache size limits | Low | Low | Limit to 50 stations; compress data; implement cache eviction strategy |

## Success Metrics

### Performance
- First screen load time < 3 seconds
- Station list fetch time < 200ms (p95)
- Station search time < 500ms (p95)
- Station detail load time < 200ms (p95)
- Map with 1000+ markers renders smoothly (60fps, no jitter)
- Bundle size: Mobile < 5MB, Web < 200KB

### Quality
- 80%+ unit test coverage on critical paths
- 100% manual testing checklist passed
- No crashes on real iOS and Android devices
- WCAG AA contrast ratios met on all screens
- Dark mode transitions < 300ms

### UX
- 100% skeleton screens (no spinners)
- Haptic feedback on all primary actions
- Pull-to-refresh works on list and map
- Error messages provide recovery actions
- Navigation between screens is smooth
- Bottom sheet dismisses with swipe gestures

## Timeline Estimate

- **Phase 1 (Setup)**: 2 days
- **Phase 2 (Navigation)**: 2 days
- **Phase 3 (Map)**: 3 days
- **Phase 4 (List & Search)**: 3 days
- **Phase 5 (Details)**: 3 days
- **Phase 6 (Offline)**: 2 days
- **Phase 7 (Error Handling)**: 2 days
- **Phase 8 (Theme)**: 2 days
- **Phase 9 (Testing)**: 3 days
- **Phase 10 (Documentation)**: 1 day

**Total**: 23 days (≈3.5 weeks)

## Next Steps

1. Execute `/speckit.tasks` to generate detailed task breakdown
2. Prioritize tasks by dependency and risk
3. Begin implementation following the phased approach
4. Conduct regular code reviews and testing
5. Validate against success metrics throughout development

## Resources

- **Design System Reference**: `design-system/bornemap/MASTER.md`
- **Mobile App Reference**: `docs/mvp/mvp-1-discovery-core.md` (Phase 4 section)
- **API Contracts**: Already validated in Phase 2 (`specs/002-backend-services/spec.md`)
- **Component Reference**: `specs/003-design-system-components/spec.md`
