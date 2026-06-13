# Research: MVP-1 Stabilization Sprint

**Date**: 2026-06-13
**Feature**: MVP-1 Stabilization Sprint
**Reference**: [spec.md](./spec.md)

## Overview

This document consolidates research findings for performance optimization, UX polish, and production readiness improvements in the MVP-1 BorneMap application.

---

## 1. React Native Performance Optimization

### 1.1 Rendering 1000+ Map Markers

**Decision**: Implement marker clustering with react-native-maps clustering (ClusteringMarkerView)

**Rationale**:
- Clustering reduces rendering load from 1000+ individual markers to ~50 clusters
- Predefined cluster sizes (50m radius) match UI patterns
- Trade-off: Aggregated data instead of granular selection (acceptable for discovery phase)

**Alternatives Considered**:
- Custom marker rendering with manual clustering (rejected: reinvents library patterns)
- Spatial indexing without clustering (rejected: still renders too many views)

**Implementation Approach**:
```typescript
// Use react-native-maps clustering library
import ClusteringMarkerView from 'react-native-maps-clustering';
// Cluster size: 50m radius, max points: 50
<ClusteringMarkerView
  data={markers}
  radius={50}
  minClusterSize={2}
  maxSize={50}
>
  {(clusteredMarkers) => clusteredMarkers.map(marker => (
    <Marker key={marker.key} coordinate={marker.coordinate}>
      <Callout>{marker.title}</Callout>
    </Marker>
  ))}
</ClusteringMarkerView>
```

### 1.2 Preventing Marker Jitter

**Decision**: Use React.memo and useMemo for marker rendering

**Rationale**:
- Markers re-render when props change (even if only coordinate changes)
- Memoization prevents unnecessary re-renders during map pan
- Matches performance requirement of no frame drops

**Implementation Approach**:
```typescript
// Memoize marker data to prevent re-renders
const markers = useMemo(() => {
  return nearbyStations.map(station => ({
    key: station.id,
    coordinate: {
      latitude: station.geometry?.coordinates[1] || 36.8065,
      longitude: station.geometry?.coordinates[0] || 10.1815,
    },
    title: station.name,
    description: station.address,
  }));
}, [nearbyStations]);

// Memoize cluster markers
const clusteredMarkers = useMemo(() => {
  return clusterMarkers(markers, 50);
}, [markers]);
```

### 1.3 React Native Reanimated v3 Usage

**Decision**: Use reanimated v3 for all animations (skeleton screens, transitions, haptics)

**Rationale**:
- Requires by constitution (I. UX-First principle)
- Better performance than core Animated API
- Smooth 60fps animations across all platforms

**Implementation Approach**:
```typescript
import Animated, { SlideInUp } from 'react-native-reanimated';

// Animated skeleton screen
<Animated.View entering={SlideInUp.duration(300)}>
  <Skeleton />
</Animated.View>

// Animated marker clustering transition
const animatedMarkers = useAnimatedValue(0);
const animatedMarkersStyle = useAnimatedStyle(() => ({
  transform: [{ scale: animatedMarkers.value }],
}));

// Haptic feedback
import * as Haptics from 'expo-haptics';
Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Medium);
```

### 1.4 Haptic Feedback Implementation

**Decision**: Use expo-haptics for all primary CTAs

**Rationale**:
- User expects tactile feedback on important actions
- Matches UX Pro Max rules (haptic feedback on CTAs)
- Cross-platform (iOS + Android)

**Implementation Approach**:
```typescript
import * as Haptics from 'expo-haptics';

// Primary action: medium impact
Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Medium);

// Success action: light impact
Haptics.notificationAsync(Haptics.NotificationFeedbackType.Success);

// Error action: heavy impact
Haptics.notificationAsync(Haptics.NotificationFeedbackType.Error);
```

---

## 2. PostGIS Query Optimization

### 2.1 Radius Search Performance

**Decision**: Use GIST index with ST_DWithin and LIMIT/OFFSET pagination

**Rationale**:
- GIST index on location column enables fast spatial queries
- ST_DWithin ensures accurate radius search (not approximate)
- LIMIT/OFFSET pagination prevents loading excessive results

**Implementation Approach**:
```sql
-- Existing index is correct
CREATE INDEX idx_station_location_gist
  ON inventory.station USING GIST (location)
  WHERE deleted_at IS NULL;

-- Optimized query
SELECT id, name, address,
       ST_DWithin(location, ST_SetSRID(ST_MakePoint($1, $2), 4326), $3) AS nearby,
       ST_Distance(location, ST_SetSRID(ST_MakePoint($1, $2), 4326)) AS distance_km
FROM inventory.station
WHERE deleted_at IS NULL
  AND ST_DWithin(location, ST_SetSRID(ST_MakePoint($1, $2), 4326), $3)
ORDER BY distance_km
LIMIT $4 OFFSET $5;
```

### 2.2 N+1 Query Pattern Prevention

**Decision**: Eager loading for station details with pagination

**Rationale**:
- Previously found N+1 queries (review issue DS-2)
- Causes performance degradation at scale
- Eager loading reduces query count to 2 (1 list + 1 details)

**Implementation Approach**:
```rust
// Backend: Use joins to fetch all needed data in one query
async fn get_stations_paginated(
    pool: &PgPool,
    page: u32,
    per_page: u32,
) -> Result<Vec<Station>> {
    let offset = (page - 1) * per_page;
    sqlx::query_as!(
        Station,
        r#"
        SELECT id, name, address, lat, lng, status, opening_hours, partner_id,
               charger_count, available_count, total_count
        FROM inventory.station
        WHERE deleted_at IS NULL
        ORDER BY id
        LIMIT $1 OFFSET $2
        "#,
        per_page,
        offset
    )
    .fetch_all(pool)
    .await
}
```

### 2.3 API Payload Size Reduction

**Decision**: Strip null fields from JSON responses

**Rationale**:
- Reduces payload size (improves network performance)
- Reduces JSON parsing time (frontend optimization)
- Matches SC-006: <100MB app size constraint

**Implementation Approach**:
```rust
// Backend: Use serde_json with skip_serializing_if
#[derive(Deserialize, Serialize)]
struct Station {
    id: String,
    name: String,
    address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    lat: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lng: Option<f64>,
    // ... other fields
}

// Or use serde_json::json! with custom formatting
let json = serde_json::json!({
    "id": &station.id,
    "name": &station.name,
    "address": &station.address,
    // Only include non-null fields
});
```

### 2.4 Request Timeout Configuration

**Decision**: Set request timeout to 30 seconds

**Rationale**:
- Prevents indefinite waiting on slow queries
- Improves perceived performance (timeout = error with recovery)
- Matches DS-1 requirement (missing timeout, now added)

**Implementation Approach**:
```rust
// Backend: Configure timeout in Actix server
let server = HttpServer::new(move || {
    App::new()
        .app_data(AppState::new(pool.clone()))
        .route("/api/v1/stations", web::get().to(get_stations))
})
.bind(("0.0.0.0", 8080))?
.client_request_timeout(Duration::from_secs(30))
.client_shutdown(Duration::from_secs(5))
.run()
.await?;
```

---

## 3. Expo Performance Best Practices

### 3.1 Skeleton Screen Implementation

**Decision**: Use react-native-skeleton-placeholder with reanimated transitions

**Rationale**:
- Skeleton screens match UX Pro Max rules (no spinners)
- Animations improve perceived performance
- Consistent with React Native best practices

**Implementation Approach**:
```typescript
import SkeletonPlaceholder from 'react-native-skeleton-placeholder';
import Animated, { FadeInUp } from 'react-native-reanimated';

const StationListItemSkeleton = () => {
  return (
    <Animated.View entering={FadeInUp.duration(200)}>
      <SkeletonPlaceholder>
        <SkeletonPlaceholder.Rectangle height={80} radius={8} />
      </SkeletonPlaceholder>
    </Animated.View>
  );
};
```

### 3.2 Dark Mode Implementation

**Decision**: Use existing theme store with AsyncStorage persistence

**Rationale**:
- Constitution requires dark mode from day one
- AsyncStorage provides persistence across sessions
- No additional dependencies needed

**Implementation Approach**:
```typescript
// Theme store already exists
import { useThemeStore } from '../store/useThemeStore';

const { isDarkMode, toggleTheme } = useThemeStore();

// Apply theme to components
<View style={{
  backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
  color: isDarkMode ? '#ffffff' : '#000000',
}}>
```

### 3.3 App Bundle Size Optimization

**Decision**: Code splitting and lazy loading with expo-router

**Rationale**:
- Reduces initial bundle size
- Loads heavy code only when needed
- Improves app launch time

**Implementation Approach**:
```typescript
// Use expo-router lazy loading
import { Stack } from 'expo-router';
import { lazy, Suspense } from 'react';

// Lazy load heavy components
const HeavyComponent = lazy(() => import('./HeavyComponent'));

<Stack.Screen
  name="station/[id]"
  options={{ lazy: true }} // Only load detail screen when visited
/>
```

---

## 4. Mobile Performance Profiling

### 4.1 iOS Performance Profiling

**Decision**: Use Xcode Instruments - Time Profiler, Allocations, Energy Impact

**Rationale**:
- Native iOS profiling tools provide accurate metrics
- Time Profiler identifies CPU bottlenecks
- Allocations track memory leaks
- Energy Impact measures battery drain

**Testing Workflow**:
```bash
# Open Xcode, select target
# Run: Product -> Profile

# Instruments:
# - Time Profiler: Record map interactions
# - Allocations: Check for memory leaks
# - Energy Impact: Measure battery usage during 30 min session
```

### 4.2 Android Performance Profiling

**Decision**: Use Android Studio Profiler - CPU, Memory, Network

**Rationale**:
- Android Studio provides comprehensive profiling
- CPU profiler identifies rendering bottlenecks
- Memory profiler detects leaks
- Network profiler measures API call times

**Testing Workflow**:
```bash
# Run app in Android Studio
# Tools -> Profiler

# Analyze:
# - CPU: Monitor thread usage during map interactions
# - Memory: Check for memory leaks
# - Network: Measure API response times
```

### 4.3 Automated Performance Regression Testing

**Decision**: Use Jest + performance measurement libraries

**Rationale**:
- Automated testing catches regressions
- Performance benchmarks provide quantitative metrics
- CI/CD integration ensures consistent performance

**Implementation Approach**:
```typescript
// performance.test.ts
import { measurePerformance } from 'performance-measure';

describe('Performance', () => {
  it('stations list loads in <200ms', async () => {
    const start = performance.now();
    await render(<StationsList />);
    const end = performance.now();
    const duration = end - start;
    expect(duration).toBeLessThan(200);
  });

  it('marker clustering renders in <100ms', async () => {
    const markers = Array.from({ length: 1000 }, (_, i) => ({
      key: `marker-${i}`,
      coordinate: { latitude: 36.8 + Math.random() * 0.1, longitude: 10.1 + Math.random() * 0.1 },
    }));
    const start = performance.now();
    clusterMarkers(markers, 50);
    const end = performance.now();
    expect(end - start).toBeLessThan(100);
  });
});
```

---

## 5. Analytics Database Reliability

### 5.1 Event Ingestion Patterns

**Decision**: Batch events (max 100) with retry logic on transient failures

**Rationale**:
- Batch processing reduces database load
- Retry on transient failures (network blips)
- Drop on permanent failures (analytics DB unreachable)

**Implementation Approach**:
```rust
// Backend: Batch event ingestion
async fn ingest_events_batch(
    pool: &PgPool,
    events: Vec<RawEvent>,
) -> Result<(), DatabaseError> {
    if events.is_empty() {
        return Ok(());
    }

    sqlx::query!(
        r#"
        INSERT INTO analytics_db.raw_events (event_type, user_action, metadata, timestamp)
        VALUES ($1, $2, $3, $4)
        "#,
        &events[0].event_type,
        &events[0].user_action,
        &events[0].metadata,
        &events[0].timestamp
    )
    .execute(pool)
    .await?;

    Ok(())
}
```

### 5.2 Event Tracking Error Handling

**Decision**: Async batch queue with guaranteed delivery

**Rationale**:
- Immediate event logging blocks UI
- Batch queue improves responsiveness
- Retry logic ensures eventual delivery

**Implementation Approach**:
```typescript
// Frontend: Batch event logging
import { batchLogEvent } from '../services/eventTracking';

// Log event immediately
batchLogEvent('station_view', { stationId: 'STA-123' });

// In background, batch up to 100 events
// Send to backend every 5 seconds or 100 events
```

### 5.3 Event Tracking Monitoring

**Decision**: Track event ingestion success rate in admin service logs

**Rationale**:
- Monitor reliability (SC-007 requires 100% accuracy)
- Alert on event ingestion failures
- Track retry counts

**Implementation Approach**:
```rust
// Backend: Log event tracking metrics
let success = match ingestion_events(&pool, &events).await {
    Ok(_) => true,
    Err(e) => {
        log::warn!("Failed to ingest events: {}", e);
        false
    }
};

// Log metrics for monitoring
log::info!(
    "Event ingestion: success={}, total={}, timestamp={}",
    success,
    events.len(),
    Utc::now()
);
```

---

## 6. Web Performance Optimization

### 6.1 Memory Leak Detection

**Decision**: Use Chrome DevTools Memory profiler and React DevTools Profiler

**Rationale**:
- Chrome DevTools identifies memory leaks
- React DevTools tracks component re-renders
- Fixes needed for web app (WEB-1 HIGH priority issue)

**Implementation Approach**:
```javascript
// Web app: Check for memory leaks
// 1. Open Chrome DevTools
// 2. Performance tab -> Memory -> Take heap snapshot
// 3. Reproduce memory-intensive actions (switch screens multiple times)
// 4. Take another heap snapshot
// 5. Compare snapshots - look for growing node counts
```

### 6.2 React Re-render Optimization

**Decision**: Use React.memo and useMemo for expensive components

**Rationale**:
- Reduces unnecessary re-renders
- Improves web app performance
- Matches mobile optimization patterns

**Implementation Approach**:
```typescript
// Memoize expensive components
const StationDetail = React.memo(({ station }: StationDetailProps) => {
  // ... component logic
}, (prev, next) => {
  return prev.station.id === next.station.id;
});

// Memoize computed values
const filteredStations = useMemo(() => {
  return stations.filter(s => s.status === 'available');
}, [stations]);
```

### 6.3 Lazy Loading for React Router

**Decision**: Use React.lazy and Suspense for route-level code splitting

**Rationale**:
- Loads heavy page code only when needed
- Reduces initial bundle size
- Improves page load performance

**Implementation Approach**:
```typescript
// Lazy load station detail page
const StationDetailPage = lazy(() => import('./station/[id]'));

// Wrap in Suspense with loading state
<Suspense fallback={<StationDetailSkeleton />}>
  <Route path="/station/:id" component={StationDetailPage} />
</Suspense>
```

---

## 7. Cross-Device Testing

### 7.1 iOS Device Testing Matrix

**Decision**: Test on iPhone 12, 13, 14+ (all available models)

**Rationale**:
- Covers range of hardware capabilities
- 12 is oldest supported by Expo SDK 54
- 14 is current flagship (comprehensive test)

**Implementation Approach**:
```bash
# iOS Simulator setup
# Xcode -> Tools -> Devices and Simulators
# Install iOS 16, 17, 18 simulators
# Test on:
# - iPhone 14 Pro (latest)
# - iPhone 13 Pro (mid-range)
# - iPhone 12 Pro (oldest supported)

# Run automated tests
cd source/front/mobile-driver
pnpm ios:profile
```

### 7.2 Android Device Testing

**Decision**: Test on Android 10+ (physical devices, not emulator)

**Rationale**:
- Emulators have different performance characteristics
- Physical devices match real user environment
- Battery testing requires real hardware

**Implementation Approach**:
```bash
# Physical device testing
# Connect Android 10 device via USB
# Enable USB debugging
# Run tests
cd source/front/mobile-driver
pnpm android:profile
```

### 7.3 Battery Impact Testing

**Decision**: Use iOS Energy Impact and Android Battery Historian

**Rationale**:
- Battery drain directly impacts user experience
- <5% drain per hour is strict requirement (SC-004)
- Real-world testing required (not simulated)

**Implementation Approach**:
```bash
# iOS: Energy Impact profiler
# Xcode -> Product -> Profile -> Energy Impact
# Run app for 30 minutes with map interactions
# Record battery drain

# Android: Battery Historian
# adb shell dumpsys batterystats > battery.txt
# Run app for 30 minutes
# Analyze battery.txt for app-specific usage
```

---

## 8. Error Recovery UX Patterns

### 8.1 Network Error Handling

**Decision**: Retry button with exponential backoff (2s, 5s, 10s)

**Rationale**:
- Network failures are transient (retry succeeds)
- Exponential backoff prevents server overload
- User-friendly error message

**Implementation Approach**:
```typescript
// Frontend: Network error with retry
import { useRetry } from '../hooks/useRetry';

const useNearbyStations = (lat: number, lng: number, radius: number) => {
  const [retry, retryCount] = useRetry();

  const { data, error } = useQuery(
    ['nearbyStations', lat, lng, radius],
    () => fetchNearbyStations(lat, lng, radius),
    {
      retry: retryCount > 0,
      retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 10000),
    }
  );

  return { data, error, retry };
};

// UI with retry button
{error && (
  <ErrorState
    message="Network error - unable to load stations"
    retryAction={retry}
  />
)}
```

### 8.2 Error Recovery UI Components

**Decision**: Unified ErrorState component with recovery actions

**Rationale**:
- Consistent error handling across all screens
- User-friendly error messages (never raw error strings)
- Clear recovery actions (button, retry)

**Implementation Approach**:
```typescript
// ErrorState component
interface ErrorStateProps {
  message: string;
  recoveryAction?: () => void;
  icon?: string;
}

const ErrorState = ({ message, recoveryAction }: ErrorStateProps) => {
  return (
    <View style={styles.errorContainer}>
      <Icon name="error" size={48} color="#ef4444" />
      <Text style={styles.errorMessage}>{message}</Text>
      {recoveryAction && (
        <Button onPress={recoveryAction} title="Try Again" />
      )}
    </View>
  );
};
```

### 8.3 Empty States

**Decision**: Fully designed empty states (no blank screens)

**Rationale**:
- UX Pro Max rule (empty states must be fully designed)
- Provides user feedback (no stations found, not app error)
- Encourages exploration (try different parameters)

**Implementation Approach**:
```typescript
// Empty state component
const EmptyState = ({ icon, message, action, actionText }: EmptyStateProps) => {
  return (
    <View style={styles.emptyContainer}>
      <Icon name={icon} size={64} color="#94a3b8" />
      <Text style={styles.emptyMessage}>{message}</Text>
      {action && (
        <Button onPress={action} title={actionText}>
          {actionText}
        </Button>
      )}
    </View>
  );
};

// Use in stations list
{stations.length === 0 && (
  <EmptyState
    icon="map-pin"
    message="No stations found in this area"
    action={() => setSearchRadius(radius + 10)}
    actionText="Search Wider"
  />
)}
```

---

## 9. Accessibility Testing

### 9.1 WCAG AA Compliance Verification

**Decision**: Use automated accessibility testing tools + manual audit

**Rationale**:
- Required for dark mode (SC-005)
- Ensures legal compliance (Tunisia regulations)
- Improves user experience for disabled users

**Implementation Approach**:
```bash
# Automated accessibility testing
# React Native: Use accessibility components correctly
# Web: Use axe DevTools
npm run accessibility-test

# Manual audit checklist
# 1. Contrast ratios: Text vs background (min 4.5:1 normal, 3:1 large)
# 2. Touch targets: Min 44x44pt
# 3. Keyboard navigation: All actions accessible via keyboard
# 4. Screen reader: Descriptive labels for all interactive elements
```

### 9.2 Screen Reader Support

**Decision**: Use accessibilityLabel and accessibilityHint on all interactive elements

**Rationale**:
- Enables screen reader usage
- WCAG AA compliance
- Inclusive design

**Implementation Approach**:
```typescript
// React Native accessibility
<MapView
  accessibilityLabel={`Station: ${station.name}`}
  accessibilityHint={`Located at ${station.address}`}
  accessibilityRole="button"
  onPress={() => handleStationPress(station)}
/>
```

---

## Summary of Decisions

| Category | Decision | Rationale |
|----------|----------|-----------|
| Rendering | Marker clustering with 50m radius | Reduces rendering load from 1000+ to ~50 markers |
| Performance | React.memo + useMemo optimization | Prevents unnecessary re-renders, no marker jitter |
| Animations | reanimated v3 for all animations | 60fps performance, required by constitution |
| API | Batch events, strip null fields | Reduces payload size, improves network performance |
| Queries | GIST index with ST_DWithin | Fast radius search, accurate results |
| Testing | Xcode Instruments + Android Profiler | Native profiling for accurate metrics |
| Battery | Physical device testing | Real-world battery impact measurement |
| Error Recovery | Retry button with exponential backoff | User-friendly, prevents server overload |
| Accessibility | WCAG AA compliance + screen reader | Inclusive design, legal compliance |

All technical decisions align with constitution principles and performance requirements.
