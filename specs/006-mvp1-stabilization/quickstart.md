# Quickstart: MVP-1 Stabilization Testing

**Date**: 2026-06-13
**Feature**: MVP-1 Stabilization Sprint
**Reference**: [spec.md](./spec.md), [research.md](./research.md)

## Overview

This workflow guides you through testing and validating the stabilization sprint improvements: performance optimization, UX polish, error recovery, dark mode, and event tracking reliability.

---

## Prerequisites

- **Backend Running**: Driver service on http://localhost:8080, Admin service on http://localhost:8081
- **Database Connected**: PostgreSQL + PostGIS with test data (2 partners, 5 stations, 10 chargers)
- **Mobile App**: Expo app built and running (Android/iOS or web view)
- **Performance Tools**: Chrome DevTools, Xcode Instruments, or Android Profiler
- **Physical Devices**: iPhone 12/13/14+ and Android 10+ for device testing

---

## Phase 1: Performance Optimization Tests

### 1.1 API Response Time Testing

#### Test 1: Stations List Endpoint
```bash
# Measure response time for paginated stations list
curl -w "\nTotal Time: %{time_total}s\n" -H "Accept-Encoding: gzip" \
     "http://localhost:8080/api/v1/stations?page=1&per_page=20"

# Expected: <200ms p95
# Run multiple times (10+ requests) and calculate average
```

#### Test 2: Nearby Stations Endpoint
```bash
# Measure response time for radius search
curl -w "\nTotal Time: %{time_total}s\n" -H "Accept-Encoding: gzip" \
     "http://localhost:8080/api/v1/stations/nearby?lat=36.8&lng=10.1&radius=50"

# Expected: <100ms p95
# Vary radius (10km, 50km, 100km) and verify consistent performance
```

#### Test 3: Payload Size Comparison
```bash
# Check payload size before optimization (existing)
curl -s -I "http://localhost:8080/api/v1/stations" | grep Content-Length

# After optimization (check if size reduced)
curl -s -I "http://localhost:8080/api/v1/stations" | grep Content-Length

# Expected: 20-30% reduction (strip null fields)
```

#### Test 4: ETag Caching
```bash
# First request (ETag generated)
ETAG_1=$(curl -s -I "http://localhost:8080/api/v1/stations" | grep ETag | cut -d' ' -f2)
echo "First ETag: $ETAG_1"

# Second request (ETag used, 304 Not Modified)
curl -s -I -H "If-None-Match: $ETAG_1" "http://localhost:8080/api/v1/stations"

# Expected: 304 Not Modified response
```

### 1.2 React Native Performance Testing

#### Test 5: Marker Clustering Performance
```bash
# Open mobile app on Android/iOS
# Navigate to map screen with 1000+ stations
# Measure time to render and pan

# Expected:
# - Initial render: <300ms
# - Pan animation: 60fps (no frame drops)
# - Zoom in/out: Smooth transitions
# - Marker clustering: No jitter or flashing
```

#### Test 6: Screen Transition Times
```bash
# Measure screen transitions:
# 1. Map screen → Station detail
# 2. Station detail → Map screen
# 3. List screen → Map screen

# Use React DevTools (Performance tab):
# 1. Open React DevTools (F12)
# 2. Go to Performance tab
# 3. Record a screen transition
# 4. Stop recording
# 5. Check "Main" thread profile for frame drops

# Expected: <200ms transition time, no frame drops
```

#### Test 7: Bundle Size Optimization
```bash
# Check app bundle size after optimization
# Android: Build APK and check size
cd source/front/mobile-driver
pnpm android:release
# Check: android/app/build/outputs/apk/release/app-release.apk

# Expected: <100MB

# iOS: Build IPA and check size
# Expected: <100MB
```

---

## Phase 2: UX Polish Testing

### 2.1 Skeleton Screens

#### Test 8: Loading States on Screens
```bash
# Navigate through all screens:
# 1. Stations list (initial load)
# 2. Station detail (tap marker)
# 3. Nearby search (search radius)

# Expected:
# - Animated skeleton placeholders appear immediately
# - No spinner (no spinning loaders)
# - Smooth transitions (no flickering)
# - Layout matches actual content
```

#### Test 9: Skeleton State Consistency
```bash
# 1. Navigate to a screen with data
# 2. Wait for data to load
# 3. Go back, immediately navigate forward

# Expected:
# - Skeleton appears immediately on navigation
# - Skeleton animation continues during load
# - No content flashing during transition
```

### 2.2 Dark Mode Testing

#### Test 10: Dark Mode Toggle
```bash
# 1. Open app
# 2. Toggle dark mode in settings
# 3. Navigate through all screens:
#    - Map screen
#    - Stations list
#    - Station detail
#    - Settings screen

# Expected:
# - All screens update instantly with consistent colors
# - No broken elements (stretched, inverted, missing)
# - Text remains readable
# - No jarring color changes
```

#### Test 11: Dark Mode Contrast
```bash
# Use automated accessibility testing:
cd source/front/mobile-driver
pnpm accessibility-test

# Or manual check:
# 1. Open each screen in dark mode
# 2. Check text vs background contrast ratios
# 3. Verify WCAG AA compliance (4.5:1 for normal text, 3:1 for large text)

# Expected:
# - All text meets WCAG AA contrast requirements
# - No low-contrast sections
```

### 2.3 Haptic Feedback

#### Test 12: Haptic Feedback on CTAs
```bash
# Test all primary call-to-action buttons:
# 1. Station marker press (marker press)
# 2. Station detail open (station detail view)
# 3. Search nearby button (search button)
# 4. Dark mode toggle (settings button)

# Expected:
# - Medium haptic impact on station marker press
# - Success haptic on station detail open
# - Success haptic on search button
# - Light haptic on settings toggle
```

---

## Phase 3: Error Recovery Testing

### 3.1 Network Error Handling

#### Test 13: No Internet Connection
```bash
# 1. Turn off internet (Airplane mode on mobile)
# 2. Try to load stations list
# 3. Observe error state

# Expected:
# - Error message: "Network error - unable to load stations"
# - Retry button present
# - Error message is user-friendly (no raw error string)
# - Retry button works (network restored, retry successful)
```

#### Test 14: Server Error (5xx)
```bash
# 1. Start admin service with invalid database (simulated)
# 2. Try to load stations list
# 3. Observe error state

# Expected:
# - Error message explains issue
# - Retry button present
# - System maintains state (no data loss)
# - Retry attempts (exponential backoff: 2s, 5s, 10s)
```

#### Test 15: Timeout Handling
```bash
# 1. Slow down API (edit docker-compose.yml to increase timeout)
# 2. Try to load stations list

# Expected:
# - Timeout occurs after 30 seconds
# - Error message: "Request timed out"
# - Retry button present
# - User can retry after timeout
```

### 3.2 Error Recovery UI

#### Test 16: Error State with Recovery
```bash
# Test error recovery on each screen:
# 1. Stations list (network error)
# 2. Station detail (cache error)
# 3. Map (location permission denied)

# For each:
# 1. Trigger error
# 2. Observe error message
# 3. Check recovery action (retry button)
# 4. Click retry
# 5. Verify recovery success (error cleared, content loads)

# Expected:
# - User-friendly error messages (never raw error strings)
# - Clear recovery actions (retry button, not just "OK")
# - Recovery succeeds on retry
# - No blank screens after error
```

### 3.3 Empty States

#### Test 17: Empty Stations Found
```bash
# 1. Search for stations in remote area (radius=5000)
# 2. Observe empty state

# Expected:
# - Fully designed empty state (not blank screen)
# - Icon displayed (e.g., "map-pin")
# - User-friendly message: "No stations found in this area"
# - Action: "Search Wider" button
# - Button works (searches larger radius)
```

#### Test 18: Error on Empty
```bash
# 1. Trigger network error on empty state
# 2. Observe error message

# Expected:
# - Error message clearly explains issue
# - Retry button present
# - No blank screen
```

---

## Phase 4: Event Tracking Testing

### 4.1 Event Logging

#### Test 19: Station View Event
```bash
# 1. Open app
# 2. Tap on a station marker
# 3. Check analytics database for event log

# Query:
```bash
docker compose exec -T analytics-db psql -U borneadmin -d analytics_db -c \
  "SELECT event_type, user_action, station_id, timestamp FROM raw_events \
   WHERE event_type = 'station_view' ORDER BY timestamp DESC LIMIT 1;"
```

# Expected:
# - Event type: "station_view"
# - User action: "marker_press"
# - Station ID present
# - Timestamp in ISO 8601 format
# - Device info present
```

#### Test 20: Search Event
```bash
# 1. Search for nearby stations
# 2. Check analytics database

# Expected:
# - Event type: "search_nearby"
# - User action: "search_action"
# - Lat/lng parameters present
# - Timestamp recorded
```

#### Test 21: Error Event
```bash
# 1. Trigger network error
# 2. Check analytics database

# Expected:
# - Event type: "error_occurred"
# - Error message present
# - Recovery action logged (if user clicked retry)
# - Retry count present
```

### 4.2 Batch Event Ingestion

#### Test 22: Batch Event Logging
```bash
# 1. Perform 10 user actions (open app, view stations, search, etc.)
# 2. Wait 10 seconds (batch timeout)
# 3. Check analytics database

# Query:
```bash
docker compose exec -T analytics-db psql -U borneadmin -d analytics_db -c \
  "SELECT COUNT(*) FROM raw_events WHERE timestamp >= NOW() - INTERVAL '10 seconds';"
```

# Expected:
# - 10 events logged (or batched into 1 insert)
# - All events have unique IDs
# - All events have valid JSONB metadata
```

#### Test 23: Batch Timeout Handling
```bash
# 1. Try to send batch of 100 events simultaneously
# 2. Observe response

# Expected:
# - Response: 200 OK
# - X-Processed-Count: 100
# - All 100 events logged to database
# - No timeout error (handled gracefully)
```

#### Test 24: Batch Retry Logic
```bash
# 1. Stop analytics-db temporarily
# 2. Try to log events
# 3. Start analytics-db
# 4. Retry events

# Expected:
# - Events log successfully after database restored
# - Retry count increases
# - No data loss (events queued until successful)
```

---

## Phase 5: Cross-Device Testing

### 5.1 iOS Device Testing

#### Test 25: iPhone 14 Pro
```bash
# 1. Connect iPhone 14 Pro via USB
# 2. Build and run app in Xcode
# 3. Test all stabilization features:

# Performance:
# - Map rendering: 1000+ markers, no jitter
# - Screen transitions: <200ms
# - Battery drain: <5% per hour

# UX:
# - Dark mode: All screens, perfect contrast
# - Skeleton screens: Smooth animations
# - Haptics: Medium impact on CTAs

# Expected:
# - 60fps smooth animations
# - No console errors/warnings
# - Battery drain <5% per hour
# - All UX requirements met
```

#### Test 26: iPhone 12 Pro (Oldest Supported)
```bash
# 1. Build and run on iPhone 12 Pro
# 2. Test performance and battery

# Expected:
# - Map rendering: Stable (may be slightly slower than 14 Pro)
# - Battery drain: <8% per hour (acceptable for older hardware)
# - All features work correctly
```

### 5.2 Android Device Testing

#### Test 27: Android 10+ (Physical Device)
```bash
# 1. Connect Android 10+ device via USB
# 2. Enable USB debugging
# 3. Build and run app
# 4. Test all stabilization features

# Performance:
# - Map rendering: 1000+ markers, no jitter
# - Screen transitions: <200ms
# - Battery drain: <5% per hour

# Expected:
# - 60fps smooth animations
# - No console errors/warnings
# - Battery drain <5% per hour
# - All UX requirements met
```

### 5.3 Battery Impact Testing

#### Test 28: Battery Drain Measurement
```bash
# iOS:
# 1. Open Xcode -> Product -> Profile
# 2. Select "Energy Impact" template
# 3. Run app for 30 minutes with map interactions
# 4. Analyze energy impact

# Android:
# 1. Open Android Studio Profiler -> Battery Historian
# 2. Run app for 30 minutes with map interactions
# 3. Analyze battery.txt for app-specific usage

# Expected:
# - Battery drain: <5% per hour (iOS), <8% per hour (Android 10)
# - Map interactions: Energy efficient
# - No background battery drain
```

---

## Phase 6: Console Error & Warning Testing

### 6.1 No Console Errors
```bash
# Mobile app:
# 1. Run app on iOS Simulator / Android Emulator / Web
# 2. Open Chrome DevTools (F12) or Xcode Console / Android Logcat
# 3. Navigate through all screens
# 4. Check for errors/warnings

# Expected:
# - Zero console errors
# - Zero console warnings
# - No JavaScript errors
# - No React Native errors
# - No network errors (except expected errors)

# Common errors to fix:
# - Map jitter (fixed with React.memo)
# - Unnecessary re-renders (fixed with useMemo)
# - Memory leaks (fixed in web app)
# - Unhandled promise rejections (fixed with error boundaries)
```

### 6.2 Post-Flight Review
```bash
# Check backend console logs for errors:
docker compose logs driver-service --tail 50
docker compose logs admin-service --tail 50

# Expected:
# - Zero errors in logs
# - Only info level logs for normal operation
# - No stack traces
# - No panic errors
```

---

## Phase 7: Accessibility Testing

### 7.1 WCAG AA Compliance
```bash
# Automated testing:
cd source/front/mobile-driver
pnpm accessibility-test

# Manual check:
# 1. Open each screen in dark mode and light mode
# 2. Check text contrast ratios:
#    - Normal text: 4.5:1 minimum
#    - Large text: 3:1 minimum
# 3. Check touch targets: 44x44pt minimum
# 4. Check keyboard navigation: All actions accessible
# 5. Check screen reader: Descriptive labels

# Expected:
# - All screens pass WCAG AA
# - Contrast ratios meet requirements
# - Touch targets are 44x44pt or larger
# - Keyboard accessible
# - Screen reader compatible
```

---

## Phase 8: Analytics Database Integrity

### 8.1 Append-Only Validation
```bash
# Check that analytics_db is truly append-only:
docker compose exec -T analytics-db psql -U borneadmin -d analytics_db -c \
  "SELECT event_type, COUNT(*) FROM raw_events GROUP BY event_type ORDER BY event_type;"

# Expected:
# - All rows have valid event_type
# - No updates or deletes possible
# - Data integrity maintained
```

### 8.2 Event Tracking Reliability
```bash
# Test event tracking reliability:
# 1. Trigger 50 different user actions
# 2. Wait for batch processing
# 3. Check analytics_db for all events

# Query:
```bash
docker compose exec -T analytics-db psql -U borneadmin -d analytics_db -c \
  "SELECT COUNT(*) FROM raw_events WHERE timestamp >= NOW() - INTERVAL '1 minute';"
```

# Expected:
# - All 50 events logged
# - Unique IDs for each event
# - Valid timestamps
# - No duplicate events
# - No missing events
```

---

## Test Summary Checklist

### Performance
- [ ] Stations list: <200ms p95
- [ ] Nearby search: <100ms p95
- [ ] Payload size: Reduced 20-30%
- [ ] ETag caching: Works correctly
- [ ] Marker clustering: 1000+ markers, no jitter
- [ ] Screen transitions: <200ms, 60fps
- [ ] Bundle size: <100MB

### UX Polish
- [ ] Skeleton screens: Animated, no flickering
- [ ] Dark mode: All screens, perfect contrast
- [ ] Haptics: Medium impact on CTAs
- [ ] Empty states: Fully designed, not blank
- [ ] Error states: User-friendly, recovery actions

### Error Recovery
- [ ] Network errors: Retry button works
- [ ] Server errors: System maintains state
- [ ] Timeout: Handled gracefully
- [ ] Error messages: User-friendly (no raw strings)

### Event Tracking
- [ ] Single event: Logged correctly
- [ ] Batch event: Max 100 events, 500ms timeout
- [ ] Retry logic: Works on transient failures
- [ ] Analytics DB: Append-only, no updates

### Cross-Device
- [ ] iOS 14 Pro: 60fps, <5% battery drain
- [ ] iOS 12 Pro: Stable, <8% battery drain
- [ ] Android 10+: 60fps, <5% battery drain

### Quality
- [ ] Zero console errors
- [ ] Zero console warnings
- [ ] WCAG AA compliance: All screens
- [ ] Analytics integrity: Append-only, reliable

---

## Completion Criteria

All tests passed:

**Performance**: ✅
- [ ] Response times meet targets
- [ ] Bundle size <100MB
- [ ] 60fps smooth animations

**UX**: ✅
- [ ] Skeleton screens working
- [ ] Dark mode perfect
- [ ] Haptics on CTAs
- [ ] Empty states designed

**Reliability**: ✅
- [ ] Error recovery works
- [ ] Retry logic functional
- [ ] No blank screens

**Testing**: ✅
- [ ] iOS 12/13/14+ tested
- [ ] Android 10+ tested
- [ ] Battery drain <5% (iOS) / <8% (Android 10)

**Quality**: ✅
- [ ] Zero console errors
- [ ] WCAG AA compliant
- [ ] Analytics DB append-only

**Ready for MVP-1 Launch** 🚀
