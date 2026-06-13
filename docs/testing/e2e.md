# End-to-End Tests

## Version: 1.0
## Status: Active
## Focus: Complete user workflows

---

## 🎯 TESTING OBJECTIVE

Test complete user journeys from app launch to station discovery.

---

## 🧪 E2E SCENARIOS

### 1. Complete Discovery Flow

**Test File:** `e2e/discovery.spec.ts`

```typescript
describe('Complete Discovery Flow', () => {
  beforeEach(async () => {
    await resetApp();
    await mockGPS(36.8, 10.2);
    await mockStations(['STA-001', 'STA-002', 'STA-003']);
  });

  it('should discover stations on app load', async () => {
    // Step 1: Launch app
    await launchApp();

    // Step 2: Verify map loads
    await expect(element(by.id('map-container')))
      .toBeVisible();

    // Step 3: Verify markers appear
    await expect(element(by.id('station-marker-STA-001')))
      .toBeVisible();
    await expect(element(by.id('station-marker-STA-002')))
      .toBeVisible();

    // Step 4: Verify no loading state
    await expect(element(by.id('loading-indicator')))
      .toBeNotVisible();
  });

  it('should update stations on map movement', async () => {
    // Step 1: Load initial stations
    await launchApp();
    await wait(2000);

    // Step 2: Move map
    await moveMapTo(36.85, 10.25);

    // Step 3: Wait for debounce
    await wait(300);

    // Step 4: Verify new stations loaded
    // Step 5: Verify no full reload
    // Step 6: Verify smooth animation
  });

  it('should show station details', async () => {
    // Step 1: Tap on station marker
    await tapStationMarker('STA-002');

    // Step 2: Verify detail view opens
    await expect(element(by.id('station-detail')))
      .toBeVisible();

    // Step 3: Verify correct station data
    await expect(element(by.text('Airport Station')))
      .toBeVisible();

    // Step 4: Verify chargers listed
    await expect(element(by.text('CCS')))
      .toBeVisible();

    // Step 5: Verify data loaded
    await wait(1000);
  });
});
```

---

### 2. Error Recovery Flow

**Test File:** `e2e/error_recovery.spec.ts`

```typescript
describe('Error Recovery Flow', () => {
  beforeEach(async () => {
    await resetApp();
  });

  it('should handle network errors', async () => {
    // Step 1: Launch app
    await launchApp();

    // Step 2: Mock network failure
    await mockNetworkFailure();

    // Step 3: Verify error state
    await expect(element(by.id('error-message')))
      .toBeVisible();

    // Step 4: Verify retry option
    await expect(element(by.id('retry-button')))
      .toBeVisible();

    // Step 5: Tap retry
    await tapRetry();

    // Step 6: Verify retry works
    await wait(2000);
    await expect(element(by.id('error-message')))
      .toBeNotVisible();
  });

  it('should handle GPS errors', async () => {
    // Step 1: Launch app
    await launchApp();

    // Step 2: Mock GPS denied
    await mockGPSDenied();

    // Step 3: Verify permission error
    await expect(element(by.text('Location Permission')))
      .toBeVisible();

    // Step 4: Verify retry option
    await expect(element(by.id('retry-button')))
      .toBeVisible();
  });

  it('should handle no stations nearby', async () => {
    // Step 1: Mock no stations
    await mockStations([]);

    // Step 2: Launch app
    await launchApp();

    // Step 3: Verify empty state
    await expect(element(by.id('empty-stations')))
      .toBeVisible();

    // Step 4: Verify retry option
    await expect(element(by.id('retry-button')))
      .toBeVisible();
  });
});
```

---

### 3. Analytics Flow

**Test File:** `e2e/analytics.spec.ts`

```typescript
describe('Analytics Integration', () => {
  it('should track MapViewed event', async () => {
    // Step 1: Launch app
    await launchApp();

    // Step 2: Verify MapViewed event sent
    await verifyAnalyticsEvent('MapViewed');
    await verifyAnalyticsPayload({
      timestamp: expect.any(Date),
      source: 'app'
    });
  });

  it('should track StationOpened event', async () => {
    // Step 1: Launch app
    await launchApp();

    // Step 2: Tap on station
    await tapStationMarker('STA-002');

    // Step 3: Verify StationOpened event sent
    await verifyAnalyticsEvent('StationOpened');
    await verifyAnalyticsPayload({
      station_id: 'STA-002',
      timestamp: expect.any(Date)
    });
  });

  it('should track NearbySearchExecuted event', async () => {
    // Step 1: Launch app
    await launchApp();

    // Step 2: Move map
    await moveMapTo(36.85, 10.25);

    // Step 3: Wait for debounce
    await wait(300);

    // Step 4: Verify NearbySearchExecuted event sent
    await verifyAnalyticsEvent('NearbySearchExecuted');
    await verifyAnalyticsPayload({
      latitude: 36.85,
      longitude: 10.25,
      radius: 5000,
      timestamp: expect.any(Date)
    });
  });
});
```

---

### 4. Performance Flow

**Test File:** `e2e/performance.spec.ts`

```typescript
describe('Performance Tests', () => {
  it('should load map in < 2 seconds', async () => {
    const startTime = Date.now();

    await launchApp();

    await expect(element(by.id('map-container')))
      .toBeVisible();

    const loadTime = Date.now() - startTime;

    expect(loadTime).toBeLessThan(2000);
  });

  it('should handle 100+ markers smoothly', async () => {
    // Step 1: Mock 100 stations
    await mockStations(100);

    // Step 2: Launch app
    await launchApp();

    // Step 3: Measure marker count
    const markers = await getMarkerCount();
    expect(markers).toBe(100);

    // Step 4: Pan map
    for (let i = 0; i < 10; i++) {
      await moveMapTo(36.8 + i * 0.001, 10.2 + i * 0.001);
      await wait(100);
    }

    // Step 5: Verify smooth panning
    await verifySmoothPanning();
  });

  it('should not block UI during interactions', async () => {
    // Step 1: Launch app
    await launchApp();

    // Step 2: Simulate rapid map movements
    const moves = 20;
    for (let i = 0; i < moves; i++) {
      await moveMapTo(
        36.8 + (i % 5) * 0.001,
        10.2 + (i % 5) * 0.001
      );
      await wait(50);
    }

    // Step 3: Verify UI remains responsive
    await expect(element(by.id('map-container')))
      .toBeVisible();
  });
});
```

---

### 5. Platform-Specific Flows

**Mobile Flow Test:**

```typescript
describe('Mobile-Specific Flows', () => {
  it('should show haptic feedback', async () => {
    // Step 1: Tap on marker
    await tapStationMarker('STA-002');

    // Step 2: Verify haptic feedback
    await verifyHapticFeedback();
  });

  it('should handle gesture swipe', async () => {
    // Step 1: Open station detail
    await tapStationMarker('STA-002');

    // Step 2: Swipe down to close
    await swipeDown();

    // Step 3: Verify detail closes
    await expect(element(by.id('station-detail')))
      .toBeNotVisible();
  });
});
```

**Web Flow Test:**

```typescript
describe('Web-Specific Flows', () => {
  it('should support keyboard navigation', async () => {
    // Step 1: Focus on map
    await element(by.id('map-container')).setFocus();

    // Step 2: Use arrow keys to pan
    await pressKey('ArrowRight');
    await pressKey('ArrowDown');

    // Step 3: Verify map moved
    await verifyMapMoved();
  });

  it('should support mouse interactions', async () => {
    // Step 1: Hover over marker
    await hoverOverMarker('STA-002');

    // Step 2: Click marker
    await clickMarker('STA-002');

    // Step 3: Verify detail opens
    await expect(element(by.id('station-detail')))
      .toBeVisible();
  });
});
```

---

## 🧪 TEST EXECUTION

### Running E2E Tests

**Mobile (Expo):**
```bash
# Run all E2E tests
npm run e2e

# Run specific test file
npm run e2e -- discovery.spec.ts

# Run with coverage
npm run e2e:coverage
```

**Web (Cypress):**
```bash
# Run all E2E tests
npm run test:e2e

# Run specific test file
npm run test:e2e -- discovery.spec.ts

# Run in headless mode
npm run test:e2e:headless
```

---

## 🎯 TEST COVERAGE REQUIREMENTS

### Critical Flows

- [ ] App launch → map load
- [ ] Map movement → search update
- [ ] Station selection → detail view
- [ ] Network error → retry
- [ ] GPS error → permission request
- [ ] No stations → empty state

### Platform Coverage

- [ ] Mobile interactions
- [ ] Web interactions
- [ ] Keyboard navigation (web)
- [ ] Mouse interactions (web)

---

## 🚀 CI/CD INTEGRATION

### Automated Testing

```yaml
# GitHub Actions example
name: E2E Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-node@v2
      - name: Install dependencies
        run: npm ci
      - name: Run E2E tests
        run: npm run e2e
      - name: Upload test results
        uses: actions/upload-artifact@v2
        with:
          name: test-results
          path: coverage/
```

---

## 🧠 CORE PRINCIPLE

**E2E tests are the ultimate validation. If the user can't complete the flow, the tests catch it.**

---

*This document provides comprehensive E2E testing scenarios for MVP-1.*