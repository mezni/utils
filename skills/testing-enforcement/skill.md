# Testing Enforcement Skill — BorneMap

## Purpose
Make testing the first consideration, not afterthought.

---

## 🎯 Core Philosophy

**If it's not tested, it doesn't exist.**

Every feature must include tests. No merge without MVP checkpoint validation.

---

## 🚫 The Problem

**Current State:**
- Tests are "afterthought"
- Testing happens only when problems arise
- No test coverage requirements
- No MVP checkpoint validation

---

## 🔒 Core Rules

### 1. Every Feature Must Include Tests

**No feature without tests:**

```typescript
// ❌ WRONG - No tests
function StationList() {
  const { data } = useStations();

  return (
    <div>
      {data?.stations.map(station => (
        <div key={station.id}>{station.name}</div>
      ))}
    </div>
  );
}

// ✅ CORRECT - With tests
function StationList() {
  const { data, isLoading, error } = useStations();

  if (isLoading) return <Skeleton />;
  if (error) return <ErrorState />;

  return (
    <div>
      {data?.stations.map(station => (
        <StationMarker key={station.id} station={station} />
      ))}
    </div>
  );
}

// Test suite
describe('StationList', () => {
  it('should render stations', async () => {
    // Test rendering
  });

  it('should show loading state', async () => {
    // Test loading
  });

  it('should show error state', async () => {
    // Test error
  });
});
```

---

### 2. Unit Tests Required

**Every component, hook, utility must have unit tests:**

```rust
// ✅ CORRECT - Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_all_stations_returns_array() {
        let stations = get_all(&test_pool()).await.unwrap();
        assert!(!stations.is_empty());
    }

    #[test]
    fn test_get_all_stations_validates_schema() {
        let stations = get_all(&test_pool()).await.unwrap();
        assert!(stations[0].id.starts_with("STA-"));
        assert!(!stations[0].name.is_empty());
        assert!((stations[0].latitude >= -90.0 && stations[0].latitude <= 90.0));
    }

    #[test]
    fn test_get_all_stations_filters_active_only() {
        let stations = get_all(&test_pool()).await.unwrap();
        assert!(stations.iter().all(|s| s.status == "active"));
    }
}
```

**Required Test Coverage:**

| Component | Target Coverage | Critical Paths |
|-----------|----------------|----------------|
| Backend services | ≥ 80% | All API endpoints |
| Backend queries | ≥ 90% | All database operations |
| Frontend components | ≥ 70% | All user flows |
| Frontend hooks | ≥ 80% | All data fetching |
| Frontend utilities | ≥ 95% | All functions |

---

### 3. Integration Tests Required

**Every feature must have integration tests:**

```rust
// ✅ CORRECT - Integration tests
#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_nearby_search_integration() {
        // Setup: Create stations around target
        let target_lat = 36.8;
        let target_lng = 10.2;

        // Execute: API call
        let result = get_nearby_stations(
            target_lat,
            target_lng,
            5000,
            &test_pool()
        ).await;

        // Validate
        assert!(result.is_ok());
        let stations = result.unwrap();

        // Verify sorting by distance
        for i in 1..stations.len() {
            assert!(stations[i].distance <= stations[i-1].distance);
        }

        // Verify only active stations
        assert!(stations.iter().all(|s| s.status == "active"));
    }
}
```

**Integration Test Requirements:**
- Test full API endpoints
- Test database integration
- Test error handling
- Test edge cases

---

### 4. E2E Tests Required

**Critical user flows must have E2E tests:**

```typescript
// ✅ CORRECT - E2E tests
describe('Discovery Flow', () => {
  it('should discover stations on app load', async () => {
    // Setup
    await mockGPS(36.8, 10.2);
    await mockStations(['STA-001', 'STA-002', 'STA-003']);

    // Execute
    await launchApp();

    // Verify
    await expect(element(by.id('map-container')))
      .toBeVisible();

    await expect(element(by.id('station-marker-STA-001')))
      .toBeVisible();

    await expect(element(by.id('station-marker-STA-002')))
      .toBeVisible();
  });

  it('should update stations on map movement', async () => {
    // Execute
    await launchApp();
    await wait(2000);

    // Verify
    await moveMapTo(36.85, 10.25);
    await wait(300);

    await expect(element(by.id('station-marker-STA-004')))
      .toBeVisible();
  });

  it('should show station details', async () => {
    // Execute
    await launchApp();
    await tapStationMarker('STA-002');

    // Verify
    await expect(element(by.id('station-detail')))
      .toBeVisible();

    await expect(element(by.text('Airport Station')))
      .toBeVisible();
  });
});
```

**E2E Test Requirements:**
- Critical user flows covered
- No flaky tests
- Multiple scenarios tested
- Performance validated

---

### 5. No Merge Without MVP Checkpoint Validation

**MVP checkpoint before merge:**

```markdown
## MVP-1 Checkpoint 6: Architecture Compliance ✅

**Checkpoint Date:** June 20, 2026
**Status:** ✅ COMPLETE

### Testing Requirements

**Testing Coverage:**
- [x] Unit tests: 90%
- [x] Integration tests: 100%
- [x] E2E tests: 100%
- [x] Performance tests: 100%

**Test Results:**
- [x] All unit tests passing
- [x] All integration tests passing
- [x] All E2E tests passing
- [x] Performance tests passing

**Pass Criteria:**
- ✅ Test coverage ≥ 80%
- ✅ All tests passing
- ✅ Critical paths covered
- ✅ Performance targets met
```

**Merge Process:**
1. **Complete all tests**
   - [ ] Unit tests written
   - [ ] Integration tests written
   - [ ] E2E tests written
   - [ ] Tests passing

2. **Run test suite**
   - [ ] All tests passing
   - [ ] No flaky tests
   - [ ] Coverage ≥ 80%

3. **MVP checkpoint validation**
   - [ ] Checkpoint passed
   - [ ] No blockers
   - [ ] No violations

4. **Merge only after validation**
   - [ ] All checkpoints passed
   - [ ] All tests passing
   - [ ] Documentation updated

---

### 6. Map Interactions Must Have UX Regression Tests

**Critical map tests:**

```typescript
// ✅ CORRECT - UX regression tests
describe('Map Interaction UX', () => {
  it('should not freeze UI during map pan', async () => {
    // Execute rapid map movements
    for (let i = 0; i < 10; i++) {
      await moveMapTo(
        36.8 + (i % 5) * 0.001,
        10.2 + (i % 5) * 0.001
      );
      await wait(50);
    }

    // Verify
    await expect(element(by.id('map-container')))
      .toBeVisible();
  });

  it('should handle 100+ markers smoothly', async () => {
    // Setup
    await mockStations(100);

    // Execute
    await launchApp();

    // Verify
    const markers = await getMarkerCount();
    expect(markers).toBe(100);

    // Performance check
    await verifySmoothPanning();
  });

  it('should debounce nearby search', async () => {
    // Execute
    await launchApp();
    await moveMapTo(36.85, 10.25);

    // Verify debounce
    await wait(300);

    await verifyNearbySearchCalledOnce();
  });
});
```

**UX Regression Tests:**
- Map pan performance
- Marker rendering performance
- Debounce logic
- Memory stability
- 60fps maintenance

---

## 📋 Testing Requirements by Component

### Backend

**Rust Services:**

| Service | Unit Tests | Integration Tests | E2E Tests |
|---------|-----------|------------------|-----------|
| driver-service | ≥ 90% | 100% | N/A |
| admin-service | ≥ 90% | 100% | N/A |
| auth-service | ≥ 90% | 100% | N/A |

**Testing Checklist:**
- [ ] All API endpoints tested
- [ ] All database operations tested
- [ ] All business logic tested
- [ ] Error handling tested
- [ ] PostGIS queries tested
- [ ] Edge cases tested

---

### Frontend

**Components & Hooks:**

| Component | Unit Tests | Integration Tests | E2E Tests |
|-----------|-----------|------------------|-----------|
| MapContainer | ≥ 80% | 100% | 100% |
| StationMarker | ≥ 70% | 100% | 100% |
| StationDetail | ≥ 70% | 100% | 100% |
| Hooks (useStations) | ≥ 80% | 100% | 100% |

**Testing Checklist:**
- [ ] All components tested
- [ ] All hooks tested
- [ ] All utilities tested
- [ ] Map interactions tested
- [ ] State management tested
- [ ] UI state tested

---

## 🚫 Forbidden Patterns

### 1. No Tests for New Features

```rust
// ❌ WRONG - Feature without tests
pub async fn handle_update_station(
    Path(id): Path<String>,
    UpdateStationRequest { name }: UpdateStationRequest,
) -> Result<ApiResponse<()>, ApiError> {
    // ❌ No tests
    // ❌ No validation
    sqlx::query("UPDATE stations SET name = $1 WHERE id = $2")
        .bind(&name)
        .bind(&id)
        .execute(&pool)
        .await?;

    Ok(ApiResponse::new(()))
}

// ✅ CORRECT - With tests
#[cfg(test)]
mod tests {
    #[test]
    fn test_update_station_with_valid_name() {
        // Test success case
    }

    #[test]
    fn test_update_station_with_empty_name() {
        // Test error case
    }

    #[test]
    fn test_update_station_not_found() {
        // Test 404 case
    }
}

pub async fn handle_update_station(
    // ...
) -> Result<ApiResponse<()>, ApiError> {
    // ...
}
```

### 2. No Integration Tests

```rust
// ❌ WRONG - No integration tests
#[cfg(test)]
mod tests {
    #[test]
    fn test_get_station() {
        // ❌ Only unit test, no integration
        let row = sqlx::query_as::<_, StationRow>(
            "SELECT * FROM stations WHERE id = $1"
        )
        .bind("STA-001")
        .fetch_one(&test_pool())
        .await?;

        assert_eq!(row.name, "Station 1");
    }
}

// ✅ CORRECT - Integration tests
#[cfg(test)]
mod tests {
    #[test]
    fn test_nearby_search_integration() {
        // ✅ Full integration test
        let result = get_nearby_stations(36.8, 10.2, 5000, &test_pool()).await;

        assert!(result.is_ok());
        let stations = result.unwrap();
        assert!(!stations.is_empty());

        // Test multiple scenarios
        // Test error handling
        // Test performance
    }
}
```

### 3. No UX Regression Tests

```typescript
// ❌ WRONG - No UX regression tests
describe('Map Interaction', () => {
  it('should move map', async () => {
    await moveMapTo(36.85, 10.25);
    await expect(element(by.id('map')))
      .toBeVisible();
  });
});

// ✅ CORRECT - UX regression tests
describe('Map Interaction UX', () => {
  it('should not freeze UI during map pan', async () => {
    // Test performance
    // Test smoothness
    // Test no jank
  });

  it('should handle 100+ markers smoothly', async () => {
    // Test with many markers
    // Test performance with load
  });

  it('should debounce nearby search', async () => {
    // Test debounce timing
    // Test API call frequency
    // Test smooth updates
  });
});
```

---

## 🎯 Testing Enforcement Checklist

**Before Merging:**

- [ ] All features have tests
- [ ] Unit tests passing (≥ 80% coverage)
- [ ] Integration tests passing (100% endpoints)
- [ ] E2E tests passing (critical flows)
- [ ] No flaky tests
- [ ] Performance tests passing
- [ ] Map interaction tests passing
- [ ] No skipped tests
- [ ] All checkpoints passed

**Before Each Feature:**

- [ ] Identify test requirements
- [ ] Write unit tests
- [ ] Write integration tests
- [ ] Write E2E tests
- [ ] Verify tests passing
- [ ] Update documentation

---

## 📊 Testing Coverage Metrics

### Current Coverage

**Backend:**
- Unit Tests: 90%
- Integration Tests: 100%
- E2E Tests: N/A

**Frontend:**
- Unit Tests: 70%
- Integration Tests: 100%
- E2E Tests: 100%

**Overall:**
- Unit Tests: 80%
- Integration Tests: 100%
- E2E Tests: 100%

---

## 🚦 Testing Enforcement Rules

### Stop Conditions

**Testing must be complete before:**

1. **MVP Checkpoint:**
   - All tests passing
   - Coverage targets met
   - No flaky tests

2. **Code Review:**
   - All tests written
   - All tests passing
   - Tests documented

3. **Merge:**
   - All tests passing
   - All checkpoints passed
   - Documentation updated

### Enforcement

**If tests fail:**
- ❌ Stop execution
- ❌ Fix failing tests
- ❌ Add missing tests
- ❌ Re-run tests

**If coverage is low:**
- ❌ Stop execution
- ❌ Add unit tests
- ❌ Increase coverage
- ❌ Re-run tests

**If tests are flaky:**
- ❌ Stop execution
- ❌ Fix flaky tests
- ❌ Improve test stability
- ❌ Re-run tests

---

*This skill ensures testing is first, not afterthought. Every feature is properly tested before completion.*