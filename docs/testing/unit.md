# Unit Tests

## Version: 1.0
## Status: Active
## Focus: MVP-1 Core Components

---

## 🎯 TESTING OBJECTIVE

Ensure correctness of individual components before integration.

---

## 🧱 BACKEND UNIT TESTS

### driver-service Tests

#### 1. Station Query Tests

**Test File:** `tests/station_queries.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_all_stations() {
        // Test getting all stations
    }

    #[test]
    fn test_get_station_by_id() {
        // Test fetching specific station
    }

    #[test]
    fn test_nearby_search_valid() {
        // Test valid nearby search
    }

    #[test]
    fn test_nearby_search_no_results() {
        // Test empty result set
    }

    #[test]
    fn test_nearby_search_invalid_coordinates() {
        // Test error handling
    }

    #[test]
    fn test_distance_calculation() {
        // Test Haversine formula accuracy
    }
}
```

---

#### 2. Distance Calculation Tests

**Test File:** `tests/distance.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_haversine_simple() {
        // Test basic distance calculation
    }

    #[test]
    fn test_haversine_zero_distance() {
        // Test zero distance case
    }

    #[test]
    fn test_haversine_same_location() {
        // Test same coordinates
    }

    #[test]
    fn test_haversine_large_distance() {
        // Test long distances
    }

    #[test]
    fn test_haversine_rounding() {
        // Test proper rounding
    }
}
```

---

#### 3. API Validation Tests

**Test File:** `tests/api_validation.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_coordinates_valid() {
        // Test valid coordinates
    }

    #[test]
    fn test_validate_coordinates_invalid_latitude() {
        // Test invalid latitude
    }

    #[test]
    fn test_validate_coordinates_invalid_longitude() {
        // Test invalid longitude
    }

    #[test]
    fn test_validate_radius_valid() {
        // Test valid radius
    }

    #[test]
    fn test_validate_radius_too_large() {
        // Test radius exceeding max
    }
}
```

---

#### 4. Error Handling Tests

**Test File:** `tests/error_handling.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_station_not_found_error() {
        // Test 404 error case
    }

    #[test]
    fn test_invalid_parameter_error() {
        // Test 400 error case
    }

    #[test]
    fn test_internal_server_error() {
        // Test 500 error case
    }
}
```

---

## 🔌 API CLIENT UNIT TESTS

### @bm/api-client Tests

#### 1. getStations() Tests

**Test File:** `@bm/api-client/__tests__/getStations.test.ts`

```typescript
describe('getStations', () => {
  it('should fetch all stations', async () => {
    // Test successful fetch
  });

  it('should return typed response', async () => {
    // Test response type
  });

  it('should handle network error', async () => {
    // Test error handling
  });

  it('should handle timeout', async () => {
    // Test timeout handling
  });
});
```

---

#### 2. getNearbyStations() Tests

**Test File:** `@bm/api-client/__tests__/getNearbyStations.test.ts`

```typescript
describe('getNearbyStations', () => {
  it('should fetch nearby stations', async () => {
    // Test successful fetch
  });

  it('should sort by distance', async () => {
    // Test sorting order
  });

  it('should filter by radius', async () => {
    // Test radius filtering
  });

  it('should accept only active stations', async () => {
    // Test status filtering
  });
});
```

---

#### 3. getStationById() Tests

**Test File:** `@bm/api-client/__tests__/getStationById.test.ts`

```typescript
describe('getStationById', () => {
  it('should fetch station by ID', async () => {
    // Test successful fetch
  });

  it('should return 404 for invalid ID', async () => {
    // Test not found case
  });

  it('should include chargers array', async () => {
    // Test response structure
  });
});
```

---

## 📱 FRONTEND UNIT TESTS

### MapContainer Tests

**Test File:** `mobile-driver/__tests__/MapContainer.native.test.tsx`

```typescript
describe('MapContainer.native', () => {
  it('should render without crashing', () => {
    // Test rendering
  });

  it('should receive markers correctly', () => {
    // Test marker props
  });

  it('should call onMarkerClick', () => {
    // Test interaction handler
  });

  it('should update region correctly', () => {
    // Test region updates
  });

  it('should handle platform differences', () => {
    // Test platform-specific behavior
  });
});
```

---

### Marker Component Tests

**Test File:** `mobile-driver/__tests__/StationMarker.test.tsx`

```typescript
describe('StationMarker', () => {
  it('should render correct number of markers', () => {
    // Test marker count
  });

  it('should highlight selected marker', () => {
    // Test selection state
  });

  it('should not duplicate renders', () => {
    // Test memoization
  });

  it('should call onPress on tap', () => {
    // Test interaction
  });
});
```

---

### Store Tests

**Test File:** `mobile-driver/__tests__/useMapStore.test.ts`

```typescript
describe('useMapStore', () => {
  it('should update selectedStationId', () => {
    // Test state update
  });

  it('should update mapCenter', () => {
    // Test position updates
  });

  it('should update radius', () => {
    // Test filter updates
  });

  it('should persist state', () => {
    // Test state persistence
  });
});
```

---

## 🧪 UTILITY TESTS

### @bm/utils Tests

#### 1. Date Formatting

**Test File:** `@bm/utils/__tests__/dateFormatting.test.ts`

```typescript
describe('dateFormatting', () => {
  it('should format date correctly', () => {
    // Test formatting
  });

  it('should handle edge cases', () => {
    // Test boundary cases
  });
});
```

---

#### 2. Distance Calculations

**Test File:** `@bm/utils/__tests__/distance.test.ts`

```typescript
describe('distance', () => {
  it('should calculate distance accurately', () => {
    // Test Haversine formula
  });

  it('should return correct units', () => {
    // Test unit conversion
  });
});
```

---

#### 3. Validation

**Test File:** `@bm/utils/__tests__/validation.test.ts`

```typescript
describe('validation', () => {
  it('should validate email correctly', () => {
    // Test email validation
  });

  it('should validate coordinates', () => {
    // Test coordinate validation
  });

  it('should validate radius', () => {
    // Test radius validation
  });
});
```

---

## 🎯 TEST COVERAGE REQUIREMENTS

### Backend (driver-service)

| Component | Target Coverage |
|-----------|----------------|
| Station queries | ≥ 80% |
| Distance calculations | ≥ 90% |
| API validation | ≥ 90% |
| Error handling | ≥ 95% |

### Frontend

| Component | Target Coverage |
|-----------|----------------|
| MapContainer | ≥ 80% |
| StationMarker | ≥ 70% |
| Store | ≥ 90% |
| API Client | ≥ 90% |
| Utilities | ≥ 95% |

---

## 🚫 ANTI-PATTERNS

**Forbidden:**
- [ ] Testing without mocking
- [ ] Testing implementation details
- [ ] Missing error case coverage
- [ ] No type checking
- [ ] Skipping edge cases

---

## 🧠 CORE PRINCIPLE

**Unit tests ensure correctness of individual components. If a component fails, its tests fail.**

---

*This document provides comprehensive unit testing guidelines for MVP-1 components.*