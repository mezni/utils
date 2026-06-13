# Integration Tests

## Version: 1.0
## Status: Active
## Focus: MVP-1 Service Integration

---

## 🎯 TESTING OBJECTIVE

Test communication between API and database to ensure data flow works correctly.

---

## 🧱 API → DATABASE INTEGRATION

### 1. Station Endpoints Tests

**Test File:** `tests/integration/stations.rs`

```rust
#[cfg(test)]
mod integration_tests {
    use crate::handlers::stations::get_all_stations;
    use crate::db::Pool;

    #[test]
    fn test_get_all_stations_returns_data() {
        // Setup test database
        let pool = create_test_pool();

        // Execute query
        let result = get_all_stations(&pool).await;

        // Assertions
        assert!(result.is_ok());
        let stations = result.unwrap();
        assert!(!stations.is_empty());
        assert_eq!(stations[0].id, "STA-001");
    }

    #[test]
    fn test_get_all_stations_validates_schema() {
        // Test response format
        // Test field types
        // Test required fields
    }

    #[test]
    fn test_get_all_stations_handles_empty_db() {
        // Test with empty table
        // Verify empty array returned
    }
}
```

---

### 2. Nearby Search Integration Tests

**Test File:** `tests/integration/nearby.rs`

```rust
#[cfg(test)]
mod integration_tests {
    use crate::handlers::stations::get_nearby_stations;

    #[test]
    fn test_nearby_search_with_valid_coordinates() {
        // Setup: Create stations around target
        let target_lat = 36.8;
        let target_lng = 10.2;
        let radius = 5000;

        // Execute
        let result = get_nearby_stations(
            target_lat,
            target_lng,
            radius,
            &test_pool()
        ).await;

        // Assertions
        assert!(result.is_ok());
        let stations = result.unwrap();
        assert!(!stations.is_empty());

        // Verify sorting by distance
        for i in 1..stations.len() {
            assert!(stations[i].distance <= stations[i-1].distance);
        }
    }

    #[test]
    fn test_nearby_search_filters_active_only() {
        // Setup: Mix of active and inactive stations
        // Execute nearby search
        // Verify only active stations returned
    }

    #[test]
    fn test_nearby_search_distance_calculation() {
        // Test mathematical accuracy
        // Compare with known distances
    }
}
```

---

### 3. PostGIS Query Integration Tests

**Test File:** `tests/integration/postgis.rs`

```rust
#[cfg(test)]
mod integration_tests {
    use crate::handlers::stations::get_nearby_stations;

    #[test]
    fn test_postgis_distance_function() {
        // Test PostGIS distance functions
        // Verify accurate calculations
        // Test various scenarios
    }

    #[test]
    fn test_postgis_geometry_operations() {
        // Test point geometry creation
        // Test distance calculations
        // Test coordinate conversions
    }
}
```

---

### 4. Error Handling Integration Tests

**Test File:** `tests/integration/errors.rs`

```rust
#[cfg(test)]
mod integration_tests {
    use crate::handlers::stations::get_station_by_id;

    #[test]
    fn test_nonexistent_station_returns_404() {
        // Test 404 error handling
        let result = get_station_by_id("NONEXISTENT", &test_pool()).await;
        assert_eq!(result.unwrap_err().status(), 404);
    }

    #[test]
    fn test_invalid_coordinates_returns_400() {
        // Test invalid parameter handling
        let result = get_nearby_stations(999.0, -999.0, 1000, &test_pool()).await;
        assert_eq!(result.unwrap_err().status(), 400);
    }

    #[test]
    fn test_database_error_handling() {
        // Test database connection failures
        // Test query execution errors
        // Verify proper error responses
    }
}
```

---

## 🔌 FRONTEND → BACKEND INTEGRATION

### API Client Integration Tests

**Test File:** `@bm/api-client/__tests__/integration/client.test.ts`

```typescript
describe('@bm/api-client Integration Tests', () => {
  it('should successfully connect to driver-service', async () => {
    // Setup: Mock API response
    // Execute: Call getStations()
    // Verify: Response matches expected format
  });

  it('should handle API errors gracefully', async () => {
    // Setup: Mock API error
    // Execute: Call with error scenario
    // Verify: Error object returned
  });

  it('should maintain type safety', async () => {
    // Execute: Call all API functions
    // Verify: TypeScript compilation
    // Verify: Runtime type checking
  });
});
```

---

### React Query Integration Tests

**Test File:** `mobile-driver/__tests__/hooks/stations.test.ts`

```typescript
describe('useStations hook', () => {
  it('should fetch stations on mount', async () => {
    // Test: Initial fetch
    // Verify: Data loaded
  });

  it('should handle cache', async () => {
    // Test: Subsequent calls
    // Verify: No duplicate API calls
  });

  it('should handle errors', async () => {
    // Test: API failure
    // Verify: Error state
  });
});
```

---

## 🧪 DATABASE INTEGRATION TESTS

### Test Database Setup

**Test File:** `tests/integration/database.rs`

```rust
#[cfg(test)]
mod integration_tests {
    use sqlx::postgres::PgPoolOptions;

    pub fn create_test_pool() -> PgPool {
        // Setup in-memory database
        // Run migrations
        // Populate with test data
    }

    #[test]
    fn test_database_connection() {
        let pool = create_test_pool();
        let result = sqlx::query("SELECT 1").fetch_one(&pool).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_database_operations() {
        // Test insert, update, delete
        // Verify data integrity
    }
}
```

---

## 🎯 TEST COVERAGE REQUIREMENTS

### Integration Test Coverage

| Component | Target Coverage |
|-----------|----------------|
| API endpoints | ≥ 90% |
| Database queries | ≥ 85% |
| Error handling | ≥ 90% |
| Data transformation | ≥ 95% |

---

## 🚀 TEST AUTOMATION

### Rust Test Framework

```rust
// Run all tests
cargo test

// Run specific test
cargo test test_get_nearby_stations

// Run with coverage
cargo tarpaulin --out Html

// Run integration tests
cargo test --test integration_tests
```

---

## 🧠 CORE PRINCIPLE

**Integration tests ensure the pieces work together. If the API fails to query the database, the tests catch it.**

---

*This document provides comprehensive integration testing guidelines for MVP-1.*