# BorneMap — Testing Strategy

## 1. Testing Layers

### 1.1 Domain Unit Tests
- **Location:** `services/*/src/domain/`
- **Scope:** Business rules, value objects, entity invariants
- **Examples:**
  - Station coordinates validation (lat -90..90, lng -180..180)
  - Connector power_kw > 0 constraint
  - Status transitions (Available → Busy, Offline, Faulted)
  - Partner name uniqueness enforcement
- **Dependencies:** None (pure domain logic)
- **Coverage target:** 90%+ for domain logic

### 1.2 Application/Service Integration Tests
- **Location:** `services/*/tests/`
- **Scope:** Use case orchestration, repository interaction
- **Examples:**
  - Create partner → verify DB row
  - Create station with partner FK → verify trigger creates GIS row
  - Create connector → verify FK to station
  - Delete station → verify cascade deletes connectors + GIS entry
- **Dependencies:** Test PostgreSQL instance

### 1.3 API Integration Tests
- **Location:** `services/*/tests/api/`
- **Scope:** HTTP endpoint validation, request/response format
- **Examples:**
  - `POST /api/v1/partners` returns 201 with correct body
  - `POST /api/v1/partners` with missing name returns 400
  - `GET /api/v1/stations/nearby` returns 200 with geo results
  - `GET /api/v1/stations/nearby` with invalid lat returns 400
- **Dependencies:** Running service + test PostgreSQL

## 2. GIS-Specific Tests

- **Distance correctness:**
  - Create station at known coordinates
  - Query `gis.nearby_stations()` from reference point
  - Assert distance matches haversine calculation (within 1m tolerance)
- **Spatial index performance:**
  - Verify `EXPLAIN ANALYZE` shows GiST index scan
- **Trigger correctness:**
  - INSERT station → verify `gis.station_locations` row created
  - UPDATE coordinates → verify `gis.station_locations` updated
  - DELETE station → verify `gis.station_locations` cascade deleted

## 3. Test Configuration

```toml
# Each service's Cargo.toml
[dev-dependencies]
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
tokio = { version = "1", features = ["full"] }
serde_json = "1"
reqwest = { version = "0.12", features = ["json"] }
```

## 4. Test Database Setup

```bash
# Create test database
createdb bornemap_test

# Run migrations
sqlx migrate run --database-url postgres://localhost/bornemap_test
```

## 5. Running Tests

```bash
# All tests
cargo test --workspace

# Domain tests only
cargo test --lib

# Integration tests (require DB)
cargo test --test '*'

# With database URL
DATABASE_URL=postgres://localhost/bornemap_test cargo test
```
