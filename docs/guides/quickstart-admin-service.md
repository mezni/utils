# Admin Service Quickstart Guide

## Prerequisites

- **Rust 1.70+** - Install via rustup (https://rustup.rs)
- **Docker** 24+ - Docker Desktop or Docker CLI
- **Docker Compose** - Included with Docker Desktop

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/mezni/BorneMap.git
cd BorneMap
```

### 2. Create database URL

```bash
export POSTGRES_URL=postgres://postgres:postgres@localhost:5432/ev_platform
```

### 3. Build the Docker image

```bash
docker build -t bornemap-admin-service -f services/admin-service/Dockerfile services/admin-service
```

### 4. Start PostgreSQL and Admin Service

```bash
docker compose -f infra/compose/docker-compose.yml up -d postgres

# Wait for PostgreSQL to be ready (watch logs):
docker logs -f postgres

# When you see "database system is ready to accept connections", press Ctrl+C and start the admin service:

docker compose -f infra/compose/docker-compose.yml up -d admin-service
```

### 5. Verify service is running

```bash
curl http://localhost:8080/api/v1/health
```

Expected response:
```json
{"status":"ok","service":"admin-service","db":"ok"}
```

## Running Tests

### Run all tests

```bash
cd services/admin-service
cargo test
```

### Run specific test

```bash
cargo test test_partner_crud
```

### Run with SQL query tests

```bash
cargo test --test integration_test
```

## Development Setup

### Install dev dependencies

```bash
cd services/admin-service
cargo install sqlx-cli
```

### Run with hot reload

```bash
cargo watch -x run
```

### Run tests with SQL fixtures

```bash
# Create test database
createdb ev_platform_test

# Run migrations on test database
sqlx migrate run --database-url postgresql://postgres:postgres@localhost:5432/ev_platform_test

# Run tests with test database
cargo test --database-url postgresql://postgres:postgres@localhost:24321/ev_platform_test
```

## Integration Testing

### Test health endpoint

```bash
curl http://localhost:8080/api/v1/health
# Expected: {"status":"ok","service":"admin-service","db":"ok"}
```

### Test partner CRUD

**Create partner**:
```bash
curl -X POST http://localhost:8080/api/v1/partners \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Carsharing Tunis",
    "email": "support@carsharing.tn",
    "phone": "+216 71 789 012",
    "address": "Tunis, Tunisia"
  }'
```

**Get partners**:
```bash
curl http://localhost:8080/api/v1/partners
```

**Get single partner**:
```bash
curl http://localhost:8080/api/v1/partners/PRT-001
```

**Update partner**:
```bash
curl -X PUT http://localhost:8080/api/v1/partners/PRT-001 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Tunis Power Updated",
    "email": "support@carsharing.tn",
    "phone": "+216 71 789 012",
    "address": "Updated Address, Tunis"
  }'
```

**Delete partner**:
```bash
curl -X DELETE http://localhost:8080/api/v1/partners/PRT-001
```

### Test station CRUD

**Create station**:
```bash
curl -X POST http://localhost:8080/api/v1/stations \
  -H "Content-Type: application/json" \
  -d '{
    "partner_id": "PRT-001",
    "name": "New Station",
    "latitude": 36.864702,
    "longitude": 10.158423,
    "address": "Address Line 1"
  }'
```

**Get stations**:
```bash
curl http://localhost:8080/api/v1/stations
```

### Test charger CRUD

**Create charger**:
```bash
curl -X POST http://localhost:8080/api/v1/chargers \
  -H "Content-Type: application/json" \
  -d '{
    "station_id": "STN-1a2b",
    "connector_type": "Type 2",
    "power_kw": 22.0,
    "status": "available"
  }'
```

**Get chargers**:
```bash
curl http://localhost:8080/api/v1/chargers
```

**Get single charger**:
```bash
curl http://localhost:8080/api/v1/chargers/CHR-1a2b
```

## Error Handling Examples

**Invalid partner data**:
```bash
curl -X POST http://localhost:8080/api/v1/partners \
  -H "Content-Type: application/json" \
  -d '{"name": ""}'
# Expected: 400 Bad Request
```

**Partner not found**:
```bash
curl http://localhost:8080/api/v1/partners/NOTEXIST
# Expected: 404 Not Found
```

**Duplicate email**:
```bash
curl -X POST http://localhost:8080/api/v1/partners \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Another Partner",
    "email": "contact@tunispower.tn",
    "phone": "+216 71 111 222",
    "address": "Tunis"
  }'
# Expected: 409 Conflict
```

## Database Setup

### Connect to database

```bash
docker exec -it bornemap-postgres psql -U postgres -d ev_platform
```

### Verify tables exist

```sql
\dt inventory.*
```

### View partner data

```sql
SELECT * FROM inventory.partner;
```

### View station data

```sql
SELECT * FROM inventory.station;
```

### View charger data

```sql
SELECT * FROM inventory.charger;
```

### Manually test CRUD operations

```sql
-- Insert partner
INSERT INTO inventory.partner (id, name, email, phone, address, created_at, updated_at)
VALUES ('PRT-TEST', 'Test Partner', 'test@test.com', '+216 71 111 111', 'Test Address', NOW(), NOW());

-- Insert station
INSERT INTO inventory.station (id, partner_id, name, latitude, longitude, address, created_at, updated_at)
VALUES ('STN-TEST', 'PRT-TEST', 'Test Station', 36.864702, 10.158423, 'Test Address', NOW(), NOW());

-- Insert charger
INSERT INTO inventory.charger (id, station_id, connector_type, power_kw, status, created_at, updated_at)
VALUES ('CHR-TEST', 'STN-TEST', 'Type 2', 22.0, 'available', NOW(), NOW());

-- Query results
SELECT * FROM inventory.partner WHERE id = 'PRT-TEST';
SELECT * FROM inventory.station WHERE id = 'STN-TEST';
SELECT * FROM inventory.charger WHERE id = 'CHR-TEST';

-- Cleanup
DELETE FROM inventory.charger WHERE id = 'CHR-TEST';
DELETE FROM inventory.station WHERE id = 'STN-TEST';
DELETE FROM inventory.partner WHERE id = 'PRT-TEST';
```

## Troubleshooting

### Service won't start

```bash
# Check logs
docker logs bornemap-admin-service

# Check PostgreSQL is running
docker ps | grep postgres

# Check database URL is set
echo $POSTGRES_URL
```

### Health check returns "db": "error"

```bash
# Verify PostgreSQL is accessible
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "SELECT 1;"

# Verify POSTGRES_URL includes database name
# Correct: postgresql://user:pass@host:port/database
# Incorrect: postgresql://user:pass@host:port
```

### CRUD endpoint returns 404 for existing entities

```bash
# Check that migrations were applied
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "\dt inventory.partner"
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "\dt inventory.station"
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "\dt inventory.charger"

# Verify seeded data exists
docker exec -it bornemap-postgres psql -U postgres -d ev_platform -c "
SELECT COUNT(*) FROM inventory.partner;
SELECT COUNT(*) FROM inventory.station;
SELECT COUNT(*) FROM inventory.charger;
"
```

### Foreign key violations

```bash
# Test creating station with non-existent partner
curl -X POST http://localhost:8080/api/v1/stations \
  -H "Content-Type: application/json" \
  -d '{
    "partner_id": "NONEXISTENT",
    "name": "Test Station",
    "latitude": 36.864702,
    "longitude": 10.158423,
    "address": "Test Address"
  }'
# Expected: 400 Bad Request or 404 Not Found
```

## Performance Testing

### Measure response time

```bash
# Health check
time curl http://localhost:8080/api/v1/health

# Create partner
time curl -X POST http://localhost:8080/api/v1/partners \
  -H "Content-Type: application/json" \
  -d '{"name":"Test","email":"test@test.com","phone":"+216","address":"Test"}'

# Get partners
time curl http://localhost:8080/api/v1/partners
```

### Benchmark with 100 concurrent requests

```bash
# Run Apache Bench
ab -n 100 -c 10 http://localhost:8080/api/v1/health

# Run wrk
wrk -t4 -c100 -d10s http://localhost:8080/api/v1/health
```

## Clean Up

### Stop services

```bash
docker compose -f infra/compose/docker-compose.yml down
```

### Remove volumes (complete reset)

```bash
docker compose -f infra/compose/docker-compose.yml down -v
```

## Next Steps

1. **Test all CRUD endpoints**: Health, partners, stations, chargers
2. **Run integration tests**: `cargo test`
3. **Deploy to test environment**: Build Docker image and deploy
4. **Begin Sprint 1.5**: Frontend Apps Scaffold (after Sprint 1.4 validation)

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| POSTGRES_URL | Yes | - | PostgreSQL connection URL (format: `postgresql://user:pass@host:port/database`) |
| LOG_LEVEL | No | info | Logging level (debug, info, warn, error) |
| PORT | No | 8080 | Service port |