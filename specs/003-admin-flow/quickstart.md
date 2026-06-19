# Quickstart: Admin Service Core Operations

## Overview

This guide helps you get started with developing the Admin Service (Sprint 2 of MVP-1). The Admin Service handles CRUD operations for partners, stations, and chargers with transactional integrity, cache busting, and comprehensive audit logging.

---

## Prerequisites

Before starting development, ensure you have:

1. **Rust 1.88+**: Required for Actix-web 4.x compatibility
2. **PostgreSQL 16 with PostGIS**: Installed and running
3. **Redis**: Installed and running
4. **Docker & Docker Compose**: For running the full infrastructure stack
5. **Node.js & npm**: For running tests and development tools

---

## Project Setup

### 1. Clone the Repository

```bash
git clone https://github.com/mezni/BorneMap.git
cd BorneMap
```

### 2. Install Dependencies

```bash
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install Node.js (if not already installed)
nvm install 20
nvm use 20

# Install cargo-nextest (optional, for faster testing)
cargo install cargo-nextest
```

### 3. Setup Environment Variables

Copy the example environment file:

```bash
cp source/services/admin-service/.env.example source/services/admin-service/.env
```

Edit `source/services/admin-service/.env` with your configuration:

```env
# Database configuration
DATABASE_URL=postgresql://auth_user:your_password@localhost:5432/platform_db

# Redis configuration
REDIS_URL=redis://localhost:6379

# Keycloak configuration (for JWT validation)
KEYCLOAK_URL=http://localhost:8080
KEYCLOAK_CLIENT_ID=admin-dashboard

# Application configuration
PORT=3002
RUST_LOG=auth_service=info,sqlx=info
```

### 4. Build the Admin Service

```bash
cd source/services/admin-service
cargo build --release
```

### 5. Run Tests

```bash
# Run all tests
cargo test

# Run only unit tests
cargo test --lib

# Run integration tests
cargo test --test '*'

# Run tests with output
cargo test -- --nocapture
```

---

## Running the Service

### Development Mode

```bash
cd source/services/admin-service

# Run in development mode (with auto-reload)
cargo run

# Or run in release mode
cargo run --release
```

### Production Mode (Docker)

```bash
cd source/infra
docker compose up -d admin-service
```

### Verify Service is Running

```bash
# Check health endpoint
curl http://localhost:3002/health

# Expected response:
# {
#   "status": "healthy",
#   "service": "admin-service"
# }
```

---

## Database Setup

### 1. PostgreSQL Schemas

The Admin Service uses the `inventory` schema (for partners, stations, chargers) and `analytics_db` (for audit logs).

**Existing Setup** (from Sprint 0):
- `inventory` schema: partners, stations, chargers tables, lookup tables, materialized views
- `analytics_db`: audit_log table
- PostgreSQL roles: `admin_service_role`, `admin_analytics_role`

**Check Schemas**:
```sql
\c platform_db
\dt inventory.*
\dt analytics_db.*

\c analytics_db
\dt audit_log
```

### 2. Create Partner (Example)

```sql
INSERT INTO inventory.partners (
    id, name, network_type, is_verified,
    created_at, updated_at
) VALUES (
    'OPR-a1b2c3d4e5f6g7h8i9j0',
    'Partner Alpha',
    'COMPANY',
    false,
    NOW(),
    NOW()
);
```

### 3. Create Station (Example)

```sql
INSERT INTO inventory.stations (
    id, partner_id, name, address, location,
    created_at, updated_at
) VALUES (
    'STA-a1b2c3d4e5f6g7h8i9j0',
    'OPR-a1b2c3d4e5f6g7h8i9j0',
    'Tunis Central Station',
    '12 Rue de la Liberté, Tunis, Tunisia',
    ST_SetSRID(ST_MakePoint(10.1816, 36.8065), 4326),
    NOW(),
    NOW()
);
```

---

## Testing the Endpoints

### 1. Get a Bearer Token

First, authenticate with Auth Service:

```bash
curl -X POST http://localhost:3000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "admin@bornemap.tn",
    "password": "test123"
  }'
```

Save the `access_token` from the response.

### 2. Create a Partner

```bash
curl -X POST http://localhost:3002/api/v1/admin/partner \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "name": "Partner Beta",
    "network_type": "COMPANY",
    "support_phone": "+216 71 123 456",
    "support_email": "contact@partner-beta.tn"
  }'
```

**Expected Response**: 201 Created

### 3. Create a Station

```bash
curl -X POST http://localhost:3002/api/v1/admin/station \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "partner_id": "OPR-a1b2c3d4e5f6g7h8i9j0",
    "name": "Sousse Central Station",
    "address": "45 Avenue Habib Bourguiba, Sousse, Tunisia",
    "location": {
      "type": "Point",
      "coordinates": [10.6084, 35.8256]
    }
  }'
```

**Expected Response**: 201 Created

### 4. Create a Charger

```bash
curl -X POST http://localhost:3002/api/v1/admin/charger \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "station_id": "STA-a1b2c3d4e5f6g7h8i9j0",
    "connector_type_id": 1,
    "status_id": 1,
    "current_type_id": 2,
    "power_kw": 50.0,
    "voltage": 480,
    "amperage": 100,
    "count_available": 1,
    "count_total": 1
  }'
```

**Expected Response**: 201 Created

### 5. Update a Station

```bash
curl -X PUT http://localhost:3002/api/v1/admin/station/STA-a1b2c3d4e5f6g7h8i9j0 \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "name": "Sousse Central Station Updated",
    "address": "46 Avenue Habib Bourguiba, Sousse, Tunisia"
  }'
```

**Expected Response**: 200 OK

### 6. Get an Entity

```bash
# Get partner
curl http://localhost:3002/api/v1/admin/partner/OPR-a1b2c3d4e5f6g7h8i9j0 \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

# Get station
curl http://localhost:3002/api/v1/admin/station/STA-a1b2c3d4e5f6g7h8i9j0 \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"

# Get charger
curl http://localhost:3002/api/v1/admin/charger/CHG-a1b2c3d4e5f6g7h8i9j0 \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Expected Response**: 200 OK

---

## Common Tasks

### 1. Audit Log Query

Check audit log entries:

```sql
\c analytics_db

SELECT
    actor_id,
    action,
    target_type,
    target_id,
    TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') as created_at,
    BEFORE_SNAPSHOT,
    AFTER_SNAPSHOT
FROM audit_log
ORDER BY created_at DESC
LIMIT 20;
```

### 2. Refresh Materialized View

Manually refresh materialized views (for testing):

```sql
\c platform_db

-- Refresh station summaries
REFRESH MATERIALIZED VIEW CONCURRENTLY inventory.mv_stations_summary;

-- Refresh geospatial station summaries
REFRESH MATERIALIZED VIEW CONCURRENTLY inventory.mv_stations_geo;
```

### 3. Check Redis Cache

Check Redis keys:

```bash
# List all stations tile cache keys
redis-cli KEYS "stations:tile:*"

# Get idempotency keys
redis-cli KEYS "idempotency:*"

# Check idempotency key
redis-cli GET "idempotency:a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# Delete idempotency key (for testing)
redis-cli DEL "idempotency:a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

### 4. Clear All Audit Logs

```sql
\c analytics_db

DELETE FROM audit_log;
```

---

## Development Workflow

### 1. Make Changes

```bash
cd source/services/admin-service

# Edit code in src/
# Example: src/routes/partner.rs
vim src/routes/partner.rs
```

### 2. Run Linter

```bash
cargo clippy -- -D warnings
```

### 3. Run Tests

```bash
cargo test
```

### 4. Build

```bash
cargo build --release
```

### 5. Restart Service

```bash
# Stop current service
pkill -f admin-service

# Run new version
cargo run --release
```

---

## Troubleshooting

### Issue 1: Database Connection Failed

**Error**: `Failed to connect to database`

**Solution**:
```bash
# Check PostgreSQL is running
docker compose ps postgres

# Check DATABASE_URL in .env
cat source/services/admin-service/.env | grep DATABASE_URL

# Test database connection
psql $DATABASE_URL -c "SELECT 1"
```

### Issue 2: Redis Connection Failed

**Error**: `Redis connection error`

**Solution**:
```bash
# Check Redis is running
docker compose ps redis

# Check Redis URL in .env
cat source/services/admin-service/.env | grep REDIS_URL

# Test Redis connection
redis-cli ping
```

### Issue 3: Keycloak Not Accessible

**Error**: `Failed to extract claims from Keycloak`

**Solution**:
```bash
# Check Keycloak is running
docker compose ps keycloak

# Check Keycloak URL in .env
cat source/services/admin-service/.env | grep KEYCLOAK_URL

# Test Keycloak connection
curl http://localhost:8080/realms/bornemap
```

### Issue 4: Materialized View Refresh Fails

**Error**: `failed to refresh materialized view`

**Solution**:
```bash
# Check materialized views exist
\c platform_db
\d inventory.mv_stations_summary

# Refresh manually (requires lock on write queries)
REFRESH MATERIALIZED VIEW inventory.mv_stations_summary;

# Refresh concurrently (recommended for production)
REFRESH MATERIALIZED VIEW CONCURRENTLY inventory.mv_stations_summary;
```

### Issue 5: Entity Not Found

**Error**: `404 Not Found`

**Solution**:
```sql
# Check if entity exists
\c platform_db

-- Check partner
SELECT * FROM inventory.partners WHERE id = 'OPR-xxxxxxxxxxxxxxxxxxxxxx';

-- Check station
SELECT * FROM inventory.stations WHERE id = 'STA-xxxxxxxxxxxxxxxxxxxxxx';

-- Check if deleted_at is set
SELECT id, deleted_at FROM inventory.partners WHERE id = 'OPR-xxxxxxxxxxxxxxxxxxxxxx';
```

---

## Performance Testing

### Test Cache Bust Overhead

```bash
# Enable SQL logging
export RUST_LOG=auth_service=debug,sqlx=trace

# Run a mutation and measure time
time curl -X POST http://localhost:3002/api/v1/admin/partner \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "name": "Performance Test Partner",
    "network_type": "COMPANY"
  }'

# Expected: Response time < 500ms (including cache bust)
```

### Test Idempotency

```bash
# First request
curl -X POST http://localhost:3002/api/v1/admin/partner \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Idempotency-Key: perf-test-key" \
  -d '{
    "name": "Test Partner",
    "network_type": "COMPANY"
  }'

# Second request (should replay)
curl -X POST http://localhost:3002/api/v1/admin/partner \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Idempotency-Key: perf-test-key" \
  -d '{
    "name": "Test Partner",
    "network_type": "COMPANY"
  }'

# Check Idempotency-Replayed header
# Should be: Idempotency-Replayed: true
```

---

## Additional Resources

### Documentation
- [Feature Specification](./spec.md)
- [Implementation Plan](./plan.md)
- [Data Model](./data-model.md)
- [API Contracts](./contracts/api-contracts.md)
- [Error Contracts](./contracts/error-contracts.md)
- [Research Findings](./research.md)

### Constitution
- [BorneMap Constitution](../../.specify/memory/constitution.md)

### Development Tools
- [Actix-web Documentation](https://actix.rs/docs/)
- [sqlx Documentation](https://docs.rs/sqlx/)
- [PostGIS Documentation](https://postgis.net/documentation/)

---

## Next Steps

1. Review the [Feature Specification](./spec.md) for detailed requirements
2. Review the [Implementation Plan](./plan.md) for architecture decisions
3. Read the [Research Findings](./research.md) for technical details
4. Implement CRUD endpoints following the [API Contracts](./contracts/api-contracts.md)
5. Write tests following the [Error Contracts](./contracts/error-contracts.md)
6. Deploy to Docker and test end-to-end
