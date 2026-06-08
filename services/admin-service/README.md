# Admin Service

A Rust service that provides REST API endpoints for administrative operations: health check and CRUD operations for partners, stations, and chargers.

## Overview

- **Framework**: Rust 1.70+ with Actix-web 4.0 async framework
- **Database**: PostgreSQL 16 + PostGIS 3.4 (from Sprint 1.1 Docker Compose)
- **Testing**: Integration tests with postgres test container
- **Containerization**: Docker with multi-stage Rust build
- **Endpoints**:
  - `GET /api/v1/health` - Service and database status verification
  - `POST /api/v1/partners` - Create new partner
  - `GET /api/v1/partners` - List all partners
  - `GET /api/v1/partners/:id` - Get partner by ID
  - `PUT /api/v1/partners/:id` - Update partner
  - `DELETE /api/v1/partners/:id` - Delete partner
  - `POST /api/v1/stations` - Create new station
  - `GET /api/v1/stations` - List all stations
  - `GET /api/v1/stations/:id` - Get station by ID
  - `PUT /api/v1/stations/:id` - Update station
  - `DELETE /api/v1/stations/:id` - Delete station
  - `POST /api/v1/chargers` - Create new charger
  - `GET /api/v1/chargers` - List all chargers
  - `GET /api/v1/chargers/:id` - Get charger by ID
  - `PUT /api/v1/chargers/:id` - Update charger
  - `DELETE /api/v1/chargers/:id` - Delete charger

## Key Features

### Health Check
- Returns service status (`"service":"admin-service"`) and database connection status (`"db":"ok"`)
- Verifies database connectivity with simple query before accepting requests
- Returns 500 error when database connection fails
- Returns 503 error when service not running

### Partner CRUD
- Create, Read, Update, Delete partners with validation
- Email must be unique (returns 409 Conflict for duplicates)
- Returns 404 when partner not found
- Returns 400 for invalid data

### Station CRUD
- Create, Read, Update, Delete stations with FK validation
- Validates partner_id exists before creating station
- Returns 404 when station not found
- Returns 400 for invalid data or invalid partner_id
- Returns 400 for FK violations

### Charger CRUD
- Create, Read, Update, Delete chargers with FK validation
- Validates station_id exists before creating charger
- Returns 404 when charger not found
- Returns 400 for invalid data or invalid station_id
- Returns 400 for FK violations

### Infrastructure
- **Rust 1.70+** with **Actix-web 4.0** async framework
- **PostgreSQL 16 + PostGIS 3.4** database (from Sprint 1.1 Docker Compose)
- **Database connection pooling** via `ev-db` shared crate
- **Automatic migration application** on startup
- **Multi-stage Dockerfile** for production builds (builder + runner)
- **Integration tests** with 10 test scenarios covering valid/invalid cases
- **Input validation** with proper error messages
- **Foreign key constraints** for data integrity

## Tests

### Integration Tests (10 tests)
- ✅ Health endpoint returns 200 with correct JSON
- ✅ Health endpoint returns 500 when database connection fails
- ✅ Partner CRUD operations (create, get, list, update, delete)
- ✅ Station CRUD operations with FK validation
- ✅ Charger CRUD operations with FK validation
- ✅ Partner CRUD with duplicate email validation (409 conflict)
- ✅ Entity not found validation (404) for all entities
- ✅ Invalid parameter validation (400 Bad Request) for all endpoints

### Performance Targets
- Health check: < 50ms
- CRUD operations: < 200ms

## Configuration

| Environment Variable | Required | Default | Description |
|---------------------|----------|---------|-------------|
| POSTGRES_URL | Yes | - | PostgreSQL connection URL (format: `postgresql://user:pass@host:port/database`) |
| LOG_LEVEL | No | info | Logging level (debug, info, warn, error) |
| PORT | No | 8080 | Service port |

## API Documentation

### Health Check

**Endpoint**: `GET /api/v1/health`

**Response (200 OK)**:
```json
{
  "status": "ok",
  "service": "admin-service",
  "db": "ok"
}
```

### Partner CRUD

**Create Partner**: `POST /api/v1/partners`
**Get Partner**: `GET /api/v1/partners/:id`
**List Partners**: `GET /api/v1/partners`
**Update Partner**: `PUT /api/v1/partners/:id`
**Delete Partner**: `DELETE /api/v1/partners/:id`

**Request Body**:
```json
{
  "name": "Tunis Power",
  "email": "contact@tunispower.tn",
  "phone": "+216 71 123 456",
  "address": "Tunis, Tunisia"
}
```

**Response (201 Created)**:
```json
{
  "id": "PRT-001",
  "name": "Tunis Power",
  "email": "contact@tunispower.tn",
  "phone": "+216 71 123 456",
  "address": "Tunis, Tunisia"
}
```

**Response (409 Conflict)**:
```json
{
  "error": "Invalid data: Partner with this email already exists"
}
```

## Database Schema

The service reads from the following tables (from Sprint 1.2):

### Inventory Schema

- `partner` - Partner information
- `station` - Station information (id, partner_id, name, latitude, longitude, address, etc.)
- `charger` - Charger information (id, station_id, connector_type, power_kw, status)

### GIS Schema

- `station_locations` - Spatial location data with GiST index for fast spatial queries

## Project Structure

```
services/admin-service/
├── Cargo.toml           # Rust project configuration
├── src/
│   ├── main.rs          # Entry point, migration application
│   ├── lib.rs           # Library entry point, public API
│   ├── config.rs        # Configuration struct (Postgres URL)
│   ├── db.rs            # Database connection pool
│   ├── routes.rs        # API routes (health, partner CRUD, station CRUD, charger CRUD)
│   ├── handlers.rs      # Request handlers
│   ├── error.rs         # Error types and responses
│   └── models.rs        # Request/response schemas
├── tests/
│   ├── integration_test.rs  # Integration tests
│   └── sql/
│       └── test_admin_crud.sql  # Test fixtures
├── migrations/          # SQL migrations
├── Dockerfile           # Production Docker image
└── Dockerfile.dev       # Development Docker image
```

## Quick Start

### Build Docker image

```bash
docker build -t bornemap-admin-service -f Dockerfile .
```

### Start PostgreSQL

```bash
docker compose -f infra/compose/docker-compose.yml up -d postgres
```

### Run admin service

```bash
docker compose -f infra/compose/docker-compose.yml up -d admin-service
```

### Test health endpoint

```bash
curl http://localhost:8080/api/v1/health
# Expected: {"status":"ok","service":"admin-service","db":"ok"}
```

### Test partner CRUD

```bash
# Create partner
curl -X POST http://localhost:8080/api/v1/partners \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Carsharing Tunis",
    "email": "support@carsharing.tn",
    "phone": "+216 71 789 012",
    "address": "Tunis, Tunisia"
  }'

# Get partners
curl http://localhost:8080/api/v1/partners
```

### Run tests

```bash
cargo test
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

## Future Enhancements

- **Authentication**: OAuth2/JWT (Sprint 2.x)
- **Pagination**: For all CRUD endpoints (Sprint 1.5+)
- **Filters**: By partner_id, status, etc. (Sprint 2.x)
- **Station location auto-creation**: Automatic GIS sync when station created (Sprint 1.5+)
- **Detailed error messages with codes**: VALIDATION_ERROR, DUPLICATE_ENTITY, etc. (Sprint 2.x)
- **Rate limiting**: Header-based (Sprint 2.x)
- **Bulk operations**: Batch create/update endpoints (Sprint 2.x)
- **Audit logging**: Created_at, updated_at, updated_by fields (Sprint 2.x)