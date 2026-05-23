# Quickstart: Core Service Implementation

**Feature**: [spec.md](./spec.md)
**Plan**: [plan.md](./plan.md)
**Date**: 2026-05-23

## Purpose

This quickstart guide provides step-by-step instructions to set up, run, and verify the core service implementation. Follow these steps to get the core service running locally.

## Prerequisites

Before starting, ensure you have:

1. **Docker and Docker Compose** installed
2. **Rust 1.75+** installed (for local development)
3. **Git** installed
4. **Phase 1 CI/CD & Dev Environment** completed (from `specs/002-ci-cd-dev-env`)

## Step 1: Checkout the Code

```bash
# Clone the repository
git clone https://github.com/mezni/BorneMap.git
cd BorneMap

# Switch to the core-service feature branch
git checkout 003-core-service-implementation
```

## Step 2: Set Up Environment Variables

```bash
# Copy the environment template
cp .env.example .env

# Edit the environment variables
nano .env
```

Ensure these variables are set correctly:

```bash
# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=bornemap
POSTGRES_USER=bornemap
POSTGRES_PASSWORD=bornemap_dev

# RabbitMQ
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest

# Keycloak
KEYCLOAK_URL=http://localhost:8080
KEYCLOAK_REALM=bornemap

# Services
AUTH_SERVICE_PORT=3000
CORE_SERVICE_PORT=3001
GEO_SERVICE_PORT=3002
ANALYTICS_SERVICE_PORT=3003

# Logging
LOG_LEVEL=debug
```

## Step 3: Start Infrastructure Services

```bash
# Start PostgreSQL, MongoDB, RabbitMQ, and Keycloak
make up
```

Wait for all services to be healthy:

```bash
# Check service status
make logs
```

You should see all services running without errors.

## Step 4: Initialize the Database

```bash
# Run database migrations
cd services/core-service
cargo install sqlx-cli
sqlx migrate run

# Seed the database with test data
cargo run --bin seed
```

## Step 5: Start the Core Service

```bash
# Start the core service in development mode
cargo run
```

The service should start successfully and you should see output like:

```
   Compiling core-service v0.1.0 (/home/dali/WORK/BorneMap/services/core-service)
    Finished dev [unoptimized + debuginfo] target(s) in 12.45s
     Running `target/debug/core-service`
2026-05-23T10:00:00.123456Z  INFO core_service::main: Starting core service...
2026-05-23T10:00:00.234567Z  INFO core_service::database: Database connection pool initialized (10 min, 20 max)
2026-05-23T10:00:00.345678Z  INFO core_service::server: Server listening on http://0.0.0.0:3001
2026-05-23T10:00:00.456789Z  INFO core_service::outbox: Outbox relay worker started
2026-05-23T10:00:00.567890Z  INFO core_service::main: Core service started successfully
```

## Step 6: Verify Service Health

```bash
# Check the health endpoint
curl http://localhost:3001/health/core-service
```

Expected response:

```json
{
  "status": "healthy",
  "timestamp": "2026-05-23T10:00:00Z",
  "version": "1.0.0",
  "database": "healthy",
  "details": {
    "database": {
      "status": "healthy",
      "response_time_ms": 12
    }
  }
}
```

## Step 7: Verify API Access

### Get Authentication Token

First, get a JWT token from Keycloak:

```bash
# Get admin token (replace with your credentials)
curl -X POST http://localhost:8080/realms/bornemap/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password&username=admin&password=admin&client_id=boremap-client"
```

Save the `access_token` from the response.

### Test API Endpoints

```bash
# Set your JWT token
export JWT_TOKEN="your-access-token-here"

# Test companies endpoint
curl -X GET http://localhost:3001/api/core/v1/companies \
  -H "Authorization: Bearer $JWT_TOKEN"
```

Expected response:

```json
{
  "data": [
    {
      "id": "CMP-abc123def",
      "name": "Tunisia EV Charging",
      "description": "Leading EV charging network in Tunisia",
      "email": "contact@tunisiaev.tn",
      "is_active": true,
      "created_at": "2026-05-23T10:00:00Z",
      "updated_at": "2026-05-23T10:00:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 1,
    "pages": 1
  }
}
```

### Test Creating a Company

```bash
# Create a new company
curl -X POST http://localhost:3001/api/core/v1/companies \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Test EV Company",
    "description": "Test company for verification",
    "email": "test@testev.tn",
    "phone": "+216-71-123-456",
    "website": "https://testev.tn",
    "address": "Test Address, Tunisia"
  }'
```

Expected response (201 Created):

```json
{
  "id": "CMP-test123def",
  "name": "Test EV Company",
  "description": "Test company for verification",
  "email": "test@testev.tn",
  "phone": "+216-71-123-456",
  "website": "https://testev.tn",
  "address": "Test Address, Tunisia",
  "is_active": true,
  "created_at": "2026-05-23T10:01:00Z",
  "updated_at": "2026-05-23T10:01:00Z"
}
```

## Step 8: Verify OpenAPI Documentation

```bash
# Access OpenAPI JSON specification
curl http://localhost:3001/api/core/v1/api-json | jq '.info.title'
```

Expected output: `Core Service API`

```bash
# Access Swagger UI in your browser
open http://localhost:3001/api/core/v1/docs
```

## Step 9: Verify Event Publishing

```bash
# Create a station (this should publish events)
curl -X POST http://localhost:3001/api/core/v1/stations \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "company_id": "CMP-abc123def",
    "name": "Test Station",
    "description": "Test station for verification",
    "address": "Test Address, Tunisia",
    "latitude": 36.8065,
    "longitude": 10.1815,
    "access_type": "public"
  }'
```

Check the outbox table in PostgreSQL:

```bash
# Connect to PostgreSQL
docker exec -it bornemap-postgres-1 psql -U bornemap -d bornemap

# Check outbox table
SELECT event_type, aggregate_type, status, created_at FROM outbox ORDER BY created_at DESC LIMIT 5;
```

You should see the `StationCreated` event with status `pending` or `published`.

## Step 10: Run Tests

```bash
# Run all tests
cd services/core-service
cargo test

# Run integration tests
cargo test --test integration

# Run e2e tests
cargo test --test e2e
```

All tests should pass.

## Step 11: Verify NGINX Routing

```bash
# Test routing through NGINX gateway
curl -X GET http://localhost/api/core/v1/companies \
  -H "Authorization: Bearer $JWT_TOKEN"
```

This should return the same result as the direct service call, verifying that NGINX is correctly routing requests.

## Step 12: Verify Metrics

```bash
# Access metrics endpoint
curl http://localhost:3001/metrics/core-service
```

You should see Prometheus-compatible metrics including:
- HTTP request counts and durations
- Database connection pool metrics
- Application metrics

## Troubleshooting

### Service Won't Start

1. Check if PostgreSQL is running:
   ```bash
   docker ps | grep postgres
   ```

2. Check database connection:
   ```bash
   docker exec -it bornemap-postgres-1 psql -U bornemap -d bornemap -c "SELECT 1;"
   ```

3. Check environment variables:
   ```bash
   cat .env
   ```

### JWT Authentication Issues

1. Verify Keycloak is running:
   ```bash
   curl http://localhost:8080
   ```

2. Check Keycloak realm:
   ```bash
   curl http://localhost:8080/realms/bornemap
   ```

3. Verify token format:
   ```bash
   echo $JWT_TOKEN | jq -R 'split(".") | .[1] | @base64d | fromjson'
   ```

### Database Connection Issues

1. Check PostgreSQL logs:
   ```bash
   docker logs bornemap-postgres-1
   ```

2. Verify connection parameters:
   ```bash
   docker exec -it bornemap-core-service-1 env | grep POSTGRES
   ```

### Event Publishing Issues

1. Check RabbitMQ is running:
   ```bash
   docker ps | grep rabbitmq
   ```

2. Check RabbitMQ management UI:
   ```bash
   open http://localhost:15672
   ```

3. Verify outbox table:
   ```sql
   SELECT * FROM outbox WHERE status = 'failed';
   ```

## Next Steps

After successfully completing this quickstart:

1. **Review the API documentation** at `http://localhost:3001/api/core/v1/docs`
2. **Explore the data model** in `data-model.md`
3. **Understand the event contracts** in `contracts/events.md`
4. **Run the full test suite** to verify all functionality
5. **Start implementing additional features** following the established patterns

## Verification Checklist

- [ ] Infrastructure services (PostgreSQL, RabbitMQ, Keycloak) are running
- [ ] Core service starts without errors
- [ ] Health endpoint returns healthy status
- [ ] Can authenticate with JWT tokens
- [ ] Can access API endpoints with proper authentication
- [ ] Can create, read, update, and delete entities
- [ ] OpenAPI documentation is accessible
- [ ] Events are published to the outbox table
- [ ] All tests pass
- [ ] NGINX gateway routes requests correctly
- [ ] Metrics endpoint is accessible

If all items in this checklist are verified, the core service implementation is working correctly.