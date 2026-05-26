# Quickstart: Backend Core — Schema, Identity & CRUD

## Prerequisites

- Docker & Docker Compose
- Rust 1.78+ (via rustup)
- sqlx-cli (`cargo install sqlx-cli --no-default-features --features postgres`)

## 1. Start the Database

```bash
docker compose -f docker-compose.dev.yml up postgres -d
```

Wait for the health check to pass:
```bash
docker compose -f docker-compose.dev.yml logs postgres --tail 5
```

## 2. Run Migrations

```bash
export DATABASE_URL=postgres://bornemap_admin:development_secret_key@localhost:5432/bornemap_dev
sqlx migrate run --source sources/backend/migrations
```

This creates all tables, enums, indexes, and loads seed data.

## 3. Start the Backend

```bash
cd sources/backend
cargo run
```

The server starts on `http://localhost:8080`.

## 4. Verify Health

```bash
curl http://localhost:8080/api/v1/health
```

Expected:
```json
{"status":"ok","service":"bornemap-backend"}
```

## 5. Register a User

```bash
curl -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "username": "testuser",
    "password": "securepass123"
  }'
```

Save the `token` from the response.

## 6. Authenticate Requests

```bash
export TOKEN="eyJhbGciOiJIUzI1NiIs..."
curl http://localhost:8080/api/v1/stations \
  -H "Authorization: Bearer $TOKEN"
```

## 7. CRUD Examples

### Create a Station

```bash
curl -X POST http://localhost:8080/api/v1/stations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "owner_id": "USR-...",
    "name": "Test Station",
    "address": "1 Test Street",
    "city": "Tunis",
    "longitude": 10.1815,
    "latitude": 36.8065
  }'
```

### List Stations (Paginated)

```bash
curl "http://localhost:8080/api/v1/stations?limit=10" \
  -H "Authorization: Bearer $TOKEN"
```

### Update a Station

```bash
curl -X PATCH http://localhost:8080/api/v1/stations/STN-... \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "name": "Updated Station",
    "updated_at": "2026-05-26T10:00:00.123456Z"
  }'
```

### Delete a Station

```bash
curl -X DELETE http://localhost:8080/api/v1/stations/STN-... \
  -H "Authorization: Bearer $TOKEN"
```

## 8. Run Tests

```bash
cd sources/backend
cargo test
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (required) | PostgreSQL connection string |
| `JWT_SECRET` | (required) | HS256 signing key (min 32 chars) |
| `RUST_LOG` | `info` | Log level filter |

## Seed Data

The seed migration creates test records with `is_test = true`:

| Entity | Count |
|--------|-------|
| Connector Types | 2 |
| Admin User | 1 |
| Partner Users | 5 |
| Partner Profiles | 5 |
| Stations | 100 |
| Chargers | 300 |

To include test records in queries, add `?include_test=true`.
