# Quickstart: Admin Service

**Branch**: `010-admin-service` | **Port**: 8081

## Prerequisites

- Rust toolchain 1.85+ (edition 2024)
- PostgreSQL 17 + PostGIS 3.5 running (Docker: `postgis/postgis:17-3.5`)
- Database migrated and seeded (see [008-database-schema quickstart](../008-database-schema/quickstart.md) or run `sqlx migrate run` against `database/migrations/`)

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `HOST` | No | `0.0.0.0` | Bind address |
| `PORT` | No | `8081` | Listen port |
| `RUST_LOG` | No | `info` | Log level |

Example:
```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/borne_map"
export HOST="0.0.0.0"
export PORT="8081"
```

## Running

From workspace root (`source/`):

```bash
# Run directly
cargo run --package admin-service
```

## Docker

```bash
# Build
docker build -f source/services/admin-service/Dockerfile -t admin-service .

# Run
docker run -p 8081:8081 \
  -e DATABASE_URL="postgres://postgres:postgres@host.docker.internal:5432/borne_map" \
  admin-service
```

## Testing

```bash
cargo test --package admin-service
```

Requires `DATABASE_URL` pointing to a test database. Tests run `sqlx::migrate!` on setup.

## Verification

```bash
# Health check
curl http://localhost:8081/api/health
# → {"status":"ok","version":"0.1.0"}

# Create a partner
curl -X POST http://localhost:8081/api/partners \
  -H "Content-Type: application/json" \
  -d '{"name":"Test Partner","type":"business"}'
# → 201 with PartnerResponse

# List partners
curl http://localhost:8081/api/partners
# → 200 with paginated list
```

## Dev Header

For scope testing, pass `X-Partner-Id` header:
```bash
curl http://localhost:8081/api/stations \
  -H "X-Partner-Id: PRT001"
# → Only stations owned by PRT001
```
