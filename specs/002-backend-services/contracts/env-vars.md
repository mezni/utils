# Environment Variables Contract: Backend Services

## Driver-Service (:8080)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| HOST | No | `0.0.0.0` | Bind address |
| PORT | No | `8080` | HTTP port |
| PLATFORM_DB_URL | Yes | — | Full PostgreSQL connection string for platform_db |
| RUST_LOG | No | `info` | Tracing log level |

Example `.env` for driver-service:
```env
HOST=0.0.0.0
PORT=8080
PLATFORM_DB_URL=postgresql://borneadmin:borne_dev_2026@localhost:5432/platform_db
RUST_LOG=info
```

## Admin-Service (:8081)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| HOST | No | `0.0.0.0` | Bind address |
| PORT | No | `8081` | HTTP port |
| PLATFORM_DB_URL | Yes | — | Full PostgreSQL connection string for platform_db |
| ANALYTICS_DB_URL | Yes | — | Full PostgreSQL connection string for analytics_db |
| RUST_LOG | No | `info` | Tracing log level |

Example `.env` for admin-service:
```env
HOST=0.0.0.0
PORT=8081
PLATFORM_DB_URL=postgresql://borneadmin:borne_dev_2026@localhost:5432/platform_db
ANALYTICS_DB_URL=postgresql://borneadmin:borne_dev_2026@localhost:5433/analytics_db
RUST_LOG=info
```

## Test Environment

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| TEST_DB_HOST | No | `localhost` | Test database host |
| TEST_DB_PORT | No | `5434` | Test database port (separate from dev DBs) |
| TEST_DB_USER | No | `borneadmin` | Test database user |
| TEST_DB_PASSWORD | No | `borne_dev_2026` | Test database password |
| TEST_DB_NAME | No | `bornemap_test` | Test database name |
| TEST_PLATFORM_DB_URL | No | Built from above | Override full connection string |

## Docker Environment

In Docker Compose, database hosts use container names:
- `platform-db:5432` for platform_db
- `analytics-db:5433` for analytics_db
