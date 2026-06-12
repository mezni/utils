# Environment Variable Contract

## Database Connection Strings

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| PLATFORM_DB_URL | postgresql://borneadmin:borne_dev_2026@localhost:5432/platform_db | Yes | platform_db connection string |
| ANALYTICS_DB_URL | postgresql://borneadmin:borne_dev_2026@localhost:5433/analytics_db | Yes | analytics_db connection string |

## Database Credentials

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| PLATFORM_DB_USER | borneadmin | Yes | platform_db username |
| PLATFORM_DB_PASSWORD | borne_dev_2026 | Yes | platform_db password |
| ANALYTICS_DB_USER | borneadmin | Yes | analytics_db username |
| ANALYTICS_DB_PASSWORD | borne_dev_2026 | Yes | analytics_db password |

## Service Ports

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| PLATFORM_DB_PORT | 5432 | Yes | platform_db host port |
| ANALYTICS_DB_PORT | 5433 | Yes | analytics_db host port |

## PostGIS

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| POSTGIS_VERSION | 3.3 | Yes | PostGIS extension version |

## Logging

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| DB_LOG_LEVEL | notice | No | PostgreSQL log level (notice, warning, error) |
