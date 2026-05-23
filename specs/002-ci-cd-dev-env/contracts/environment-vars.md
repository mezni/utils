# Contract: Environment Variable Schema

**Path**: `.env.example`
**Consumers**: developers (local), CI pipeline, all services
**Source**: spec FR-009, research R-004

## Required variables

| Variable | Description | Local default | CI source | Secret? |
|----------|-------------|---------------|-----------|---------|
| `POSTGRES_HOST` | PostgreSQL host | `localhost` | GitHub service container | No |
| `POSTGRES_PORT` | PostgreSQL port | `5432` | GitHub service container | No |
| `POSTGRES_DB` | PostgreSQL database name | `bornemap` | GitHub service container | No |
| `POSTGRES_USER` | PostgreSQL user | `bornemap` | GitHub secret | Yes (if overridden) |
| `POSTGRES_PASSWORD` | PostgreSQL password | `bornemap_dev` | GitHub secret | Yes |
| `MONGO_HOST` | MongoDB host | `localhost` | GitHub service container | No |
| `MONGO_PORT` | MongoDB port | `27017` | GitHub service container | No |
| `MONGO_DB` | MongoDB database name | `bornemap` | GitHub service container | No |
| `RABBITMQ_HOST` | RabbitMQ host | `localhost` | GitHub service container | No |
| `RABBITMQ_PORT` | RabbitMQ port | `5672` | GitHub service container | No |
| `RABBITMQ_USER` | RabbitMQ user | `guest` | GitHub secret (if overridden) | No |
| `RABBITMQ_PASSWORD` | RabbitMQ password | `guest` | GitHub secret | Yes |
| `KEYCLOAK_URL` | Keycloak base URL | `http://localhost:8080` | GitHub variable | No |
| `KEYCLOAK_REALM` | Keycloak realm | `bornemap` | GitHub variable | No |
| `AUTH_SERVICE_PORT` | auth-service port | `3000` | GitHub variable | No |
| `CORE_SERVICE_PORT` | core-service port | `3001` | GitHub variable | No |
| `GEO_SERVICE_PORT` | geo-service port | `3002` | GitHub variable | No |
| `ANALYTICS_SERVICE_PORT` | analytics-service port | `3003` | GitHub variable | No |
| `LOG_LEVEL` | Logging level | `debug` | GitHub variable | No |

## CI behavior

1. CI workflow copies `.env.example` to `.env`.
2. Non-secret overrides are set via GitHub Actions `env:` or `vars:` context.
3. Secret overrides are injected from GitHub Actions encrypted secrets.
4. The working `.env` is never committed or cached.

## Local development

1. Developer copies `.env.example` to `.env`.
2. Overrides any values as needed (ports, passwords).
3. `.env` is listed in `.gitignore` and MUST NOT be committed.

## Non-goals

- Per-environment `.env` files (`.env.production`, `.env.staging`) — deferred to Phase 11.
- Secret rotation policy — handled outside this contract.
