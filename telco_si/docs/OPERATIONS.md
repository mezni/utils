# Operations

## Containerized Development Environment

The environment is fully repeatable using Docker Compose with two services:

| Service | Purpose |
| --- | --- |
| `db` | PostgreSQL instance with six isolated application schemas. |
| `app` | The Telco SI API plus migration and seeding tooling. |

## Quick Start

```bash
# Build and start the environment
docker compose up -d

# Run multi-schema migrations
docker compose exec app alembic upgrade head

# Seed synthetic data (see docs/SEEDING.md)
docker compose exec app python -m telco_si.seed --size demo

# Run the API
docker compose exec app uvicorn telco_si.main:app --host 0.0.0.0 --port 8000
```

The API is available at `http://localhost:8000` with interactive docs at `/docs`.

## Common Commands

### Migrations

Alembic performs **automated multi-schema introspection and version tracking** across all application schemas.

```bash
# Apply all migrations
docker compose exec app alembic upgrade head

# Roll back one revision
docker compose exec app alembic downgrade -1

# Autogenerate a new revision after model changes
docker compose exec app alembic revision --autogenerate -m "description"

# Show current revision
docker compose exec app alembic current
```

### Database Shell

```bash
docker compose exec db psql -U telco -d telco
```

## Environment Configuration

Configuration is driven by environment variables (see `pyproject.toml` / Docker Compose defaults):

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://telco:telco@db:5432/telco` | Database connection string. |
| `API_HOST` | `0.0.0.0` | API bind host. |
| `API_PORT` | `8000` | API bind port. |

## Verification / Smoke Test

```bash
# Confirm service is healthy and schemas exist
docker compose exec db psql -U telco -d telco -c '\dn'

# Confirm seeded data per schema
docker compose exec db psql -U telco -d telco \
  -c 'SELECT schema_name FROM information_schema.schemata;'
```

Expected schemas: `catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`.

## Development Workflow

1. Edit `SQLModel` entities.
2. Autogenerate and apply an Alembic revision.
3. Reseed as needed to keep synthetic data aligned.
4. Run smoke tests (see `PLAN.md` / project test target).

## Logging

Structured logs for API requests and Dunning lifecycle events (notice issuance, SIM barring, settlement) are emitted to stdout for capture by the container runtime.
