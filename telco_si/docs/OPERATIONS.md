# Operations

## Containerized Development Environment

The environment is fully repeatable using Docker Compose with two services:

| Service | Purpose |
| --- | --- |
| `db` | PostgreSQL instance with six isolated application schemas. |
| `app` | The Telco SI API (FastAPI) with automatic migrations; test/lint tooling included. |

## Quick Start

```bash
# Build and start the environment
docker compose up -d --build

# Confirm readiness
curl http://localhost:8000/health
# {"status":"ok","database":"up"}
```

The API is available at `http://localhost:8000` with interactive docs at `/docs`.

### Startup Behavior

On container start the app (`app.main:app` via `uvicorn`) runs the startup
sequence defined in `contracts/startup-migrations.md`:

1. Polls the database for up to `DB_RETRY_WINDOW` seconds (default 30).
2. Verifies the integrity of already-applied migrations against the recorded
   checksums in `public.alembic_revision_checksum` (modified applied revisions
   fail startup with a clear error).
3. Runs `alembic upgrade head` — creates the six domain schemas
   (`catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`) on a fresh
   instance and applies only pending revisions otherwise.
4. Logs the pinned readiness line `READY: app listening on {API_HOST}:{API_PORT}`
   and starts serving.

If the database never becomes reachable within `DB_RETRY_WINDOW`, or a migration
fails, the app logs a clear error and exits with a non-zero status.

## Common Commands

### Migrations

Alembic performs **automated multi-schema introspection and version tracking**
across all application schemas. The `alembic_version` table and the revision
checksum ledger live in the `public` schema.

```bash
# Apply all migrations (also performed automatically at startup)
docker compose exec app alembic upgrade head

# Show current revision
docker compose exec app alembic current

# Roll back one revision
docker compose exec app alembic downgrade -1

# Autogenerate a new revision after model changes
docker compose exec app alembic revision --autogenerate -m "description"
```

Applied migrations are treated as immutable: the startup runner records each
applied revision's file checksum and refuses to start if an applied revision is
edited in place (edit by adding a new revision instead).

### Database Shell

```bash
docker compose exec db psql -U telco -d telco
```

## Environment Configuration

Configuration is driven by environment variables (see `contracts/environment-config.md`):

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql+asyncpg://telco:telco@db:5432/telco` | asyncpg connection string. |
| `API_HOST` | `0.0.0.0` | API bind host. |
| `API_PORT` | `8000` | API bind port. |
| `DB_RETRY_WINDOW` | `30` | Seconds to retry database connectivity at startup. |

Any PostgreSQL instance reachable from the app container can be targeted by
overriding `DATABASE_URL` (no code changes).

## Verification / Smoke Test

```bash
# Confirm readiness
curl -s http://localhost:8000/health
# {"status":"ok","database":"up"}

# Confirm the migration head
docker compose exec app alembic current
# 0001 (head)

# Confirm the six domain schemas exist
docker compose exec db psql -U telco -d telco -c '\dn'

# Run the test suite in the app container
docker compose exec app pytest -q
# 8 passed

# Lint and formatting
docker compose exec app ruff check .
docker compose exec app ruff format --check .
```

Expected schemas: `catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`
(domain tables are planned but not yet present; only the `public` schema holds
the migration tables).

## Development Workflow

1. Follow the specification-driven process (`specs/<feature>/tasks.md`); mark
   tasks `[X]` as you complete them.
2. Add or update code; when schema-affecting logic changes, add an Alembic
   revision (`migrations/versions/`) — never rewrite an applied one.
3. Run the suite and lint inside the container (see above); fix Ruff findings.
4. Update `docs/` and `CHANGELOG.md` when behavior changes.

## Logging

Startup and lifecycle messages are logged to stdout at INFO level (e.g.,
`Database connection established.`, `Migrations applied/up to date.`,
`READY: app listening on 0.0.0.0:8000`), captured by the container runtime via
`docker compose logs app`. Failure paths log a clear ERROR line before exiting
non-zero.