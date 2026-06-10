# Quickstart: Docker Compose and CI/CD

**Branch**: `011-docker-compose-ci`

## Prerequisites

- Docker Engine 24+ with `docker compose` plugin
- No local PostgreSQL needed (Docker Compose manages it)
- `.dockerignore` at repo root excludes unnecessary build context files

## Running All Services

```bash
# From repo root
docker compose up --build -d

# Check status
docker compose ps

# View logs
docker compose logs -f

# Check health
curl http://localhost:8080/api/health     # Driver Service
curl http://localhost:8081/api/health     # Admin Service

# Stop all
docker compose down
```

## Running Without Frontend Apps

If you only need backends + database:

```bash
docker compose up --build -d postgres driver-service admin-service
```

## Environment Configuration

Docker Compose sets `DATABASE_URL` automatically:
- driver-service: `postgres://postgres:postgres@postgres:5432/borne_map`
- admin-service: `postgres://postgres:postgres@postgres:5432/borne_map`

Frontend apps use `API_BASE_URL`:
- dashboard: `http://driver-service:8080`
- driver-web: `http://driver-service:8080`
- driver-mobile: `http://driver-service:8080`

## Database

```bash
# Reset database (removes volumes)
docker compose down -v && docker compose up --build -d

# Connect directly
docker exec -it borne-postgres psql -U postgres -d borne_map
```

## CI/CD

All workflows are in `.github/workflows/`. They trigger automatically on pull requests to the relevant service branch.

To run CI locally (no GitHub):
```bash
cargo build --all && cargo test --all && cargo clippy --all -- -D warnings
```
