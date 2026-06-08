# Local Development Setup Guide

This guide walks you through setting up and running the BorneMap API locally using Docker Compose.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.11+ (for local development without Docker)
- PostgreSQL 15+ (optional if using Docker)

## Setup with Docker Compose

### 1. Start Services

```bash
# From repository root
docker-compose up -d
```

This will start:
- PostgreSQL 15 on `localhost:5432`
- FastAPI service on `http://localhost:8000`

### 2. Initialize Database

```bash
# Run migrations
docker-compose exec bornemap-service alembic upgrade head
```

### 3. Load Seed Data (Optional)

```bash
# Load development seeds (3 partners, 15 stations, 24 chargers)
docker-compose exec bornemap-service python -m app.seed
```

### 4. Access the API

- **API Docs (Swagger UI)**: http://localhost:8000/api/docs
- **API ReDoc**: http://localhost:8000/api/redoc
- **OpenAPI Spec**: http://localhost:8000/api/openapi.json

### 5. Run Tests

```bash
# Run all tests
docker-compose exec bornemap-service pytest

# Run versioning tests only
docker-compose exec bornemap-service pytest tests/test_versioning.py -v

# Run with coverage
docker-compose exec bornemap-service pytest --cov=app tests/
```

### 6. View Logs

```bash
# Service logs
docker-compose logs -f bornemap-service

# Database logs
docker-compose logs -f postgres
```

### 7. Stop Services

```bash
docker-compose down

# To also remove data volumes
docker-compose down -v
```

## Local Development Without Docker

### 1. Install Python Dependencies

```bash
cd source/services/bornemap-service
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Set Environment Variables

```bash
# Copy example and edit
cp .env.example .env

# Edit .env with your database credentials
export $(cat .env | xargs)
```

### 3. Start PostgreSQL

```bash
# Using Docker (recommended)
docker run -d \
  --name postgres-bornemap \
  -e POSTGRES_USER=bornemap_user \
  -e POSTGRES_PASSWORD=bornemap_password \
  -e POSTGRES_DB=ev_platform \
  -p 5432:5432 \
  postgres:15-alpine
```

### 4. Run Migrations

```bash
cd source/services/bornemap-service
alembic upgrade head
```

### 5. Start the API Server

```bash
cd source/services/bornemap-service
python -m uvicorn app.main:app --reload --port 8000
```

Server will be available at `http://localhost:8000`

### 6. Run Tests

```bash
cd source/services/bornemap-service
pytest tests/test_versioning.py -v
```

## Environment Variables

Key environment variables (from `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `bornemap_user` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `bornemap_password` | PostgreSQL password |
| `POSTGRES_DB` | `ev_platform` | Database name |
| `DATABASE_URL` | `postgresql://...` | Full PostgreSQL connection string |
| `API_PORT` | `8000` | API server port |
| `SERVICE_NAME` | `bornemap-service` | Service identifier |
| `ENVIRONMENT` | `development` | Environment (development/production) |

## Troubleshooting

### PostgreSQL Connection Issues

```bash
# Check if PostgreSQL container is running
docker ps | grep postgres

# View PostgreSQL logs
docker-compose logs postgres

# Test connection manually
psql -h localhost -U bornemap_user -d ev_platform
```

### Port Already in Use

If port 8000 or 5432 is already in use:

```bash
# Change ports in .env
API_PORT=8001

# Or kill existing process
lsof -i :8000
kill -9 <PID>
```

### Tests Failing

```bash
# Reset database and run migrations
docker-compose exec bornemap-service alembic downgrade base
docker-compose exec bornemap-service alembic upgrade head

# Run tests with verbose output
docker-compose exec bornemap-service pytest -vv tests/test_versioning.py
```

## API Versioning

All BorneMap API endpoints are versioned via URL path prefix:

- **v1 endpoints**: `/api/v1/stations`, `/api/v1/partners`, etc.
- **Unversioned endpoints** (e.g., `/api/stations`) return **404 Not Found**
- **Invalid version** (e.g., `/api/v999/stations`) returns **404 Not Found**

For complete API documentation, see `/docs/api/bornemap-service.md`.

## Development Workflow

1. Make changes to code in `source/services/bornemap-service/app/`
2. With Docker Compose, changes auto-reload (thanks to `--reload` flag)
3. Run tests: `pytest tests/test_versioning.py -v`
4. Commit changes: `git add . && git commit -m "message"`
5. Push: `git push origin 001-backend-and-database`

## Next Steps

- Read `/docs/api/bornemap-service.md` for API documentation
- Review `/specs/001-backend-and-database/contracts/api-v1.md` for API contract details
- Check `/docs/adr/ADR-018-api-versioning.md` for versioning architecture

---

For questions or issues, see `/docs/README.md` for navigation.
