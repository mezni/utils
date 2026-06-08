# BorneMap Service

FastAPI backend service for the BorneMap EV charging station discovery platform (MVP-1).

## Overview

BorneMap Service provides REST API endpoints for discovering, managing, and querying electric vehicle (EV) charging stations across Tunisia. The service is built with Python 3.11+ and FastAPI, backed by PostgreSQL.

**Current Version**: v1.0  
**API Prefix**: `/api/v1/`  
**Status**: Active (Sprint 1.1)

---

## Architecture

### Stack

- **Framework**: FastAPI 0.109.0
- **Server**: Uvicorn
- **Database**: PostgreSQL 15
- **ORM**: SQLAlchemy 2.0
- **Validation**: Pydantic 2.5
- **Language**: Python 3.11+

### Schemas

**Inventory Schema** (business logic):
- `partner` — EV charging station operators
- `station` — Individual charging locations
- `charger` — Physical charging points

**GIS Schema** (reserved for MVP-4, empty in MVP-1)

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- OR: Python 3.11+, PostgreSQL 15+

### With Docker (Recommended)

```bash
# Start services
docker-compose up -d

# Initialize database
docker-compose exec bornemap-service alembic upgrade head

# Run tests
docker-compose exec bornemap-service pytest tests/ -v

# Access API
open http://localhost:8000/api/docs
```

### Local Development

```bash
# Install dependencies
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set environment
export DATABASE_URL=postgresql://user:pass@localhost/ev_platform

# Run migrations
alembic upgrade head

# Start server
python -m uvicorn app.main:app --reload --port 8000

# Run tests
pytest tests/ -v
```

See `docs/guides/local-setup.md` for detailed setup.

---

## API Endpoints

### v1 API (Current)

All endpoints under `/api/v1/`:

#### Health
- `GET /api/v1/health` — Service status and database connectivity

#### Partners
- `GET /api/v1/partners` — List all partners
- `POST /api/v1/partners` — Create partner
- `GET /api/v1/partners/{id}` — Get partner
- `PUT /api/v1/partners/{id}` — Update partner
- `DELETE /api/v1/partners/{id}` — Delete partner

#### Stations
- `GET /api/v1/stations` — List all stations
- `GET /api/v1/stations/nearby?lat=X&lng=Y&radius_km=50` — Find nearby stations
- `POST /api/v1/stations` — Create station
- `GET /api/v1/stations/{id}` — Get station with chargers
- `PUT /api/v1/stations/{id}` — Update station
- `DELETE /api/v1/stations/{id}` — Delete station

#### Chargers
- `GET /api/v1/chargers` — List all chargers
- `POST /api/v1/chargers` — Create charger
- `GET /api/v1/chargers/{id}` — Get charger
- `PUT /api/v1/chargers/{id}` — Update charger
- `DELETE /api/v1/chargers/{id}` — Delete charger

**Full API Documentation**: `docs/api/bornemap-service.md`

---

## API Versioning

BorneMap API uses **URL-based versioning**. All endpoints must include the version in the path:

- ✅ Correct: `/api/v1/stations`
- ❌ Incorrect: `/api/stations` (returns 404)
- ❌ Incorrect: `/api/v999/stations` (returns 404)

**Version Support**:
- v1: Active (Sprint 1.1+), supported 12 months after v2 release
- v2: Coming in MVP-2 (Rust migration)

See `docs/adr/ADR-018-api-versioning.md` for strategy.

---

## Development

### Project Structure

```
source/services/bornemap-service/
├── app/
│   ├── main.py                 # FastAPI app & routing
│   ├── database.py             # SQLAlchemy session
│   ├── models/
│   │   ├── inventory.py        # Partner, Station, Charger entities
│   ├── schemas/
│   │   ├── partners.py         # Pydantic request/response models
│   │   ├── stations.py
│   │   └── chargers.py
│   └── routers/
│       └── v1/
│           ├── health.py       # GET /api/v1/health
│           ├── partners.py     # /api/v1/partners*
│           ├── stations.py     # /api/v1/stations*
│           └── chargers.py     # /api/v1/chargers*
├── migrations/                 # Alembic database migrations
│   └── versions/
│       └── 001_init_inventory_schema.py
├── tests/
│   └── test_versioning.py      # Smoke tests
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Container image
└── pytest.ini                  # Test configuration
```

### Making Changes

1. Create feature branch: `git checkout -b feat/feature-name`
2. Make changes
3. Run tests: `pytest tests/ -v`
4. Check code style: `flake8 app/`
5. Commit: `git commit -m "feat: description"`
6. Push & create PR

### Running Tests

```bash
# All tests
pytest tests/ -v

# Specific test file
pytest tests/test_versioning.py -v

# Specific test
pytest tests/test_versioning.py::TestVersioningBehavior::test_health_endpoint_versioned -v

# With coverage
pytest tests/ --cov=app --cov-report=html
```

### Database Migrations

```bash
# Create migration
alembic revision --autogenerate -m "description"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1

# View migration history
alembic history
```

---

## Environment Variables

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

**Key variables**:
- `DATABASE_URL` — PostgreSQL connection string
- `POSTGRES_USER` — Database user
- `POSTGRES_PASSWORD` — Database password
- `POSTGRES_DB` — Database name
- `API_PORT` — Server port (default: 8000)

---

## API Documentation

### Interactive Documentation

After starting the server, open:
- **Swagger UI**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc
- **OpenAPI Spec**: http://localhost:8000/api/openapi.json

### Written Documentation

- `docs/api/bornemap-service.md` — Full API reference
- `docs/adr/ADR-018-api-versioning.md` — Versioning strategy
- `docs/guides/local-setup.md` — Local development guide
- `specs/001-backend-and-database/contracts/api-v1.md` — v1 API contract

---

## Performance

### Target Metrics

- **Endpoint latency**: <200ms p95
- **Throughput**: ≥1000 req/s
- **Database queries**: No N+1 queries

### Optimization Tips

- Use database indexes on frequently queried fields
- Implement pagination for large datasets (future)
- Cache static data (e.g., charger types)
- Profile slow endpoints with `uvicorn --profile`

---

## Deployment

### Docker

```bash
# Build image
docker build -t bornemap-service .

# Run container
docker run -p 8000:8000 \
  -e DATABASE_URL=postgresql://... \
  bornemap-service

# Or use Docker Compose
docker-compose up
```

### Production Checklist

- [ ] Set `ENVIRONMENT=production`
- [ ] Use managed PostgreSQL (not Docker)
- [ ] Enable HTTPS/SSL
- [ ] Configure API Gateway
- [ ] Set up monitoring & logging
- [ ] Configure backup strategy
- [ ] Document runbooks in `docs/ops/`

See `docs/ops/deployment.md` (when created).

---

## Troubleshooting

### Database Connection Issues

```bash
# Test connection
psql $DATABASE_URL

# Check Docker logs
docker-compose logs postgres

# Restart services
docker-compose restart
```

### Tests Failing

```bash
# Reset database
docker-compose exec bornemap-service alembic downgrade base
docker-compose exec bornemap-service alembic upgrade head

# Run tests with verbose output
pytest tests/ -vv --tb=short
```

### Port Already in Use

```bash
# Change port in .env
API_PORT=8001

# Or kill existing process
lsof -i :8000
kill -9 <PID>
```

---

## Contributing

1. Read `AGENTS.md` for project guidelines
2. Check `docs/constitution.md` for principles
3. Follow this README's "Making Changes" section
4. Ensure all tests pass before submitting PR

---

## Architecture Decisions

- **API Versioning** → See `docs/adr/ADR-018-api-versioning.md`
- **Database Schema** → See `docs/schema/inventory-schema.md`
- **Error Handling** → All errors return `{"detail": "message"}`

---

## Roadmap

### MVP-1 (Sprint 1.1, Current)
- ✅ v1 API with 16 endpoints
- ✅ PostgreSQL database setup
- ✅ API documentation
- ✅ Versioning infrastructure

### MVP-2 (Coming)
- Rust migration (Actix-web)
- v2 API with new features
- Service split (partners, stations, chargers services)
- API Gateway

### MVP-3+
- Mobile-specific APIs
- Real-time updates (OCPP integration)
- Advanced analytics

---

## Support

- **Documentation**: `/docs/`
- **Issues**: GitHub issues
- **Questions**: See `/docs/README.md` for contact info

---

## License

Proprietary — BorneMap Platform

---

**Last Updated**: 2026-06-08  
**Maintained by**: Engineering Team
