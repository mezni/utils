# Quickstart: Sprint 1 Backend and Database

## Prerequisites

- Python 3.11
- PostgreSQL
- Local Docker and Docker Compose

## Setup

1. Create the project environment for the backend service.
2. Start PostgreSQL with the `inventory` and `gis` schemas available through migrations.
3. Apply the Alembic migrations.
4. Load the seed dataset with 3 partners, 15 stations, and 2-4 chargers per station.

## Run

1. Start the FastAPI service locally.
2. Verify `GET /api/health` returns `200` and confirms the database connection.
3. Verify `GET /api/stations/nearby?lat=36.8&lng=10.1&radius_km=10` returns nearby stations.

## Smoke Checks

- Exercise CRUD for `/api/partners`, `/api/stations`, and `/api/chargers`.
- Confirm deleted records disappear from later reads.
- Confirm the nearby endpoint returns no matches cleanly when the radius is too small.

## Expected Outcome

- All documented `/api` endpoints respond against a real PostgreSQL database.
- The seed data looks realistic for Tunisia and supports manual verification.
