# Quickstart — EV Charging Platform Foundation

## Prerequisites

- Docker 24+ and Docker Compose v2
- Git
- Make (optional, for convenience targets)

## Setup

```bash
# 1. Start all services
docker compose -f docker/docker-compose.yml up -d

# 2. Verify services are running
curl http://localhost:3001/api/v1/driver/health

# 3. Run OSM import (optional — seeds station data)
bash scripts/import-osm.sh

# 4. Refresh materialized view
bash scripts/refresh-mv.sh

# 5. Seed development data (if OSM import not used)
bash scripts/seed-dev.sh
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| driver-api | 3001 | Nearby search & station detail API |
| sync-engine | — | OSM ingestion & sync pipeline |
| ingestion | — | OSM Overpass API fetcher |
| web | 5173 | Driver map application |
| postgres | 5432 | PostgreSQL 16 + PostGIS |
| redis | 6379 | Caching (optional) |

## Development

### Backend (Rust)
```bash
cd services/driver-api
cargo run
```

### Frontend (Node.js)
```bash
cd apps/web
npm install
npm run dev
```

### Database Migrations
```bash
# Run migrations manually
psql -h localhost -U bornemap -d platform_db -f db/migrations/001_extensions.sql
```

## Useful Commands

```bash
# View PostGIS spatial tables
psql -d platform_db -c "\d mv_stations_geo"

# Test nearby query
psql -d platform_db -c "SELECT * FROM find_nearby_stations(36.8065, 10.1815, 5000, 10);"

# Refresh materialized view
psql -d platform_db -c "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_stations_geo;"

# View sync job history
psql -d platform_db -c "SELECT * FROM sync_jobs ORDER BY created_at DESC LIMIT 10;"
```
