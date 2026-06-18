# Quickstart: Core Data & Storage Foundations

## Prerequisites

- Docker Engine 24+ and Docker Compose v2
- ~2GB free disk space
- Internet connection (for Docker image pull and OSM data download)

## Setup

```bash
# 1. Create .env from template
echo "DB_PASSWORD=bornemap_dev" > .env

# 2. Start the database
docker compose -f source/infra/docker-compose.yml up -d platform_db

# 3. Verify database is ready
docker compose -f source/infra/docker-compose.yml exec platform_db pg_isready -U bornemap -d platform_db

# 4. Load Tunisia OSM data (one-time)
docker compose -f source/infra/docker-compose.yml run --rm osm-importer

# 5. Verify schemas
docker compose -f source/infra/docker-compose.yml exec platform_db psql -U bornemap -d platform_db -c "\dn"

# 6. Test spatial function
docker compose -f source/infra/docker-compose.yml exec platform_db psql -U bornemap -d platform_db \
  -c "SELECT station_id, station_name, distance_meters FROM inventory.get_nearby_stations(10.1, 36.8, 10000);"
```

## Expected Results

```
# Schema verification
  List of schemas
  Name      | Owner
-----------+---------
  gis       | bornemap
  inventory | bornemap
  public    | postgres

# Spatial function (10km from central Tunis)
  station_id | station_name | distance_meters
------------+-------------+----------------
  STA_xxx   | Tunis Centre |           1200
  STA_xxx   | Tunis Nord   |           4500
```

## File Locations

| Artifact | Path |
|----------|------|
| Docker Compose | `source/infra/docker-compose.yml` |
| Schema + Seed + Function | `source/infra/db/init.sql` |
| OSM Importer | `source/infra/osm-importer/` |
| Tracking Docs | `source/docs/` |

## Useful Commands

```bash
# Inspect database
psql -h localhost -U bornemap -d platform_db

# Rebuild from scratch
docker compose -f source/infra/docker-compose.yml down -v && \
docker compose -f source/infra/docker-compose.yml up -d platform_db

# View logs
docker compose -f source/infra/docker-compose.yml logs -f platform_db
```
