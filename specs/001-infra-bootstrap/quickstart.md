# Quickstart: BorneMap Local Development Environment

## Prerequisites

- Docker Desktop (or Docker Engine 24+ with Compose v2 plugin)
- Git
- At least 4GB RAM available for containers

## Setup

```bash
# Clone the repository (first time only)
git clone <repo-url> borne-map
cd borne-map

# Copy environment file and edit credentials
cp source/infra/.env.example source/infra/.env
# Edit source/infra/.env to set passwords

# Start everything
cd source/infra
docker compose up -d
```

## Verify

```bash
# Check all containers are healthy
docker compose ps

# Test Postgres
psql -h localhost -p 5432 -U postgres -d platform_db -c "SELECT 1"

# Test Redis
redis-cli -h localhost -p 6379 PING

# Test Keycloak admin console
open http://localhost:8080/admin/master/console/

# Test Traefik routing
curl http://localhost/api/v1/auth/login      # → {"service":"auth-service","status":"stub"}
curl http://localhost/api/v1/admin/partner   # → {"service":"admin-service","status":"stub"}
curl http://localhost/api/v1/driver/stations # → {"service":"driver-service","status":"stub"}
curl http://localhost/api/v1/unknown         # → 404 {"error":"route_not_found"}
```

## Common Tasks

```bash
# View logs for a specific service
docker compose logs -f <service-name>

# Restart a single service
docker compose restart <service-name>

# Rebuild stub images after changes
docker compose build <service-name>

# Stop everything
docker compose down

# Stop and delete volumes (reset all data)
docker compose down -v

# Access Postgres
psql -h localhost -p 5432 -U postgres -d platform_db

# View databases
psql -h localhost -p 5432 -U postgres -c "\l"
```

## Service Ports

| Service | Internal | Host | Purpose |
|---------|----------|------|---------|
| Postgres | 5432 | 5432 | Direct DB access |
| Redis | 6379 | 6379 | Direct cache access |
| Keycloak | 8080 | 8080 | Admin console |
| Traefik | 80 | 80 | API gateway |
| Stub: auth | 3000 | — | Internal only |
| Stub: admin | 3002 | — | Internal only |
| Stub: driver | 3001 | — | Internal only |

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Port already in use | Another process on 5432/6379/8080/80 | Stop the conflicting process or change ports in `.env` |
| Keycloak fails to start | Postgres not ready yet | Wait 30s, then `docker compose restart keycloak` |
| `docker compose` not found | Older Docker | Install Docker Compose v2 plugin |
| "permission denied" on ports | Linux without `sudo` | Add user to `docker` group |
