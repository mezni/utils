# Docker Compose Contract

## Services

### platform-db

| Property | Value |
|----------|-------|
| Image | postgis/postgis:16-3.3 |
| Port | 5432:5432 |
| Environment | POSTGRES_DB=platform_db, POSTGRES_USER=borneadmin, POSTGRES_PASSWORD |
| Volumes | platform_db_data:/var/lib/postgresql/data |
| Health check | pg_isready -U borneadmin -d platform_db |
| Network | bornemap (bridge) |

### analytics-db

| Property | Value |
|----------|-------|
| Image | postgres:16 |
| Port | 5433:5432 |
| Environment | POSTGRES_DB=analytics_db, POSTGRES_USER=borneadmin, POSTGRES_PASSWORD |
| Volumes | analytics_db_data:/var/lib/postgresql/data |
| Health check | pg_isready -U borneadmin -d analytics_db |
| Network | bornemap (bridge) |

## Networks

| Name | Driver |
|------|--------|
| bornemap | bridge |

## Volumes

| Name | Purpose |
|------|---------|
| platform_db_data | Persistent storage for platform_db |
| analytics_db_data | Persistent storage for analytics_db |

## Port Allocation

| Port | Service | Protocol |
|------|---------|----------|
| 5432 | platform-db | TCP |
| 5433 | analytics-db | TCP |

## Startup Order

1. platform-db (health check: pg_isready)
2. analytics-db (health check: pg_isready)
3. Backend services (future Phase 2 — depend on both DBs healthy)
