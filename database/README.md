# Database

## Schema Separation

| Schema | Purpose | Owned By |
|--------|---------|----------|
| `users` | Authentication & accounts | auth-service |
| `ev` | Business domain (partners, stations, connectors) | admin-service |
| `gis` | Spatial data (PostGIS) | admin-service (write), driver-service (read) |

## Migrations

Migrations are managed via SQLx embedded migrations:

```bash
./scripts/migrate.sh
```

Migration files follow the pattern: `NNNN_description.sql`

## Requirements

- PostgreSQL 15+ with PostGIS 3.4+
