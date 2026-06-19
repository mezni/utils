# Postgres Connection Contract

## Connection Strings

| Database | JDBC URL | PSQL Connection |
|----------|----------|-----------------|
| `platform_db` | `jdbc:postgresql://localhost:5432/platform_db` | `psql -h localhost -p 5432 -U {role} -d platform_db` |
| `keycloak_db` | `jdbc:postgresql://postgres:5432/keycloak_db` | (managed by Keycloak) |
| `analytics_db` | `jdbc:postgresql://localhost:5432/analytics_db` | `psql -h localhost -p 5432 -U admin_analytics_role -d analytics_db` |

## Roles and Passwords

| Role | Schema Access | Environment Variable |
|------|--------------|---------------------|
| `auth_service_role` | `users` only | `AUTH_DB_PASSWORD` |
| `admin_service_role` | `gis`, `inventory` | `ADMIN_DB_PASSWORD` |
| `driver_service_role` | `inventory` (read-only) | `DRIVER_DB_PASSWORD` |
| `admin_analytics_role` | `analytics_db.audit_log` | `ANALYTICS_DB_PASSWORD` |
| `keycloak` (superuser within PG) | all | `KEYCLOAK_DB_PASSWORD` |

## Password Management

Passwords are set via `.env` file. Required variables:

```
POSTGRES_PASSWORD=...
AUTH_DB_PASSWORD=...
ADMIN_DB_PASSWORD=...
DRIVER_DB_PASSWORD=...
ANALYTICS_DB_PASSWORD=...
KEYCLOAK_DB_PASSWORD=...
```

Passwords are set at role creation time and persist in Docker volume `pgdata`.
