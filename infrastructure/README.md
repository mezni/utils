# BorneMap Infrastructure

This directory contains infrastructure configuration for development and deployment.

## Docker Compose

### Local Development

Run all services with:

```bash
docker-compose -f docker-compose/local.yml up -d
```

This will start:
- PostgreSQL (platform_db): port 5432
- PostgreSQL (analytics_db): port 5433
- Redis: port 6379

### Services

Services run on fixed ports (enforced by constitution):
- auth-service: port 3000
- driver-service: port 3001
- admin-service: port 3002

## Scripts

### provision_db.sh

Initialize databases and create schemas:

```bash
./infrastructure/scripts/provision_db.sh
```

Creates:
- platform_db with users, gis, inventory schemas
- analytics_db with telemetry, analytics_events, system_events schemas
- PostgreSQL roles with proper permissions

### migrate.sh

Apply database migrations:

```bash
./infrastructure/scripts/migrate.sh
```

Applies migrations for all services in order:
1. auth-service migrations (users schema)
2. driver-service migrations (gis, analytics schemas)
3. admin-service migrations (inventory schema)

### deploy.sh

Build and deploy services:

```bash
./infrastructure/scripts/deploy.sh
```

Builds all services and starts them. Note: Services need to be run separately.

## Configuration

### Database Connection Strings

Platform DB (auth-service, driver-service, admin-service):
```
postgresql://bornemap_admin:bornemap_password@localhost:5432/platform_db
```

Analytics DB (driver-service write, admin-service read):
```
postgresql://bornemap_analytics_writer:bornemap_password@localhost:5433/analytics_db
```

## PostgreSQL Roles

- **bornemap_admin**: Full access to platform_db (users, gis, inventory)
- **bornemap_driver**: Read/write access to platform_db (gis), read access to analytics_db
- **bornemap_analytics_writer**: Full access to analytics_db
- **bornemap_analytics_reader**: Read access to analytics_db

## Development Workflow

1. Start databases: `docker-compose -f docker-compose/local.yml up -d`
2. Provision databases: `./infrastructure/scripts/provision_db.sh`
3. Apply migrations: `./infrastructure/scripts/migrate.sh`
4. Build services: `make build`
5. Start services manually: See service documentation

## Testing Infrastructure

Services can be tested with:

```bash
# Health check auth-service
curl http://localhost:3000/health

# Health check driver-service
curl http://localhost:3001/health

# Health check admin-service
curl http://localhost:3002/health
```

## Troubleshooting

### Port Already in Use

If ports 3000, 3001, 3002, 5432, 5433, or 6379 are in use, you can:

1. Stop the services: `docker-compose -f docker-compose/local.yml down`
2. Change ports in docker-compose/local.yml
3. Update service configurations accordingly

### Database Connection Errors

If you get connection errors:

1. Check if databases are running: `docker-compose ps`
2. Check database logs: `docker-compose logs platform-db`
3. Verify credentials in service config.toml files

### Docker Issues

If you encounter Docker issues:

1. Check Docker status: `docker info`
2. Restart Docker daemon
3. Rebuild containers: `docker-compose -f docker-compose/local.yml down && docker-compose -f docker-compose/local.yml up -d`
