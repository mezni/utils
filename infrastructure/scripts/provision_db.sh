#!/bin/bash
set -e

# Database Provisioning Script for BorneMap
# Initializes platform_db, analytics_db, and sets up schemas

echo "=== BorneMap Database Provisioning ==="
echo ""

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
  echo "ERROR: docker-compose is not installed"
  echo "Please install docker-compose: https://docs.docker.com/compose/install/"
  exit 1
fi

# Check if docker is running
if ! docker info &> /dev/null; then
  echo "ERROR: Docker is not running"
  echo "Please start Docker: https://docs.docker.com/get-docker/"
  exit 1
fi

# Navigate to infrastructure directory
cd infrastructure

# Start database containers
echo "Starting database containers..."
docker-compose -f docker-compose/local.yml up -d

# Wait for databases to be ready
echo "Waiting for databases to be ready..."
sleep 5

# Create database users and schemas
echo "Creating database users and schemas..."

# Platform DB
docker-compose exec -T platform-db psql -U bornemap_admin -c "
  CREATE SCHEMA IF NOT EXISTS users;
  CREATE SCHEMA IF NOT EXISTS gis;
  CREATE SCHEMA IF NOT EXISTS inventory;

  GRANT ALL PRIVILEGES ON SCHEMA users TO bornemap_admin;
  GRANT ALL PRIVILEGES ON SCHEMA gis TO bornemap_driver;
  GRANT ALL PRIVILEGES ON SCHEMA inventory TO bornemap_admin;
"

# Analytics DB
docker-compose exec -T analytics-db psql -U bornemap_analytics_writer -c "
  CREATE SCHEMA IF NOT EXISTS telemetry;
  CREATE SCHEMA IF NOT EXISTS analytics_events;
  CREATE SCHEMA IF NOT EXISTS system_events;

  GRANT ALL PRIVILEGES ON SCHEMA telemetry TO bornemap_analytics_writer;
  GRANT USAGE ON SCHEMA telemetry TO bornemap_analytics_reader;
"

echo ""
echo "=== Database Provisioning Complete ==="
echo ""
echo "Databases are ready:"
echo "  Platform DB: localhost:5432 (users, gis, inventory schemas)"
echo "  Analytics DB: localhost:5433 (telemetry, analytics_events, system_events schemas)"
echo ""
echo "PostgreSQL users:"
echo "  bornemap_admin (platform_db: users, gis, inventory)"
echo "  bornemap_driver (platform_db: gis)"
echo "  bornemap_analytics_writer (analytics_db: all)"
echo "  bornemap_analytics_reader (analytics_db: read-only)"
echo ""
echo "Next steps:"
echo "  1. Review database migrations in services/*/migrations/"
echo "  2. Run migrations using: ./migrate.sh"
echo "  3. Start services using: ./deploy.sh"
