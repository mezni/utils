#!/bin/bash
set -e

# Database Migration Script for BorneMap
# Applies all migration files to each service

echo "=== BorneMap Database Migration ==="
echo ""

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
  echo "ERROR: docker-compose is not installed"
  exit 1
fi

# Navigate to infrastructure directory
cd infrastructure

# Start database containers if not running
docker-compose -f docker-compose/local.yml ps | grep -q "Exit" && {
  echo "Starting database containers..."
  docker-compose -f docker-compose/local.yml up -d
  sleep 5
}

# Function to run migrations for a service
run_migrations() {
  local service_name=$1
  local db_container=$2
  local db_user=$3

  echo "Applying migrations for $service_name..."

  # Find all migration files
  local migrations=$(find ../services/$service_name/migrations -name "*.up.sql" -type f | sort)

  if [ -z "$migrations" ]; then
    echo "  No migration files found for $service_name"
    return
  fi

  # Apply each migration
  while IFS= read -r migration_file; do
    echo "  Applying: $(basename $migration_file)"
    docker-compose exec -T "$db_container" psql -U "$db_user" -d "platform_db" < "$migration_file"
  done <<< "$migrations"

  echo "  ✓ $service_name migrations completed"
}

# Run migrations for each service
run_migrations "auth-service" "platform-db" "bornemap_admin"
run_migrations "driver-service" "platform-db" "bornemap_driver"
run_migrations "admin-service" "platform-db" "bornemap_admin"

echo ""
echo "=== Database Migration Complete ==="
echo ""
echo "All migrations have been applied successfully."
echo ""
echo "To verify migrations:"
echo "  docker-compose exec platform-db psql -U bornemap_admin -d platform_db -c '\dt'"
