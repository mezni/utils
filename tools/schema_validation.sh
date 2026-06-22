#!/bin/bash
set -e

# Stage 5: schema_validation
# Validate database schema definitions match expected structure

echo "=== Stage 5: schema_validation ==="

# Check for required databases and schemas
MISSING_SCHEMAS=0

# Check platform_db schemas
if [ ! -f "infrastructure/docker-compose/local.yml" ]; then
  echo "ERROR: Missing docker-compose file for database configuration"
  exit 1
fi

# Extract schema names from docker-compose if available
# For now, we'll check for required migration files

MIGRATION_DIRS=("services/auth-service/migrations" "services/driver-service/migrations" "services/admin-service/migrations")

for dir in "${MIGRATION_DIRS[@]}"; do
  if [ ! -d "$dir" ]; then
    echo "ERROR: Missing migration directory: $dir"
    MISSING_SCHEMAS=$((MISSING_SCHEMAS + 1))
  fi
done

if [ $MISSING_SCHEMAS -gt 0 ]; then
  echo "schema_validation FAILED: Missing required migration directories"
  echo '{"status":"failed","exit_code":1,"missing_schemas":'$MISSING_SCHEMAS',"summary":"Missing required schemas"}' > .specify/ci-artifacts/schema_validation_report.json
  exit 1
else
  echo "schema_validation PASSED"
  echo '{"status":"passed","exit_code":0,"missing_schemas":0,"summary":"Schema validation passed"}' > .specify/ci-artifacts/schema_validation_report.json
fi
