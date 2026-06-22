#!/bin/bash
set -e

# Stage 4: identity_validation
# Validate UUID vs nanoid usage across the codebase

echo "=== Stage 4: identity_validation ==="

# Define regex patterns
UUID_PATTERN="^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
STA_PATTERN="^STA[a-zA-Z0-9]{11}$"
CHG_PATTERN="^CHG[a-zA-Z0-9]{11}$"
OPR_PATTERN="^OPR[a-zA-Z0-9]{11}$"
EVT_PATTERN="^EVT[a-zA-Z0-9]{11}$"

# Find all Rust source files
RUST_FILES=$(find . -name "*.rs" -type f ! -path "./target/*" ! -path "./.cargo/*")

VIOLATIONS=0

# Check UUID usage in non-users tables
echo "Checking UUID usage in Rust source files..."
for file in $RUST_FILES; do
  # Check for UUID in entity identifiers (expecting nanoid with PREFIX)
  if grep -q "uuid::Uuid\|Uuid" "$file" 2>/dev/null; then
    # Check if file is related to users table or Keycloak
    if ! grep -q "users" "$file" 2>/dev/null && ! grep -q "Keycloak\|keycloak" "$file" 2>/dev/null; then
      VIOLATIONS=$((VIOLATIONS + 1))
      echo "  ERROR: UUID found in non-users table: $file"
    fi
  fi

done

# Check SQL migrations for identity violations
MIGRATION_FILES=$(find . -name "*.sql" -type f -path "*/migrations/*")

for file in $MIGRATION_FILES; do
  # Check for UUID in entity tables (expecting PREFIX-nanoid)
  if grep -q "CREATE TABLE" "$file" 2>/dev/null; then
    # Entity table - should use PREFIX-nanoid(12), not UUID
    if grep -E "user_id.*UUID\|station_id.*UUID\|charger_id.*UUID\|connector_id.*UUID\|partner_id.*UUID" "$file" >/dev/null 2>&1; then
      VIOLATIONS=$((VIOLATIONS + 1))
      echo "  ERROR: UUID found in entity table in $file"
    fi

    # Check for nanoid without PREFIX
    if grep -E "user_id.*nanoid\|station_id.*nanoid\|charger_id.*nanoid\|connector_id.*nanoid\|partner_id.*nanoid" "$file" >/dev/null 2>&1; then
      VIOLATIONS=$((VIOLATIONS + 1))
      echo "  ERROR: nanoid without PREFIX found in entity table in $file"
    fi
  fi
done

if [ $VIOLATIONS -gt 0 ]; then
  echo "identity_validation FAILED: Found $VIOLATIONS violations"
  echo '{"status":"failed","exit_code":1,"violation_count":'$VIOLATIONS',"summary":"Identity violations found"}' > .specify/ci-artifacts/identity_validation_report.json
  exit 1
else
  echo "identity_validation PASSED"
  echo '{"status":"passed","exit_code":0,"violation_count":0,"summary":"Identity validation passed"}' > .specify/ci-artifacts/identity_validation_report.json
fi
