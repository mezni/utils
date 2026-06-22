#!/bin/bash
set -e

# Stage 7: analytics_write_gate
# Validate that only driver-service can write to analytics_db

echo "=== Stage 7: analytics_write_gate ==="

# Scan all Rust source files and migrations for write operations to analytics_db
WRITE_OPERATIONS=0
VIOLATIONS=0

# Check Rust source files
for file in $(find . -name "*.rs" -type f ! -path "./target/*"); do
  # Check for INSERT/UPDATE/DELETE/CREATE TABLE operations
  if grep -qE "(INSERT INTO|UPDATE|DELETE FROM|CREATE TABLE|TRUNCATE)" "$file" 2>/dev/null; then
    # This is a write operation, check which service owns it
    if grep -qE "driver-service|driver_service|DriverService" "$file" 2>/dev/null; then
      WRITE_OPERATIONS=$((WRITE_OPERATIONS + 1))
    elif grep -qE "admin-service|admin_service|AdminService" "$file" 2>/dev/null; then
      VIOLATIONS=$((VIOLATIONS + 1))
      echo "  ERROR: Admin-service attempting write to analytics_db in $file"
    fi
  fi
done

# Check SQL migrations
for file in $(find . -name "*.sql" -type f -path "*/migrations/*"); do
  # Check for write operations
  if grep -qE "(INSERT INTO|UPDATE|DELETE FROM|CREATE TABLE|TRUNCATE)" "$file" 2>/dev/null; then
    # Check if this migration is for analytics_db
    if grep -q "analytics" "$file" 2>/dev/null; then
      if grep -qE "driver-service|driver_service|DriverService" "$file" 2>/dev/null; then
        WRITE_OPERATIONS=$((WRITE_OPERATIONS + 1))
      elif grep -qE "admin-service|admin_service|AdminService" "$file" 2>/dev/null; then
        VIOLATIONS=$((VIOLATIONS + 1))
        echo "  ERROR: Admin-service attempting write to analytics_db in $file"
      fi
    fi
  fi
done

if [ $VIOLATIONS -gt 0 ]; then
  echo "analytics_write_gate FAILED: Found $VIOLATIONS violations"
  echo '{"status":"failed","exit_code":1,"violation_count":'$VIOLATIONS',"write_operations":'$WRITE_OPERATIONS',"summary":"Only driver-service should write to analytics_db"}' > .specify/ci-artifacts/analytics_gate_report.json
  exit 1
else
  echo "analytics_write_gate PASSED"
  echo '{"status":"passed","exit_code":0,"violation_count":0,"write_operations":'$WRITE_OPERATIONS',"summary":"Analytics write gate passed"}' > .specify/ci-artifacts/analytics_gate_report.json
fi
