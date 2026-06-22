#!/bin/bash
set -euo pipefail

# CI Gate: Identity Validation (CI-1.1)
# Checks:
# - FAIL if users.user_profiles uses non-UUID PK
# - FAIL if nanoid CHECK found in users schema
# - FAIL if UUID found in entity tables

echo "=== CI Gate: Identity Validation ==="

# Check 1: users.user_profiles should use UUID primary key
echo "Checking users.user_profiles primary key..."
if ! psql "$DATABASE_URL" -tAc "SELECT column_name FROM information_schema.columns WHERE table_name='user_profiles' AND column_name='user_uuid' AND is_nullable='NO';" | grep -q .; then
    echo "FAIL: users.user_profiles does not have a NOT NULL UUID column 'user_uuid'"
    exit 1
fi

# Check 2: users.user_profiles should NOT have nanoid CHECK constraint
echo "Checking for nanoid CHECK constraint..."
if psql "$DATABASE_URL" -tAc "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name='user_profiles' AND constraint_type='CHECK';" | grep -q nanoid; then
    echo "FAIL: users.user_profiles has nanoid CHECK constraint (should use UUID)"
    exit 1
fi

# Check 3: No entity tables should have UUID columns (entity IDs must be nanoid with PREFIX)
echo "Checking entity tables for UUID columns..."
ENTITY_TABLES="inventory.vehicle geo.zone geo.point carrier.contract carrier.trip"

for table in $ENTITY_TABLES; do
    if psql "$DATABASE_URL" -tAc "SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = '$table'
        AND data_type = 'uuid'
    );" | grep -q t; then
        echo "FAIL: Table '$table' has UUID column (entity IDs must be nanoid with PREFIX)"
        exit 1
    fi
done

echo "PASS: Identity validation checks passed"
exit 0
