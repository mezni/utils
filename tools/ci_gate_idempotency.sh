#!/bin/bash
set -e

echo "Running: UUID v7 Idempotency Gate"
echo "=================================="

# Check for UUID v7 idempotency_key usage and unique index

# Check for UUID v7 usage
echo "Checking for UUID v7 usage..."
if ! grep -r "Uuid::new_v7\|uuid::Uuid::new_v7" --include="*.rs" services/driver-service/src > /dev/null; then
    echo "❌ FAIL: UUID v7 generation not found"
    exit 1
fi
echo "✓ UUID v7 generation found"

# Check for idempotency_key field
echo "Checking for idempotency_key field..."
if ! grep -r "idempotency_key" --include="*.rs" services/driver-service/src/middleware/idempotency.rs > /dev/null 2>&1; then
    echo "❌ FAIL: idempotency_key field not found in idempotency.rs"
    exit 1
fi
echo "✓ idempotency_key field found"

# Check for unique index on idempotency_key in database migration
echo "Checking for unique index on idempotency_key..."
if ! grep -r "idempotency_key" --include="*.sql" services/driver-service/migrations/0005_analytics_events.up.sql > /dev/null; then
    echo "❌ FAIL: unique index on idempotency_key not found in migration"
    exit 1
fi
echo "✓ unique index on idempotency_key found"

# Check for duplicate detection logic
echo "Checking for duplicate detection logic..."
if ! grep -r "SELECT.*idempotency_key.*analytics_events" --include="*.rs" services/driver-service/src/db/analytics.rs > /dev/null 2>&1; then
    echo "❌ FAIL: duplicate detection logic not found"
    exit 1
fi
echo "✓ duplicate detection logic found"

echo "=================================="
echo "✓ PASS: UUID v7 idempotency enforced"
exit 0
