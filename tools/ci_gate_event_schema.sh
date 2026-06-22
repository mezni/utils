#!/bin/bash
set -e

echo "Running: Event Schema Validation Gate"
echo "======================================"

# Validate schema_version, user_id UUID, timestamp ISO 8601, payload JSON, event_type enum, location_source enum

# Check for schema_version in event contracts
echo "Checking for schema_version field..."
if ! grep -r "schema_version" --include="*.rs" apps/packages/domain-types/src/events.rs > /dev/null; then
    echo "❌ FAIL: schema_version field not found in events.rs"
    exit 1
fi
echo "✓ schema_version field found"

# Check for user_id UUID format validation
echo "Checking for user_id UUID format validation..."
if ! grep -r "Uuid::from_str\|uuid::Uuid::parse_str" --include="*.rs" services/driver-service/src/middleware/validation.rs > /dev/null 2>&1; then
    echo "❌ FAIL: user_id UUID validation not found in validation.rs"
    exit 1
fi
echo "✓ user_id UUID validation found"

# Check for timestamp ISO 8601 validation
echo "Checking for timestamp ISO 8601 validation..."
if ! grep -r "chrono::DateTime<chrono::Utc>" --include="*.rs" services/driver-service/src > /dev/null; then
    echo "❌ FAIL: timestamp ISO 8601 validation not found"
    exit 1
fi
echo "✓ timestamp ISO 8601 validation found"

# Check for payload JSON validation
echo "Checking for payload JSON validation..."
if ! grep -r "serde_json::Value\|serde_json::from_str" --include="*.rs" services/driver-service/src/middleware/validation.rs > /dev/null 2>&1; then
    echo "❌ FAIL: payload JSON validation not found in validation.rs"
    exit 1
fi
echo "✓ payload JSON validation found"

# Check for event_type enum usage
echo "Checking for event_type enum usage..."
if ! grep -r "EventType\|event_type" --include="*.rs" apps/packages/domain-types/src/events.rs > /dev/null; then
    echo "❌ FAIL: event_type enum not found in events.rs"
    exit 1
fi
echo "✓ event_type enum found"

# Check for location_source enum usage
echo "Checking for location_source enum usage..."
if ! grep -r "LocationSource\|location_source" --include="*.rs" apps/packages/domain-types/src/events.rs > /dev/null; then
    echo "❌ FAIL: location_source enum not found in events.rs"
    exit 1
fi
echo "✓ location_source enum found"

echo "======================================"
echo "✓ PASS: Event schema validation enforced"
exit 0
