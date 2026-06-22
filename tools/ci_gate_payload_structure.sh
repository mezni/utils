#!/bin/bash
set -e

echo "Running: Payload Structure Validation Gate"
echo "==========================================="

# Check for JSON payload validation and structure

# Check for JSON payload validation
echo "Checking for JSON payload validation..."
if ! grep -r "serde_json::Value\|serde_json::from_str\|serde_json::from_value" --include="*.rs" services/driver-service/src/middleware/validation.rs > /dev/null 2>&1; then
    echo "❌ FAIL: JSON payload validation not found in validation.rs"
    exit 1
fi
echo "✓ JSON payload validation found"

# Check for object type validation
echo "Checking for object type validation..."
if ! grep -r "serde_json::Value::Object\|is_object" --include="*.rs" services/driver-service/src/middleware/validation.rs > /dev/null 2>&1; then
    echo "❌ FAIL: object type validation not found"
    exit 1
fi
echo "✓ object type validation found"

# Check for nested field type validation
echo "Checking for nested field type validation..."
if ! grep -r "payload\[" --include="*.rs" services/driver-service/src/middleware/validation.rs > /dev/null 2>&1; then
    echo "❌ FAIL: nested field type validation not found"
    exit 1
fi
echo "✓ nested field type validation found"

# Check for error handling for malformed payloads
echo "Checking for error handling for malformed payloads..."
if ! grep -r "BadRequest\|400\|Error::validation\|Validation" --include="*.rs" services/driver-service/src/middleware/validation.rs > /dev/null 2>&1; then
    echo "❌ FAIL: error handling for malformed payloads not found"
    exit 1
fi
echo "✓ error handling for malformed payloads found"

echo "==========================================="
echo "✓ PASS: Payload structure validation enforced"
exit 0
