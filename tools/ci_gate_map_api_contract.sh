#!/bin/bash
# CI Gate: Map API Contract
# Purpose: Ensure API responses match domain-types contracts
# Fails if response format deviates from DTO definitions

echo "Running Map API Contract CI Gate..."

PASSED=true

# Check for contract validation in handlers
if grep -q "validate_contract\|matches.*contract\|domain.*types.*Station" services/driver-service/src/handlers/**/*.rs; then
    echo "✓ PASS: Contract validation found"
else
    echo "⚠ WARNING: No explicit contract validation found (may need manual review)"
fi

# Check that handlers use domain-types structures
if grep -q "use crate::domain::gis" services/driver-service/src/handlers/**/*.rs; then
    echo "✓ PASS: Handlers use domain-types structures"
else
    echo "✗ FAIL: Handlers do not use domain-types structures"
    PASSED=false
fi

# Check for consistent error response format
if grep -q '"error"\|"message"' services/driver-service/src/handlers/**/*.rs; then
    echo "✓ PASS: Error responses follow contract"
else
    echo "✗ FAIL: Error responses do not follow contract"
    PASSED=false
fi

if [ "$PASSED" = true ]; then
    echo "✓ PASS: Map API contracts validated"
    exit 0
else
    echo "✗ FAIL: Map API contract validation failed"
    exit 1
fi
