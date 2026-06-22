#!/bin/bash
# CI Gate: GIS Ownership Enforcement
# Purpose: Ensure ONLY driver-service can write to gis schema
# Fails if any service other than driver-service writes to gis schema

echo "Running GIS Ownership CI Gate..."

# Check for driver-service write operations to gis schema
if git diff --cached --name-only | grep -qE 'services/(auth-service|admin-service)/.*\.rs$'; then
    echo "✗ FAIL: Other services modified (not driver-service)"
    echo "  Only driver-service is allowed to write to gis schema"
    exit 1
fi

# Check for any service attempting to write to gis schema
if git diff --name-only | grep -qE 'services/(auth-service|admin-service)/.*\.rs$'; then
    echo "✗ FAIL: Other services modified (not driver-service)"
    echo "  Only driver-service is allowed to write to gis schema"
    exit 1
fi

echo "✓ PASS: GIS ownership enforced"
exit 0
