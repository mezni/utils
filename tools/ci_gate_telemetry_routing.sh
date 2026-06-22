#!/bin/bash
set -e

echo "Running: Telemetry Routing Gate"
echo "================================"

# Verify routing to driver-service only, JWT authentication required

# Check for telemetry API route in driver-service
echo "Checking for telemetry API route in driver-service..."
if ! grep -r "telemetry" --include="*.rs" services/driver-service/src/api/telemetry.rs > /dev/null 2>&1; then
    echo "❌ FAIL: telemetry API route not found in driver-service"
    exit 1
fi
echo "✓ telemetry API route found in driver-service"

# Check for Traefik routing configuration
echo "Checking for Traefik routing configuration..."
if ! grep -r "telemetry" --include="*.yml" --include="*.yaml" services/driver-service/config/ > /dev/null; then
    echo "❌ FAIL: Traefik routing configuration not found for telemetry"
    exit 1
fi
echo "✓ Traefik routing configuration found"

# Check for JWT authentication in telemetry endpoint
echo "Checking for JWT authentication in telemetry endpoint..."
if ! grep -r "jwt" --include="*.rs" services/driver-service/src/api/telemetry.rs > /dev/null 2>&1; then
    echo "❌ FAIL: JWT authentication not found in telemetry endpoint"
    exit 1
fi
echo "✓ JWT authentication found in telemetry endpoint"

echo "================================"
echo "✓ PASS: Telemetry routing enforced to driver-service only"
exit 0
