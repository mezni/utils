#!/bin/bash
# CI Gate: Redis Access Isolation
# Purpose: Ensure ONLY driver-service can write to Redis
# Fails if Redis accessed outside driver-service

echo "Running Redis Access CI Gate..."

# Check for driver-service Redis access
if grep -q "redis::Client\|redis::Connection" services/driver-service/src/**/*.rs; then
    echo "✓ PASS: Driver-service uses Redis correctly"
else
    echo "✗ FAIL: Driver-service does not use Redis"
    exit 1
fi

# Check for other services using Redis
if grep -q "redis::Client\|redis::Connection" services/(auth-service|admin-service)/src/**/*.rs; then
    echo "✗ FAIL: Other services attempting to access Redis"
    exit 1
fi

echo "✓ PASS: Redis access isolated to driver-service"
exit 0
