#!/bin/bash
# Zero-State Docker Compose Verification
# Usage: ./scripts/verify-zero-state.sh
set -e

echo "=== Zero-State Docker Test ==="
echo "Stopping any running containers and removing volumes..."
docker compose down -v 2>/dev/null || true

echo "Building and starting backend services (postgres, driver-service, admin-service)..."
docker compose up --build -d postgres driver-service admin-service

echo "Waiting 90s for health checks..."
sleep 90

echo ""
echo "=== Container Health Status ==="
docker compose ps --filter health=healthy

echo ""
echo "=== Health Endpoint Checks ==="
DRIVER_STATUS=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/api/health || echo "000")
ADMIN_STATUS=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8081/api/health || echo "000")
echo "Driver Service (/api/health): $DRIVER_STATUS"
echo "Admin Service (/api/health):  $ADMIN_STATUS"

if [ "$DRIVER_STATUS" = "200" ] && [ "$ADMIN_STATUS" = "200" ]; then
    echo ""
    echo "=== PASS: All services healthy ==="
    exit 0
else
    echo ""
    echo "=== FAIL: One or more services unhealthy ==="
    docker compose logs --tail=20 driver-service admin-service 2>/dev/null || true
    exit 1
fi
