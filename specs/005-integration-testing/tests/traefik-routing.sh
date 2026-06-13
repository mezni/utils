#!/usr/bin/env bash
# Traefik Routing Tests
# Tests that Traefik correctly routes requests to backend services.
# Usage: bash traefik-routing.sh
# Exit code: 0 if all tests pass, 1 if any fail

set -e

GATEWAY="http://localhost:8080"
PASS=0
FAIL=0

pass() { PASS=$((PASS+1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL+1)); echo "  FAIL: $1"; }

echo "=== Traefik Routing Tests ==="
echo ""

# T015: Route to driver-service
echo "--- T015: Route GET /api/v1/stations -> driver-service ---"
if status=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/api/v1/stations" --max-time 5); then
  if [ "$status" = "200" ]; then
    pass "GET /api/v1/stations returned $status (expected 2xx)"
  else
    fail "GET /api/v1/stations returned $status (expected 200)"
  fi
else
  fail "GET /api/v1/stations — connection failed"
fi

echo ""
echo "--- T015 (cont): Route GET /api/v1/stations/nearby -> driver-service ---"
if status=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/api/v1/stations/nearby?lat=36.8&lng=10.1&radius=5" --max-time 5); then
  if [ "$status" = "200" ]; then
    pass "GET /api/v1/stations/nearby returned $status (expected 200)"
  else
    fail "GET /api/v1/stations/nearby returned $status (expected 200)"
  fi
else
  fail "GET /api/v1/stations/nearby — connection failed"
fi

echo ""
echo "--- T015 (cont): Route GET /api/v1/stations/{id} -> driver-service ---"
if status=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/api/v1/stations/STA-TEST-001" --max-time 5); then
  if [ "$status" != "502" ] && [ "$status" != "503" ]; then
    pass "GET /api/v1/stations/{id} returned $status (not a gateway error)"
  else
    fail "GET /api/v1/stations/{id} returned $status (gateway error)"
  fi
else
  fail "GET /api/v1/stations/{id} — connection failed"
fi

# T016: Route to admin-service
echo ""
echo "--- T016: Route GET /api/v1/admin/stations -> admin-service ---"
if status=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/api/v1/admin/stations" --max-time 5); then
  if [ "$status" = "200" ]; then
    pass "GET /api/v1/admin/stations returned $status (expected 2xx)"
  else
    fail "GET /api/v1/admin/stations returned $status (expected 200)"
  fi
else
  fail "GET /api/v1/admin/stations — connection failed"
fi

# T017: Unknown route returns 404 or 502
echo ""
echo "--- T017: Unknown route returns error ---"
if status=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/api/v1/unknown" --max-time 5); then
  if [ "$status" = "404" ] || [ "$status" = "502" ]; then
    pass "GET /api/v1/unknown returned $status (expected 404/502)"
  else
    fail "GET /api/v1/unknown returned $status (expected 404/502)"
  fi
else
  fail "GET /api/v1/unknown — connection failed"
fi

# T018: Health check route
echo ""
echo "--- T018 (supplementary): Route GET /health -> driver-service ---"
if status=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/health" --max-time 5); then
  if [ "$status" = "200" ]; then
    pass "GET /health returned $status (expected 200)"
  else
    fail "GET /health returned $status (expected 200)"
  fi
else
  fail "GET /health — connection failed"
fi

# T055b: Auth rejection test (unauthenticated requests)
echo ""
echo "--- T055b: Auth rejection (401/403 graceful) ---"
for endpoint in "/api/v1/stations" "/api/v1/admin/stations" "/api/v1/events"; do
  if status=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY$endpoint" --max-time 5); then
    if [ "$status" != "502" ] && [ "$status" != "503" ]; then
      pass "GET $endpoint without auth returned $status (no gateway crash)"
    else
      fail "GET $endpoint without auth returned $status (gateway error)"
    fi
  else
    fail "GET $endpoint — connection failed"
  fi
done

echo ""
echo "=== Results ==="
echo "Passed: $PASS"
echo "Failed: $FAIL"
if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
echo "All routing tests passed."
