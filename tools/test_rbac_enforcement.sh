#!/bin/bash
set -euo pipefail

# Test T030: RBAC Enforcement
# Verifies:
# - 403 for insufficient role
# - 401 for invalid token
# - 200 for correct role

echo "=== Test: RBAC Enforcement ==="

BASE_URL="${BASE_URL:-http://localhost:3001}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"
DRIVER_TOKEN="${DRIVER_TOKEN:-}"

# Check if tokens are available
if [ -z "$ADMIN_TOKEN" ] || [ -z "$DRIVER_TOKEN" ]; then
    echo "ERROR: ADMIN_TOKEN and DRIVER_TOKEN environment variables required"
    echo "Please set them to test RBAC enforcement"
    exit 1
fi

# Test 1: Valid driver token should access driver endpoint
echo "Test 1: Driver accessing driver endpoint..."
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $DRIVER_TOKEN" \
    "$BASE_URL/api/v1/drivers")

if [ "$STATUS" == "200" ]; then
    echo "  ✓ PASS: Driver can access driver endpoint (200)"
else
    echo "  ✗ FAIL: Driver cannot access driver endpoint (got $STATUS, expected 200)"
    exit 1
fi

# Test 2: Valid admin token should access admin endpoint
echo "Test 2: Admin accessing admin endpoint..."
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "$BASE_URL/api/v1/admin/users")

if [ "$STATUS" == "200" ]; then
    echo "  ✓ PASS: Admin can access admin endpoint (200)"
else
    echo "  ✗ FAIL: Admin cannot access admin endpoint (got $STATUS, expected 200)"
    exit 1
fi

# Test 3: Driver token should be blocked from admin endpoint
echo "Test 3: Driver accessing admin endpoint (should fail)..."
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $DRIVER_TOKEN" \
    "$BASE_URL/api/v1/admin/users")

if [ "$STATUS" == "403" ]; then
    echo "  ✓ PASS: Driver blocked from admin endpoint (403)"
else
    echo "  ✗ FAIL: Driver should be blocked (got $STATUS, expected 403)"
    exit 1
fi

# Test 4: Admin token should be blocked from driver-only endpoint (if exists)
echo "Test 4: Admin accessing driver-only endpoint (should fail)..."
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "$BASE_URL/api/v1/admin/drivers")

if [ "$STATUS" == "403" ]; then
    echo "  ✓ PASS: Admin blocked from driver-only endpoint (403)"
else
    echo "  ✗ FAIL: Admin should be blocked (got $STATUS, expected 403)"
    exit 1
fi

# Test 5: Invalid token should return 401
echo "Test 5: Invalid token..."
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer invalid_token" \
    "$BASE_URL/api/v1/drivers")

if [ "$STATUS" == "401" ]; then
    echo "  ✓ PASS: Invalid token rejected (401)"
else
    echo "  ✗ FAIL: Invalid token should return 401 (got $STATUS)"
    exit 1
fi

# Test 6: No token should return 401
echo "Test 6: No token provided..."
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    "$BASE_URL/api/v1/drivers")

if [ "$STATUS" == "401" ]; then
    echo "  ✓ PASS: No token rejected (401)"
else
    echo "  ✗ FAIL: No token should return 401 (got $STATUS)"
    exit 1
fi

echo ""
echo "=== ALL RBAC TESTS PASSED ==="
exit 0
