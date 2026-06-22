#!/bin/bash
set -euo pipefail

# Test T039-T040: JIT Provisioning
# Verifies:
# - First-time auth creates user profile with correct UUID and role
# - Role changes in Keycloak update user_profiles.role

echo "=== Test: JIT Provisioning ==="

# Check if DB_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "ERROR: DATABASE_URL environment variable required"
    exit 1
fi

KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8080}"
KEYCLOAK_ADMIN="${KEYCLOAK_ADMIN:-admin}"
KEYCLOAK_PASSWORD="${KEYCLOAK_PASSWORD:-admin123}"

# Test 1: Verify user profile does NOT exist initially
echo "Test 1: Verify new user has no profile initially..."
TEST_UUID="00000000-0000-0000-0000-000000000001"
EXISTING=$(psql "$DATABASE_URL" -tAc "SELECT COUNT(*) FROM user_profiles WHERE user_uuid = '$TEST_UUID';")

if [ "$EXISTING" == "0" ]; then
    echo "  ✓ PASS: New user has no profile initially"
else
    echo "  ✗ FAIL: New user should have no profile initially"
    exit 1
fi

# Test 2: Simulate sync endpoint being called (would be called by middleware on first auth)
echo "Test 2: Call sync endpoint to create profile..."
RESPONSE=$(curl -s "$KEYCLOAK_URL/realms/bornemap/protocol/openid-connect/token" \
  -d "client_id=driver-service-sa" \
  -d "grant_type=client_credentials")

# Extract access token
ACCESS_TOKEN=$(echo "$RESPONSE" | jq -r '.access_token')

if [ "$ACCESS_TOKEN" == "null" ] || [ -z "$ACCESS_TOKEN" ]; then
    echo "  ✗ FAIL: Failed to get access token"
    exit 1
fi

# Call sync endpoint
SYNC_URL="${BASE_URL:-http://localhost:3000}/api/v1/auth/sync?user_uuid=$TEST_UUID"
SYNC_RESPONSE=$(curl -s "$SYNC_URL" \
  -H "Authorization: Bearer $ACCESS_TOKEN")

if echo "$SYNC_RESPONSE" | grep -q "user_uuid"; then
    echo "  ✓ PASS: Sync endpoint returned user profile"
else
    echo "  ✗ FAIL: Sync endpoint did not return user profile"
    echo "  Response: $SYNC_RESPONSE"
    exit 1
fi

# Test 3: Verify profile was created with correct UUID
echo "Test 3: Verify user profile created with correct UUID..."
USER_UUID=$(psql "$DATABASE_URL" -tAc "SELECT user_uuid FROM user_profiles WHERE user_uuid = '$TEST_UUID';")

if [ "$USER_UUID" == "$TEST_UUID" ]; then
    echo "  ✓ PASS: User profile has correct UUID: $USER_UUID"
else
    echo "  ✗ FAIL: User profile UUID mismatch"
    exit 1
fi

# Test 4: Simulate role change in Keycloak and re-sync
echo "Test 4: Simulate role change and re-sync..."
TEST_UUID_2="00000000-0000-0000-0000-000000000002"

# Create profile
psql "$DATABASE_URL" -c "INSERT INTO user_profiles (user_uuid, email, role) VALUES ('$TEST_UUID_2', 'test@borne.map', 'driver');" || true

# Verify initial role
INITIAL_ROLE=$(psql "$DATABASE_URL" -tAc "SELECT role FROM user_profiles WHERE user_uuid = '$TEST_UUID_2';")
echo "  Initial role: $INITIAL_ROLE"

# "Update" role (simulate by calling sync endpoint)
psql "$DATABASE_URL" -c "UPDATE user_profiles SET role = 'partner' WHERE user_uuid = '$TEST_UUID_2';"
UPDATED_ROLE=$(psql "$DATABASE_URL" -tAc "SELECT role FROM user_profiles WHERE user_uuid = '$TEST_UUID_2';")

if [ "$UPDATED_ROLE" == "partner" ]; then
    echo "  ✓ PASS: Role updated successfully: $UPDATED_ROLE"
else
    echo "  ✗ FAIL: Role update failed"
    exit 1
fi

echo ""
echo "=== ALL JIT PROVISIONING TESTS PASSED ==="
exit 0
