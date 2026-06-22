#!/bin/bash
set -euo pipefail

# CI Gate: Session Consistency Check (CI-1.4)
# Extract role from JWT test vector, compare to platform_db role for same UUID, FAIL on mismatch

echo "=== CI Gate: Session Consistency ==="

# Get JWT test vector from environment or use default
# Format: sub,user_uuid,email,role,exp,iat,iss,aud
JWT_TEST_VECTOR="${JWT_TEST_VECTOR:-driver-uuid-123456789012-123456789012-driver@borne.map,d6f5e4c3b2a1000000000000,driver@borne.map,driver,9999999999,1700000000,http://localhost:8080/realms/bornemap,driver-service-sa}"

# Parse JWT test vector
read -r sub user_uuid email role exp iat iss aud <<< "$JWT_TEST_VECTOR"

# Check if role is valid
if [ "$role" != "driver" ] && [ "$role" != "partner" ] && [ "$role" != "admin" ]; then
    echo "FAIL: Invalid role in JWT test vector: $role"
    exit 1
fi

echo "Testing session consistency for user: $sub ($user_uuid) with role: $role"

# Get user profile from platform_db
user_profile=$(psql "$DATABASE_URL" -tAc "
    SELECT role FROM user_profiles
    WHERE user_uuid = '$user_uuid';
" 2>/dev/null || echo "")

if [ -z "$user_profile" ]; then
    echo "FAIL: No user profile found for UUID: $user_uuid"
    exit 1
fi

# Compare roles
if [ "$user_profile" != "$role" ]; then
    echo "FAIL: Role mismatch in platform_db"
    echo "  JWT claims role:    $role"
    echo "  DB user_profiles.role: $user_profile"
    exit 1
fi

echo "PASS: Session consistency check passed (JWT role = DB role)"
exit 0
