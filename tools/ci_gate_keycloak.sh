#!/bin/bash
set -euo pipefail

# CI Gate: Keycloak Dependency (CI-1.2)
# Checks:
# - FAIL if non-auth-service depends on keycloak-client
# - scan Rust imports for `use keycloak` outside auth-service

echo "=== CI Gate: Keycloak Dependency ==="

# Check 1: Scan for keycloak-client dependency
echo "Checking Cargo.toml for keycloak-client dependency..."

# Find all Cargo.toml files in services/
CRATE_TOMLS=$(find services/ -name "Cargo.toml" -type f)

NON_AUTH_SERVICES=()

for crate_toml in $CRATE_TOMLS; do
    crate_name=$(basename "$(dirname "$crate_toml")")
    if [ "$crate_name" != "auth-service" ]; then
        if grep -q "keycloak-client" "$crate_toml" 2>/dev/null; then
            NON_AUTH_SERVICES+=("$crate_name")
        fi
    fi
done

if [ ${#NON_AUTH_SERVICES[@]} -gt 0 ]; then
    echo "FAIL: Non-auth-service crates depend on keycloak-client:"
    for service in "${NON_AUTH_SERVICES[@]}"; do
        echo "  - $service/Cargo.toml"
    done
    exit 1
fi

# Check 2: Scan Rust imports for keycloak outside auth-service
echo "Checking Rust imports for keycloak..."

KEYCLOAK_IMPORTS=$(find services/ -name "*.rs" -type f -exec grep -l "use keycloak" {} \; 2>/dev/null)

if [ -n "$KEYCLOAK_IMPORTS" ]; then
    echo "FAIL: Keycloak imports found outside auth-service:"
    echo "$KEYCLOAK_IMPORTS" | head -10
    exit 1
fi

echo "PASS: Keycloak dependency checks passed"
exit 0
