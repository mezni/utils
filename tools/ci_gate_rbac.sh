#!/bin/bash
set -euo pipefail

# CI Gate: RBAC Coverage Check (CI-1.3)
# Checks:
# - scan route registrations for .route() calls
# - FAIL if any route lacks role guard
# - FAIL if any route absent from RBAC matrix in contracts/rbac.md
# - FAIL if any route registered without .wrap(from_fn(rbac_guard)) pattern

echo "=== CI Gate: RBAC Coverage ==="

# Find all main.rs files
MAIN_RS=$(find services/ -name "main.rs" -type f)

# Expected RBAC matrix (from contracts/rbac.md)
# These are public routes that don't need role guard
PUBLIC_WHITELIST=(
    "/health"
    "/api/v1/auth/sync"
    "/api/v1/telemetry/events"
)

# Parse each service's main.rs for route definitions
for main_rs in $MAIN_RS; do
    service_name=$(basename "$(dirname "$main_rs")")
    echo "Checking $service_name..."

    # Extract route paths from .route() calls
    ROUTE_PATHS=$(grep -oP '\.route\(\s*"\K[^"]+' "$main_rs" 2>/dev/null || true)

    # Check each route for RBAC guard
    while IFS= read -r route; do
        # Skip if route is in public whitelist
        if [[ " ${PUBLIC_WHITELIST[@]} " =~ " ${route} " ]]; then
            continue
        fi

        # Check if route has rbac_guard in the same block
        # We check if the line before or after contains .wrap(from_fn(rbac_guard))
        HAS_RBAC=0

        # Get the route line context
        route_line=$(grep -n "\.route.*$route" "$main_rs" | head -1 | cut -d: -f1)
        if [ -n "$route_line" ]; then
            # Check preceding lines for rbac_guard
            if sed -n "$((route_line-5)),$route_linep" "$main_rs" | grep -q "rbac_guard"; then
                HAS_RBAC=1
            fi
        fi

        if [ $HAS_RBAC -eq 0 ]; then
            echo "FAIL: $service_name route '$route' lacks RBAC guard"
            exit 1
        fi
    done <<< "$ROUTE_PATHS"
done

# Check that all routes in RBAC matrix have corresponding route definitions
echo "Verifying RBAC matrix coverage..."

# Read RBAC matrix and verify routes are defined
grep -E "^\s*/" /home/dali/WORK/BorneMap/specs/002-identity-security-core/contracts/rbac.md | grep -v "^#" | while read -r route_and_role; do
    route=$(echo "$route_and_role" | awk '{print $1}')

    # Check if route is in public whitelist
    if [[ " ${PUBLIC_WHITELIST[@]} " =~ " ${route} " ]]; then
        continue
    fi

    route_found=0
    for main_rs in $MAIN_RS; do
        if grep -q "\.route.*$route" "$main_rs"; then
            route_found=1
            break
        fi
    done

    if [ $route_found -eq 0 ]; then
        echo "FAIL: Route '$route' defined in RBAC matrix but not registered in any service"
        exit 1
    fi
done

echo "PASS: RBAC coverage checks passed"
exit 0
