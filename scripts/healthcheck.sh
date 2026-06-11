#!/usr/bin/env bash
set -euo pipefail

echo "🔍 Running health checks..."
echo ""

all_healthy=true
start_time=$(date +%s)

check_service() {
  local name=$1
  local container=$2
  local status

  status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unreachable")

  if [ "$status" = "healthy" ]; then
    echo "  ✅ $name → healthy"
  elif [ "$status" = "unreachable" ]; then
    echo "  ❌ $name → container not found"
    all_healthy=false
  else
    echo "  ⏳ $name → $status"
    all_healthy=false
  fi
}

check_service "platform_db" "bornemap-platform-db"
check_service "analytics_db" "bornemap-analytics-db"
check_service "Keycloak" "bornemap-keycloak"

end_time=$(date +%s)
elapsed=$((end_time - start_time))

echo ""
if [ "$all_healthy" = true ]; then
  echo "✅ All services healthy (${elapsed}s)"
else
  echo "⚠️  Some services are not healthy yet (${elapsed}s)"
  echo "   Run again in a few seconds, or check logs:"
  echo "   docker logs bornemap-platform-db --tail 20"
  echo "   docker logs bornemap-analytics-db --tail 20"
  echo "   docker logs bornemap-keycloak --tail 20"
  exit 1
fi

# Startup timing assertion
if [ "$elapsed" -gt 60 ]; then
  echo "⚠️  Warning: healthcheck completed in ${elapsed}s (target: < 60s)"
fi
