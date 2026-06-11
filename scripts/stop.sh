#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../infra"

echo "🛑 Stopping BorneMap infrastructure..."
docker compose down

echo "✅ BorneMap infrastructure stopped."
echo ""
echo "To also remove volumes (WARNING: deletes all data):"
echo "  docker compose -f infra/docker-compose.yml down -v"
