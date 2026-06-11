#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../infra"

echo "⚠️  WARNING: This will remove all data volumes!"
read -rp "Are you sure? (yes/NO): " confirm

if [ "$confirm" != "yes" ]; then
  echo "Aborted."
  exit 0
fi

echo "🛑 Stopping and removing all containers and volumes..."
docker compose down -v
echo "✅ Done. All containers stopped, volumes removed."
echo "Run scripts/start.sh to rebuild from scratch."
