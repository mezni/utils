#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> BorneMap Dev Environment Setup"
echo ""

# Copy .env if missing
if [ ! -f .env ]; then
  cp .env.example .env
  echo "[ok] .env created from .env.example"
else
  echo "[skip] .env already exists"
fi

# Create infra directories
mkdir -p infra/db
echo "[ok] infra/ directories ready"

# Ensure init SQL exists
if [ ! -f infra/db/init-platform-db.sql ]; then
  echo "[warn] infra/db/init-platform-db.sql not found"
  echo "  Generate it from docs/spec/db-schema.md when ready."
fi

# Check Docker
if ! command -v docker &>/dev/null; then
  echo "[error] Docker not found. Install Docker first."
  exit 1
fi

if ! docker compose version &>/dev/null; then
  echo "[error] Docker Compose v2 not found."
  exit 1
fi

echo ""
echo "==> Ready. Run 'make up' to start infrastructure."
