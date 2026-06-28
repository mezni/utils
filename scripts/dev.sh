#!/usr/bin/env bash
set -euo pipefail

echo "Starting dev environment..."

if ! docker compose up -d postgres; then
  echo "Failed to start PostgreSQL. Is Docker running?"
  exit 1
fi

echo "PostgreSQL is ready."
echo "Run a service with: cargo run -p <service-name>"
echo "  e.g., cargo run -p auth-service"
