#!/usr/bin/env bash
set -euo pipefail

echo "🔍 Checking prerequisites..."

# Check Docker
if ! command -v docker &>/dev/null; then
  echo "❌ Docker is not installed."
  echo "   Install Docker Desktop or Docker Engine: https://docs.docker.com/get-docker/"
  exit 1
fi

# Check Docker Compose
if ! docker compose version &>/dev/null; then
  echo "❌ Docker Compose is not installed or not available."
  echo "   Install Docker Compose: https://docs.docker.com/compose/install/"
  exit 1
fi

# Check port availability
check_port() {
  local port=$1
  local name=$2
  if ss -tuln "sport = :$port" 2>/dev/null | grep -q ":$port "; then
    echo "❌ Port $port ($name) is already in use."
    echo "   Free the port or change the mapping in infra/docker-compose.yml"
    exit 1
  fi
}

check_port 5432 "platform_db"
check_port 5433 "analytics_db"
check_port 8083 "Keycloak"

echo "✅ All prerequisites met."
echo "🚀 Starting BorneMap infrastructure..."
cd "$(dirname "$0")/../infra"
docker compose up -d

echo ""
echo "✅ BorneMap infrastructure is running:"
echo "   platform_db  → localhost:5432"
echo "   analytics_db → localhost:5433"
echo "   Keycloak     → localhost:8083"
