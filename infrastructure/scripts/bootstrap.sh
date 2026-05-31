#!/usr/bin/env bash
set -euo pipefail

echo "=== BorneMap Server Bootstrap ==="

# Install dependencies
apt-get update
apt-get install -y docker.io docker-compose-v2

# Clone repo
git clone https://github.com/anomalyco/BorneMap.git /opt/bornemap
cd /opt/bornemap

# Configure environment
cp infrastructure/env/.env.example .env

# Start stack
docker compose -f infrastructure/compose/docker-compose.yml up -d

echo "=== Bootstrap complete ==="
