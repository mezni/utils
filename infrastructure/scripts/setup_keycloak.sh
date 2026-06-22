#!/bin/bash
set -e

# Keycloak Setup Script for BorneMap
# Sets up Keycloak container and realm configuration

echo "=== BorneMap Keycloak Setup ==="
echo ""

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
  echo "ERROR: docker-compose is not installed"
  exit 1
fi

KEYCLOAK_PORT=8080
KEYCLOAK_CONTAINER_NAME="bornemap_keycloak"
KEYCLOAK_ADMIN_USER="admin"
KEYCLOAK_ADMIN_PASSWORD="admin"

echo "Starting Keycloak container..."
docker run -d \
  --name $KEYCLOAK_CONTAINER_NAME \
  --network bornemap-network \
  -p $KEYCLOAK_PORT:8080 \
  -e KEYCLOAK_ADMIN=$KEYCLOAK_ADMIN_USER \
  -e KEYCLOAK_ADMIN_PASSWORD=$KEYCLOAK_ADMIN_PASSWORD \
  -e KC_DB=postgres \
  -e KC_DB_URL=jdbc:postgresql://platform-db:5432/keycloak_db \
  -e KC_DB_USERNAME=bornemap_admin \
  -e KC_DB_PASSWORD=bornemap_password \
  -v keycloak_data:/opt/keycloak/data \
  quay.io/keycloak/keycloak:22.0.3 start-dev

echo ""
echo "=== Keycloak Setup Complete ==="
echo ""
echo "Keycloak is running on port $KEYCLOAK_PORT"
echo "Admin console: http://localhost:$KEYCLOAK_PORT/admin"
echo "Admin user: $KEYCLOAK_ADMIN_USER / $KEYCLOAK_ADMIN_PASSWORD"
echo ""
echo "Next steps:"
echo "  1. Open Keycloak admin console"
echo "  2. Create bornemap realm"
echo "  3. Configure clients and users"
echo "  4. Export realm to infrastructure/realm-bornemap.json"
