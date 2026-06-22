#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8080}"
KEYCLOAK_ADMIN="${KEYCLOAK_ADMIN:-admin}"
KEYCLOAK_ADMIN_PASSWORD="${KEYCLOAK_ADMIN_PASSWORD:-admin123}"
REALM_NAME="${REALM_NAME:-bornemap}"

echo "=== Keycloak Setup: $REALM_NAME ==="

echo "1. Getting admin access token..."
ADMIN_TOKEN=$(curl -s -X POST "$KEYCLOAK_URL/realms/master/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=admin-cli" \
  -d "username=$KEYCLOAK_ADMIN" \
  -d "password=$KEYCLOAK_ADMIN_PASSWORD" \
  -d "grant_type=password" | jq -r '.access_token')

if [ -z "$ADMIN_TOKEN" ] || [ "$ADMIN_TOKEN" = "null" ]; then
  echo "ERROR: Failed to get admin token. Is Keycloak running?"
  exit 1
fi

echo "2. Checking if realm '$REALM_NAME' exists..."
REALM_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  "$KEYCLOAK_URL/admin/realms/$REALM_NAME")

if [ "$REALM_EXISTS" = "200" ]; then
  echo "   Realm '$REALM_NAME' already exists."
else
  echo "2. Creating realm '$REALM_NAME'..."
  curl -s -X POST "$KEYCLOAK_URL/admin/realms" \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"realm\": \"$REALM_NAME\",
      \"enabled\": true,
      \"displayName\": \"BorneMap Platform\",
      \"registrationAllowed\": false,
      \"loginWithEmailAllowed\": true,
      \"bruteForceProtected\": true
    }"
  echo "   Realm created."
fi

echo "3. Creating realm roles..."
for ROLE in driver partner admin; do
  ROLE_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "$KEYCLOAK_URL/admin/realms/$REALM_NAME/roles/$ROLE")

  if [ "$ROLE_EXISTS" != "200" ]; then
    echo "   Creating role: $ROLE"
    curl -s -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/roles" \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"name\": \"$ROLE\", \"description\": \"BorneMap $ROLE role\"}"
  else
    echo "   Role '$ROLE' already exists."
  fi
done

echo "4. Creating clients..."
declare -A CLIENTS
CLIENTS[mobile-driver]='{"clientId":"mobile-driver","name":"Driver Mobile App","publicClient":true,"standardFlowEnabled":true,"redirectUris":["*"],"webOrigins":["+"]}'
CLIENTS[web-driver]='{"clientId":"web-driver","name":"Driver Web App","publicClient":true,"standardFlowEnabled":true,"redirectUris":["http://localhost:3001/*"],"webOrigins":["http://localhost:3001"]}'
CLIENTS[admin-dashboard]='{"clientId":"admin-dashboard","name":"Admin Dashboard","publicClient":false,"standardFlowEnabled":true,"redirectUris":["http://localhost:3002/*"],"webOrigins":["http://localhost:3002"]}'
CLIENTS[auth-service-sa]='{"clientId":"auth-service-sa","name":"Auth Service Account","publicClient":false,"serviceAccountsEnabled":true,"standardFlowEnabled":false,"directAccessGrantsEnabled":false}'
CLIENTS[driver-service-sa]='{"clientId":"driver-service-sa","name":"Driver Service Account","publicClient":false,"serviceAccountsEnabled":true,"standardFlowEnabled":false,"directAccessGrantsEnabled":false}'
CLIENTS[admin-service-sa]='{"clientId":"admin-service-sa","name":"Admin Service Account","publicClient":false,"serviceAccountsEnabled":true,"standardFlowEnabled":false,"directAccessGrantsEnabled":false}'

for CLIENT_ID in "${!CLIENTS[@]}"; do
  CLIENT_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "$KEYCLOAK_URL/admin/realms/$REALM_NAME/clients?clientId=$CLIENT_ID")

  if [ "$CLIENT_EXISTS" != "200" ] || [ "$(curl -s -H "Authorization: Bearer $ADMIN_TOKEN" "$KEYCLOAK_URL/admin/realms/$REALM_NAME/clients?clientId=$CLIENT_ID" | jq 'length')" = "0" ]; then
    echo "   Creating client: $CLIENT_ID"
    curl -s -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/clients" \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "Content-Type: application/json" \
      -d "${CLIENTS[$CLIENT_ID]}"
  else
    echo "   Client '$CLIENT_ID' already exists."
  fi
done

echo "5. Creating test users..."
declare -A USERS
USERS[admin]='{"username":"admin","email":"admin@bornemap.local","enabled":true,"credentials":[{"type":"password","value":"admin123","temporary":false}],"realmRoles":["admin"]}'
USERS[driver]='{"username":"driver","email":"driver@bornemap.local","enabled":true,"credentials":[{"type":"password","value":"driver123","temporary":false}],"realmRoles":["driver"]}'
USERS[partner]='{"username":"partner","email":"partner@bornemap.local","enabled":true,"credentials":[{"type":"password","value":"partner123","temporary":false}],"realmRoles":["partner"]}'

for USERNAME in "${!USERS[@]}"; do
  USER_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    "$KEYCLOAK_URL/admin/realms/$REALM_NAME/users?username=$USERNAME")

  if [ "$USER_EXISTS" != "200" ]; then
    echo "   Creating user: $USERNAME"
    curl -s -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/users" \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      -H "Content-Type: application/json" \
      -d "${USERS[$USERNAME]}"
  else
    echo "   User '$USERNAME' already exists."
  fi
done

echo ""
echo "=== Keycloak setup complete ==="
echo "Realm:     $REALM_NAME"
echo "Clients:   ${!CLIENTS[*]}"
echo "Users:     ${!USERS[*]}"
echo ""
echo "Service account secrets can be retrieved from Keycloak admin console:"
echo "  http://localhost:8080/admin/master/console/#/realms/$REALM_NAME"
