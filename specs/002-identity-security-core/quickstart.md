# Quickstart: Identity & Security Core

**Date**: 2026-06-21
**Branch**: `002-identity-security-core`

## Prerequisites

- Sprint 0 completed (services running, databases provisioned)
- Docker & docker-compose installed
- Rust 1.75+

## Setup

### 1. Start Keycloak

```bash
cd infrastructure/docker-compose
docker compose -f local.yml up -d keycloak
```

### 2. Configure Keycloak Realm

```bash
# Import realm configuration
docker compose exec -T keycloak /opt/keycloak/bin/kc.sh import \
  --file /opt/keycloak/data/import/realm-bornemap.json
```

### 3. Add Keycloak to Service Database URLs

Update docker-compose environment variables or config.toml files to include Keycloak connection details:

```
APP_KEYCLOAK_URL=http://keycloak:8080
APP_KEYCLOAK_REALM=bornemap
```

### 4. Build and Run Services

```bash
# Build all services
cargo build --release

# Run services (separate terminals or docker-compose)
cargo run --bin auth-service
cargo run --bin driver-service
cargo run --bin admin-service
```

### 5. Verify

```bash
# Health checks (should return 200)
curl http://localhost:3000/health
curl http://localhost:3001/health
curl http://localhost:3002/health

# Test authentication flow
# Get token from Keycloak, then:
TOKEN=$(curl -s -X POST http://localhost:8080/realms/bornemap/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=mobile-driver" \
  -d "username=test@example.com" \
  -d "password=test123" \
  -d "grant_type=password" | jq -r '.access_token')

# Verify token works
curl -H "Authorization: Bearer $TOKEN" http://localhost:3000/api/v1/auth/me

# Verify RBAC
curl -H "Authorization: Bearer $TOKEN" http://localhost:3002/api/v1/analytics
# Should return 403 if token is not admin role
```

### 6. Verify CI Gates

```bash
# Run CI pipeline (includes 4 new security gates)
make ci
```

## Migration

```bash
# Apply new migration for user_profiles role column
make migrate
```

## Testing

```bash
# Run all tests
cargo test --workspace --all-features

# Run specific security tests
cargo test --package auth-service -- jwt
cargo test --package auth-service -- rbac
```
