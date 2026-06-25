# Keycloak Validation Procedures

## Prerequisites

- Keycloak container running: `docker compose ps | grep keycloak`
- Postgres container healthy: `docker compose ps | grep postgres`
- Keycloak admin console accessible: http://localhost:8080

## 1. Keycloak Health

```bash
curl -s http://localhost:8080/health | jq .
```

**Expected**: Status 200 with `{"status": "UP"}`

## 2. Realm Import Verification

```bash
# List realms via admin API (requires admin token)
curl -s http://localhost:8080/admin/realms \
  -H "Authorization: Bearer $(TOKEN)" | jq '.[].realm'
```

**Expected**: Contains `bornemap`

## 3. OIDC Discovery Endpoint

```bash
curl -s http://localhost:8080/realms/bornemap/.well-known/openid-configuration | jq .
```

**Expected**: Status 200, valid JSON with:
- `issuer`: `http://localhost:8080/realms/bornemap`
- `authorization_endpoint`: present
- `token_endpoint`: present
- `jwks_uri`: present
- `response_types_supported`: includes `code`

## 4. JWKS Endpoint

```bash
curl -s http://localhost:8080/realms/bornemap/protocol/openid-connect/certs | jq .
```

**Expected**: Status 200, valid JSON with `keys` array containing at least one RSA public key.

## 5. Token Issuance (Client Credentials)

```bash
# Get admin-dashboard client secret from Keycloak admin console first,
# then run:
curl -s -X POST http://localhost:8080/realms/bornemap/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=admin-dashboard" \
  -d "client_secret=<SECRET>" \
  -d "grant_type=client_credentials" | jq .
```

**Expected**: Status 200, JSON with:
- `access_token`: JWT string
- `expires_in`: 300
- `token_type`: Bearer

## 6. JWT Decode Verification

```bash
# Decode the access token (without verification)
TOKEN="<access_token>"
echo $TOKEN | awk -F. '{print $2}' | base64 -d 2>/dev/null | jq .
```

**Expected claims**:
```json
{
  "sub": "<uuid>",
  "email": "<email or null>",
  "realm_access": {
    "roles": ["<role>"]
  }
}
```

## 7. Database: Users Schema

```bash
docker exec bornemap-postgres psql -U bornemap -d bornemap \
  -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'users'"
```

**Expected**: Returns `users`

## 8. Database: User Profiles Table

```bash
docker exec bornemap-postgres psql -U bornemap -d bornemap \
  -c "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'users' AND table_name = 'user_profiles' ORDER BY ordinal_position"
```

**Expected columns**:
| column_name | data_type | nullable |
|-------------|-----------|----------|
| user_uuid | uuid | NO |
| email | varchar | NO |
| first_name | varchar | YES |
| last_name | varchar | YES |
| phone | varchar | YES |
| locale | varchar | YES |
| created_at | timestamptz | NO |
| updated_at | timestamptz | NO |
| deleted_at | timestamptz | YES |

## 9. Database: Keycloak Database

```bash
docker exec bornemap-postgres psql -U bornemap -d bornemap \
  -c "SELECT datname FROM pg_database WHERE datname = 'keycloak_db'"
```

**Expected**: Returns `keycloak_db`

## Acceptance Checklist

- [ ] Keycloak container healthy
- [ ] bornemap realm created
- [ ] OIDC discovery returns valid config
- [ ] JWKS endpoint returns keys
- [ ] Token issuance works
- [ ] JWT contains sub, email, realm_access.roles
- [ ] users schema exists
- [ ] user_profiles table exists with all columns
- [ ] keycloak_db exists
