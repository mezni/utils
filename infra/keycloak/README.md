# BorneMap Keycloak Identity Provider

## Overview

Keycloak provides OIDC-based authentication and authorization for the BorneMap platform.

## Deployment

### Standalone (for development)

```bash
docker compose -f infra/keycloak/docker-compose.keycloak.yml up -d
```

### With full platform (from project root)

```bash
docker compose up -d
```

## Access

| Resource | URL |
|----------|-----|
| Admin Console | http://localhost:8080/admin/master/console/ |
| Realm OIDC Discovery | http://localhost:8080/realms/bornemap/.well-known/openid-configuration |
| JWKS Endpoint | http://localhost:8080/realms/bornemap/protocol/openid-connect/certs |

## Default Credentials

| User | Username | Password |
|------|----------|----------|
| Keycloak Admin | `admin` | `admin` |

> **Note**: These are development defaults. Change before production deployment.

## Realm: `bornemap`

The `bornemap` realm contains:

### Realm Roles

| Role | Description |
|------|-------------|
| `driver` | Mobile app user (EV driver) |
| `partner` | Partner EV station owner |
| `admin` | Platform administrator |
| `super_admin` | Super administrator |

### Clients

| Client | Type | Auth Method |
|--------|------|-------------|
| `mobile-driver` | Public | PKCE (S256) |
| `web-driver` | Public | PKCE (S256) |
| `admin-dashboard` | Confidential | Client Secret |

## Operation

### Health Check

```bash
curl http://localhost:8080/health
```

### Issue Token (client credentials)

```bash
curl -X POST http://localhost:8080/realms/bornemap/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=admin-dashboard" \
  -d "client_secret=<secret>" \
  -d "grant_type=client_credentials"
```

### Issue Token (password grant — development only)

```bash
curl -X POST http://localhost:8080/realms/bornemap/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=admin-dashboard" \
  -d "client_secret=<secret>" \
  -d "grant_type=password" \
  -d "username=user@example.com" \
  -d "password=password"
```

## Validation

See `validation.md` for detailed validation procedures.
