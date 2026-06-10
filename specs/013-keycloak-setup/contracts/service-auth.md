# Service Authentication Contract

## Overview

Backend services authenticate to Keycloak as confidential clients using client credentials (client ID + client secret) to obtain service account tokens. These tokens can be used for inter-service communication.

## Token Endpoint

| Attribute | Value |
|-----------|-------|
| URL | `http://keycloak:8180/realms/ev-platform/protocol/openid-connect/token` |
| Method | POST |
| Content-Type | `application/x-www-form-urlencoded` |

## Request Parameters

| Parameter | Value |
|-----------|-------|
| `grant_type` | `client_credentials` |
| `client_id` | `{service-client-id}` |
| `client_secret` | `{service-client-secret}` |

## Response

```json
{
  "access_token": "eyJhbG...",
  "expires_in": 900,
  "token_type": "Bearer"
}
```

## Client Credentials

| Service | Client ID | Secret Source |
|---------|-----------|---------------|
| Driver Service | `driver-service` | `infra/env/driver-service.env.example` |
| Admin Service | `admin-service` | `infra/env/admin-service.env.example` |

## Usage

1. Service obtains access token from Keycloak token endpoint
2. Token is cached until near expiry (e.g., refresh at 80% of `expires_in`)
3. Token is included in `Authorization: Bearer {token}` header for outgoing requests to other services
4. Receiving service validates the JWT per [jwt-validation.md](./jwt-validation.md)
