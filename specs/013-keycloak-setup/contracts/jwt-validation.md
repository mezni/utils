# JWT Validation Contract

## Overview

Backend services validate incoming JWTs to authenticate requests. Keycloak signs tokens using RS256, and services fetch the public key from the JWKS endpoint.

## JWKS Endpoint

| Attribute | Value |
|-----------|-------|
| URL | `http://keycloak:8180/realms/ev-platform/protocol/openid-connect/certs` |
| Method | GET |
| Response | JWKS (JSON Web Key Set) containing the realm's public RSA key |

## Token Validation Rules

1. Verify signature using JWKS public key (RS256)
2. Validate `exp` (expiration) claim — reject if expired
3. Validate `iss` (issuer) claim — MUST equal `http://keycloak:8180/realms/ev-platform`
4. Validate `aud` (audience) claim — MUST contain the service's client ID (if present)
5. Extract `sub` (user ID), `email`, `realm_access.roles`, and optionally `partner_id`

## Required JWT Claims

| Claim | Type | Description | Always Present |
|-------|------|-------------|----------------|
| `sub` | String | Keycloak user UUID | Yes |
| `email` | String | User's email address | Yes |
| `realm_access.roles` | String[] | List of assigned roles | Yes |
| `exp` | Integer | Token expiration timestamp (Unix) | Yes |
| `iat` | Integer | Token issuance timestamp (Unix) | Yes |
| `iss` | String | Issuer URL | Yes |
| `partner_id` | String | Partner identifier (e.g., `PRT-00123`) | Only if user has `partner` role and attribute is set |

## Error Responses

| Scenario | HTTP Status | Error |
|----------|-------------|-------|
| Missing/invalid token | 401 | `Unauthorized` |
| Expired token | 401 | `Token expired` |
| Invalid signature | 401 | `Invalid token` |
| Insufficient role | 403 | `Forbidden` |
