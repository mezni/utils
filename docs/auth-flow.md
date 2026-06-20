# BorneMap — Auth Flow
**Version:** 1.0
**Date:** June 2026

---

## Overview

BorneMap uses **Keycloak** as the identity provider with a single realm (`bornemap`). The `auth-service` is the **only** service that calls the Keycloak Admin REST API. JWT validation happens at two layers: Traefik (forward auth) and per-service middleware.

---

## Auth Flow Diagram

```
┌─────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ Client   │     │ Traefik  │     │ auth-svc │     │ Keycloak │
│ (app)    │     │ Gateway  │     │ :3000    │     │          │
└────┬─────┘     └────┬─────┘     └────┬──────┘     └────┬─────┘
     │                │                │                 │
     │  POST /auth/register            │                 │
     │────────────────>│               │                 │
     │                │  POST /auth/register            │
     │                │───────────────>│                 │
     │                │                │  POST admin/register
     │                │                │────────────────>│
     │                │                │                 │
     │                │                │<────────────────│
     │                │<───────────────│                 │
     │<────────────────│               │                 │
     │                │                │                 │
     │  POST /auth/login               │                 │
     │────────────────>│               │                 │
     │                │  POST /auth/login                │
     │                │───────────────>│                 │
     │                │                │  POST /token    │
     │                │                │────────────────>│
     │                │                │<────────────────│
     │                │<───────────────│                 │
     │<────────────────│               │                 │
     │                │                │                 │
     │  GET /driver/stations (JWT)     │                 │
     │────────────────>│               │                 │
     │                │  Forward-Auth Token Introspect   │
     │                │─────────────────────────────────>│
     │                │<─────────────────────────────────│
     │                │  Route to driver-service         │
     │                │───────────────>│                 │
```

---

## Endpoints

### Registration
```http
POST /api/v1/auth/register
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "secure_password",
  "name": "User Name",
  "role": "driver"  // driver, partner, admin
}
```

Response: `201 Created` with user profile + initial tokens

### Login
```http
POST /api/v1/auth/login
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "secure_password"
}
```

Response: `200 OK` with access_token, refresh_token, expires_in

### Token Refresh
```http
POST /api/v1/auth/refresh
Content-Type: application/json

{
  "refresh_token": "<refresh_token>"
}
```

Response: `200 OK` with new access_token, refresh_token, expires_in

### Profile (Authenticated)
```http
GET /api/v1/auth/me
Authorization: Bearer <access_token>
```

Response: `200 OK` with user profile

---

## JWT Structure

```json
{
  "sub": "USR-k8F3aZ91LmQx",
  "email": "user@example.com",
  "name": "User Name",
  "realm_access": {
    "roles": ["role:driver"]
  },
  "iat": 1718000000,
  "exp": 1718003600,
  "iss": "https://auth.bornemap.tn/auth/realms/bornemap"
}
```

---

## Security Rules

| Rule | Implementation |
|---|---|
| Short-lived tokens | Access token: 15min, Refresh token: 24h |
| No token persistence | Tokens never stored in `platform_db` |
| Forward auth | Traefik validates JWT before routing |
| Service middleware | Second validation layer per service |
| Internal auth | Service-to-service calls use internal tokens |
| Role enforcement | Per-endpoint, explicit role checks |

---

## Keycloak Configuration

| Item | Value |
|---|---|
| Realm | `bornemap` |
| Clients | `mobile-driver-app`, `web-driver-app`, `admin-partner-dashboard` |
| Roles | `role:driver`, `role:partner`, `role:admin` |
| Admin API access | auth-service only (service account) |
| Token signature | RS256 |
