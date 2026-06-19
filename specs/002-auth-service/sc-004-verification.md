# SC-004 Verification: Zero Authentication Requests Bypass Auth Service

## Summary

SC-004 states: "Zero authentication requests bypass the Auth Service to reach the identity provider directly, verifiable through identity provider access logs."

This document provides the verification procedure to ensure no client or service calls Keycloak directly for authentication flows.

---

## Verification Setup

### Prerequisites

1. **Auth Service Deployed**: Auth Service must be running and accessible at `/api/v1/auth/*`
2. **Keycloak Logs Access**: Access to Keycloak's access logs
3. **Test Client**: A client that can make authentication requests (e.g., curl, Postman, or a web browser)

---

## Manual Verification Procedure

### Step 1: Verify Keycloak Client Configuration

1. Log in to Keycloak Admin Console
2. Navigate to **Clients** → **bornemap**
3. Verify the following:
   - **Redirect URIs**: Should include Auth Service callback URL only
   - **Valid Redirect URIs**: Should include Auth Service callback URL only
   - **Web Origins**: Should include Auth Service origin only
   - **Client Authentication**: Use **Client ID and Secret** (if using confidential client)
   - **Authentication Flow**: Should use standard OIDC flows

**Expected Result**: Auth Service is the ONLY valid client for Keycloak authentication.

---

### Step 2: Verify Auth Service Routes (Traefik)

1. Navigate to Traefik configuration at `source/infra/traefik/dynamic/routing.yml`
2. Verify the following routes exist:
   ```yaml
   routers:
     auth-ruler:
       rule: "PathPrefix(`/api/v1/auth`)"
       service: auth-service
     auth-frontend:
       rule: "PathPrefix(`/auth`)"
       service: auth-service
   ```
3. Verify these routes point to the Auth Service stub/real endpoint

**Expected Result**: All authentication requests go through Traefik to Auth Service.

---

### Step 3: Test Authentication Flow

#### Test 1: Successful Login Flow

1. Make a login request to the Auth Service:
   ```bash
   curl -X POST http://localhost/api/v1/auth/login \
     -H "Content-Type: application/json" \
     -d '{"email":"admin@bornemap.tn","password":"test123"}'
   ```

2. Check Keycloak access logs:
   ```bash
   docker logs bornemap-keycloak --tail 100 | grep "bornemap/protocol/openid-connect/token"
   ```

**Expected Result**:
- Keycloak receives the request from Auth Service IP address
- No direct client IP addresses appear in logs

---

#### Test 2: Successful Refresh Flow

1. Take the `refresh_token` from the login response
2. Make a refresh request:
   ```bash
   curl -X POST http://localhost/api/v1/auth/refresh \
     -H "Content-Type: application/json" \
     -d '{"refresh_token":"<refresh_token_from_login>"}'
   ```

3. Check Keycloak access logs:
   ```bash
   docker logs bornemap-keycloak --tail 100 | grep "bornemap/protocol/openid-connect/token"
   ```

**Expected Result**:
- Keycloak receives the refresh request from Auth Service IP address
- Client IP is NOT visible in Keycloak logs

---

#### Test 3: Logout Flow

1. Make a logout request using the refresh_token:
   ```bash
   curl -X POST http://localhost/api/v1/auth/logout \
     -H "Content-Type: application/json" \
     -d '{"refresh_token":"<refresh_token>"}'
   ```

2. Check Keycloak access logs:
   ```bash
   docker logs bornemap-keycloak --tail 100 | grep "/logout"
   ```

**Expected Result**:
- Keycloak receives the logout request from Auth Service IP address
- Client IP is NOT visible in Keycloak logs

---

### Step 4: Test Malformed Token Request

1. Make a request with an invalid token directly (without going through Auth Service):
   ```bash
   curl -X POST http://localhost/api/v1/auth/refresh \
     -H "Content-Type: application/json" \
     -d '{"refresh_token":"not_a_valid_jwt"}'
   ```

2. Check Keycloak access logs:
   ```bash
   docker logs bornemap-keycloak --tail 50
   ```

**Expected Result**:
- No Keycloak access is logged
- Request is rejected at Auth Service with 400 error (not forwarded to Keycloak)

---

### Step 5: Verify Keycloak Logs for Direct Calls

1. Check Keycloak access logs for any requests from:
   - Browser client IPs
   - Unknown/unauthorized client IPs
   - Direct API calls to Keycloak endpoints

2. Search for patterns:
   ```bash
   # Search for direct token endpoint calls from known client IPs
   docker logs bornemap-keycloak | grep "POST.*bornemap/protocol/openid-connect/token"
   ```

3. Verify all requests show Auth Service IP in the logs

---

## Automated Verification (Integration Test)

### Test: Verify No Direct Keycloak Calls

```bash
# This test verifies that:
# 1. Login request is not forwarded to Keycloak if validation fails
# 2. No client IPs appear in Keycloak logs for authentication requests

#!/bin/bash

# Start with clean Keycloak logs
docker logs bornemap-keycloak > /tmp/keycloak_start.log

# Make a login request with invalid credentials
curl -X POST http://localhost/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@test.com","password":"wrong_password"}'

# Check Keycloak logs for direct calls
if docker logs bornemap-keycloak 2>&1 | grep -q "bornemap/protocol/openid-connect/token"; then
  echo "ERROR: Keycloak received a direct token request"
  exit 1
else
  echo "SUCCESS: No direct Keycloak calls detected"
fi
```

---

## Common Pitfalls to Avoid

### 1. Misconfigured Traefik Routes

**Problem**: Clients bypass Auth Service by accessing Keycloak directly

**Solution**:
- Verify Traefik routes are correct
- Use Keycloak client restriction to only allow Auth Service origin
- Check network-level access controls

### 2. Incorrect Keycloak Client Configuration

**Problem**: Multiple clients can call Keycloak directly

**Solution**:
- Only configure Auth Service as a valid Keycloak client
- Use strict client authentication requirements
- Verify redirect URIs only include Auth Service URLs

### 3. Frontend Accessing Keycloak Directly

**Problem**: Frontend code makes direct calls to Keycloak

**Solution**:
- Review frontend code for direct Keycloak API calls
- Use CORS restrictions to block direct Keycloak calls
- Implement middleware to detect and block unauthorized Keycloak requests

---

## Acceptance Criteria

SC-004 verification is successful when:

1. ✅ All authentication requests (login, refresh, logout) go through Auth Service
2. ✅ Keycloak logs show only Auth Service IP addresses for authentication flows
3. ✅ No client IP addresses appear in Keycloak logs for authentication requests
4. ✅ Malformed tokens are rejected at Auth Service (no Keycloak calls)
5. ✅ Keycloak client configuration restricts access to Auth Service only
6. ✅ Traefik routes are correctly configured to route `/api/v1/auth/*` to Auth Service

---

## Reporting

After verification, document:

1. **Test Results**:
   - Number of successful authentication flows tested
   - Keycloak logs showing Auth Service-only access
   - Any warnings or exceptions detected

2. **Issues Found**:
   - Any direct Keycloak calls detected
   - Misconfigured routes or clients
   - Security vulnerabilities

3. **Recommendations**:
   - Configuration improvements
   - Code changes needed
   - Monitoring additions

---

## CI/CD Integration

This verification should be run as part of:

1. **Deployment Pipeline**: Before deploying to staging/production
2. **Security Audits**: Monthly security reviews
3. **PR Verification**: When merging auth-related changes

---

**Last Updated**: 2026-06-19
**Verification Owner**: Architecture Team
**Approved By**: Security Team
