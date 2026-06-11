# Threat Model

---

## Trust Boundaries

```
Internet Zone
  ↓
Edge (Traefik)
  ↓
Application Services
  ↓
Databases
  ↓
Identity (Keycloak internal)
```

---

## Threats & Mitigations

### T1: Frontend Token Theft

**Risk:** JWT stolen from mobile/web client

**Mitigation:**
- Short-lived access tokens
- Refresh token rotation
- No local storage of sensitive scopes
- HTTPS only
- Device-based session binding (future)

### T2: Direct DB Access

**Risk:** Bypassing backend logic

**Mitigation:**
- DB not exposed externally
- Private Docker network only
- No public ports on PostgreSQL

### T3: Cross-Service Privilege Escalation

**Risk:** Admin service accessing driver or analytics data improperly

**Mitigation:**
- Strict DB schema ownership
- No cross-service DB credentials
- Runtime enforcement of partner scoping

### T4: Keycloak Abuse

**Risk:** Unauthorized realm manipulation

**Mitigation:**
- Keycloak internal-only network
- Admin realm separated (bm-control)
- No public admin console exposure

### T5: Event Injection (Analytics Poisoning)

**Risk:** Fake clickstream data

**Mitigation:**
- Event validation schema
- Rate limiting per user/session
- Server-side enrichment of events

### T6: API Abuse

**Risk:** Station scraping / spam

**Mitigation:**
- Rate limiting (per IP + user)
- Request throttling on /stations/nearby
- Optional captcha for abusive patterns
