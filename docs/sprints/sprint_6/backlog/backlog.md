# Sprint 8 — Security Hardening & Compliance Lockdown

**Status**: NOT_STARTED
**Constitution Version**: 1.15.2
**Dependencies**: Sprint 7 (reliability hardened)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S8-001 | Implement request rate limiting per identity (JWT sub) | team | NOT_STARTED |
| S8-002 | Implement IP-based throttling at gateway | team | NOT_STARTED |
| S8-003 | Enforce request size limits (body + headers) | team | NOT_STARTED |
| S8-004 | Implement method whitelisting per route | team | NOT_STARTED |
| S8-005 | Implement resource-level authorization (ABAC — owner checks on every request) | team | NOT_STARTED |
| S8-006 | Expand audit trail for security events (failed logins, unauthorized access, rate limit violations) | team | NOT_STARTED |
| S8-007 | Implement pagination enforcement (no full dataset dumps) | team | NOT_STARTED |
| S8-008 | Enforce max export size limits (MAX_EXPORT_ROWS = 1000) | team | NOT_STARTED |
| S8-009 | Enforce max response payload (MAX_RESPONSE_PAYLOAD = 1MB) | team | NOT_STARTED |
| S8-010 | Implement token replay detection (jti tracking) | team | NOT_STARTED |
| S8-011 | Implement token blacklist support (logout enforcement) | team | NOT_STARTED |
| S8-012 | Implement structured secure logging (no PII, no raw tokens) | team | NOT_STARTED |

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S8-013 | Create CI authorization coverage gate | team | NOT_STARTED |
| S8-014 | Create CI rate limit gate | team | NOT_STARTED |
| S8-015 | Create CI payload safety gate | team | NOT_STARTED |
| S8-016 | Create CI security logging gate | team | NOT_STARTED |
| S8-017 | Create CI token security gate | team | NOT_STARTED |
| S8-018 | Create CI privilege escalation gate | team | NOT_STARTED |

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S8-019 | Add stricter issuer/audience JWT validation | team | NOT_STARTED |
| S8-020 | Add rate limiting dashboard | team | NOT_STARTED |

## CI Additions (Sprint 8)

| ID | Gate | Rule |
|----|------|------|
| CI-8.1 | Authorization Coverage Gate | FAIL if endpoint missing ownership check |
| CI-8.2 | Rate Limit Gate | FAIL if endpoint exposed without rate limiting rule |
| CI-8.3 | Payload Safety Gate | FAIL if response exceeds size limit or unbounded query results |
| CI-8.4 | Security Logging Gate | FAIL if logs contain PII or raw tokens |
| CI-8.5 | Token Security Gate | FAIL if missing issuer/audience validation or expiration checks |
| CI-8.6 | Privilege Escalation Gate | FAIL if role bypass detected in API layer |

## Exit Criteria

Sprint 8 is COMPLETE ONLY IF:
- [ ] All endpoints enforce ownership checks
- [ ] Rate limiting active everywhere
- [ ] No sensitive data leaks in logs
- [ ] No privilege escalation possible via API
- [ ] Authz coverage gate passes
- [ ] Payload safety enforced
- [ ] JWT security validated
