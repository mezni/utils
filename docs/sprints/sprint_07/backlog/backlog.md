# Sprint 7 — Security Hardening & Compliance Lockdown

**Status**: NOT_STARTED
**Constitution Version**: 1.15.2
**Dependencies**: Sprint 6 (reliability hardened)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S7-001 | Implement request rate limiting per identity (JWT sub) | team | NOT_STARTED |
| S7-002 | Implement IP-based throttling at gateway | team | NOT_STARTED |
| S7-003 | Enforce request size limits (body + headers) | team | NOT_STARTED |
| S7-004 | Implement method whitelisting per route | team | NOT_STARTED |
| S7-005 | Implement resource-level authorization (ABAC — owner checks on every request) | team | NOT_STARTED |
| S7-006 | Expand audit trail for security events (failed logins, unauthorized access, rate limit violations) | team | NOT_STARTED |
| S7-007 | Implement pagination enforcement (no full dataset dumps) | team | NOT_STARTED |
| S7-008 | Enforce max export size limits (MAX_EXPORT_ROWS = 1000) | team | NOT_STARTED |
| S7-009 | Enforce max response payload (MAX_RESPONSE_PAYLOAD = 1MB) | team | NOT_STARTED |
| S7-010 | Implement token replay detection (jti tracking) | team | NOT_STARTED |
| S7-011 | Implement token blacklist support (logout enforcement) | team | NOT_STARTED |
| S7-012 | Implement structured secure logging (no PII, no raw tokens) | team | NOT_STARTED |

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S7-013 | Create CI authorization coverage gate | team | NOT_STARTED |
| S7-014 | Create CI rate limit gate | team | NOT_STARTED |
| S7-015 | Create CI payload safety gate | team | NOT_STARTED |
| S7-016 | Create CI security logging gate | team | NOT_STARTED |
| S7-017 | Create CI token security gate | team | NOT_STARTED |
| S7-018 | Create CI privilege escalation gate | team | NOT_STARTED |

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S7-019 | Add stricter issuer/audience JWT validation | team | NOT_STARTED |
| S7-020 | Add rate limiting dashboard | team | NOT_STARTED |

## CI Additions (Sprint 7)

| ID | Gate | Rule |
|----|------|------|
| CI-7.1 | Authorization Coverage Gate | FAIL if endpoint missing ownership check |
| CI-7.2 | Rate Limit Gate | FAIL if endpoint exposed without rate limiting rule |
| CI-7.3 | Payload Safety Gate | FAIL if response exceeds size limit or unbounded query results |
| CI-7.4 | Security Logging Gate | FAIL if logs contain PII or raw tokens |
| CI-7.5 | Token Security Gate | FAIL if missing issuer/audience validation or expiration checks |
| CI-7.6 | Privilege Escalation Gate | FAIL if role bypass detected in API layer |

## Exit Criteria

Sprint 7 is COMPLETE ONLY IF:
- [ ] All endpoints enforce ownership checks
- [ ] Rate limiting active everywhere
- [ ] No sensitive data leaks in logs
- [ ] No privilege escalation possible via API
- [ ] Authz coverage gate passes
- [ ] Payload safety enforced
- [ ] JWT security validated
