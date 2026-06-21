# Sprint 8 — Production Release & Operational Readiness Layer

**Status**: NOT_STARTED
**Constitution Version**: 1.15.2
**Dependencies**: Sprint 7 (security hardened)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S8-001 | Set up environment parity (local, staging, production) | team | NOT_STARTED |
| S8-002 | Create deterministic CI/CD deployment pipeline | team | NOT_STARTED |
| S8-003 | Implement versioned database migrations per service | team | NOT_STARTED |
| S8-004 | Implement migration hash validation and drift detection | team | NOT_STARTED |
| S8-005 | Create rollback system (last-known-good snapshot, automated rollback on failure) | team | NOT_STARTED |
| S8-006 | Implement production health monitoring (/health, /ready, /live with DB/Redis/Keycloak checks) | team | NOT_STARTED |
| S8-007 | Implement release versioning system (vMAJOR.MINOR.PATCH) | team | NOT_STARTED |
| S8-008 | Implement configuration management (env vars for secrets, versioned config files) | team | NOT_STARTED |
| S8-009 | Create deployment safety gates (schema version check, topology validation, analytics isolation) | team | NOT_STARTED |
| S8-010 | Implement observability baseline (request latency, error rates, telemetry rate, GIS performance) | team | NOT_STARTED |

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S8-011 | Create CI deployment validation gate | team | NOT_STARTED |
| S8-012 | Create CI environment drift gate | team | NOT_STARTED |
| S8-013 | Create CI rollback safety gate | team | NOT_STARTED |
| S8-014 | Create CI health gate | team | NOT_STARTED |
| S8-015 | Create CI version consistency gate | team | NOT_STARTED |

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S8-016 | Create Docker Compose production configuration | team | NOT_STARTED |
| S8-017 | Add deployment runbook documentation | team | NOT_STARTED |

## CI/CD Additions (Sprint 8)

| ID | Gate | Rule |
|----|------|------|
| CI-8.1 | Deployment Validation Gate | FAIL if CI not fully passing before deploy or unverified schema migration detected |
| CI-8.2 | Environment Drift Gate | FAIL if staging != production schema version or service topology mismatch |
| CI-8.3 | Rollback Safety Gate | FAIL if rollback script missing or migration not reversible |
| CI-8.4 | Health Gate | FAIL if any service fails readiness check |
| CI-8.5 | Version Consistency Gate | FAIL if frontend/backend/DB versions mismatch |

## Exit Criteria

Sprint 8 is COMPLETE ONLY IF:
- [ ] Fully automated CI/CD pipeline working
- [ ] Rollback tested and verified
- [ ] Environments identical in schema + behavior
- [ ] Health endpoints functional
- [ ] Deployment, environment, and version gates pass
