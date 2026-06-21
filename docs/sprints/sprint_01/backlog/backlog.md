# Sprint 0 Backlog — System Bootstrap & Enforcement Kernel

**Status**: Current
**Priority**: Critical (foundational)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S0-001 | Initialize monorepo directory structure | team | NOT_STARTED |
| S0-002 | Create docs/constitution/speckit_enforcement.md | team | ✅ DONE |
| S0-003 | Implement tools/ci_guard.sh (all 9 stages) | team | NOT_STARTED |
| S0-004 | Create tools/01_validate_identity.sh | team | NOT_STARTED |
| S0-005 | Create tools/02_validate_deps.sh | team | NOT_STARTED |
| S0-006 | Create tools/03_validate_analytics_gate.sh | team | NOT_STARTED |
| S0-007 | Create tools/04_validate_schema.sh | team | NOT_STARTED |
| S0-008 | Create tools/05_sqlx_policy_check.sh | team | NOT_STARTED |
| S0-009 | Create tools/06_ci_guard_final.sh | team | NOT_STARTED |
| S0-010 | Bootstrap platform_db (users, gis, inventory schemas) | team | NOT_STARTED |
| S0-011 | Bootstrap analytics_db | team | NOT_STARTED |
| S0-012 | Bootstrap keycloak_db | team | NOT_STARTED |
| S0-013 | Create auth-service skeleton with health endpoint | team | NOT_STARTED |
| S0-014 | Create driver-service skeleton with health endpoint | team | NOT_STARTED |
| S0-015 | Create admin-service skeleton with health endpoint | team | NOT_STARTED |
| S0-016 | Scaffold apps/packages/ui-kit | team | NOT_STARTED |
| S0-017 | Scaffold apps/packages/domain-types | team | NOT_STARTED |
| S0-018 | Scaffold apps/packages/client-core | team | NOT_STARTED |
| S0-019 | Create .github/workflows/ci.yml | team | NOT_STARTED |
| S0-020 | Implement SQLx offline verification | team | NOT_STARTED |

---

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S0-021 | Create infrastructure/docker-compose/local.yml | team | NOT_STARTED |
| S0-022 | Create infrastructure/traefik/traefik.toml | team | NOT_STARTED |
| S0-023 | Create infrastructure/scripts/provision_db.sh | team | NOT_STARTED |
| S0-024 | Create infrastructure/scripts/deploy.sh | team | NOT_STARTED |
| S0-025 | Create infrastructure/scripts/migrate.sh | team | NOT_STARTED |

---

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S0-026 | Create Keycloak realm export (realm-bornemap.json) | team | NOT_STARTED |
| S0-027 | Set up Redis configuration | team | NOT_STARTED |
| S0-028 | Create SPEC.md for SpecKit compliance | team | NOT_STARTED |

---

## Known Bugs (Carried Forward)

| ID | Issue | Priority |
|----|-------|----------|
| KNOWN-001 | Test stations leaking — filter is_test = FALSE | MEDIUM |
| KNOWN-002 | Missing deleted_at field | MEDIUM |
| KNOWN-003 | Duplicate nearby endpoint — driver-service owns | LOW |
| KNOWN-004 | CI grep brittle — regex-safe enforcement | LOW |
