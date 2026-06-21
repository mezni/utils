# SYSTEM STATE — BorneMap

**Generated**: Sprint 0
**Status**: Bootstrap Phase
**Constitution Version**: 1.15.2

---

## System Inventory

### Services
| Service | Status | Port | Owner |
|---------|--------|------|-------|
| auth-service | NOT_STARTED | 3000 | team |
| driver-service | NOT_STARTED | 3001 | team |
| admin-service | NOT_STARTED | 3002 | team |

### Databases
| Database | Status | Engine |
|----------|--------|--------|
| platform_db | NOT_CREATED | PostgreSQL + PostGIS |
| analytics_db | NOT_CREATED | PostgreSQL |
| keycloak_db | NOT_CREATED | PostgreSQL |

### Schemas (platform_db)
| Schema | Owner | Status |
|--------|-------|--------|
| users | auth-service | NOT_CREATED |
| gis | driver-service | NOT_CREATED |
| inventory | admin-service | NOT_CREATED |

### Frontend Apps
| App | Stack | Status |
|-----|-------|--------|
| mobile-driver | Expo SDK 54 | NOT_STARTED |
| web-driver | React + Leaflet | NOT_STARTED |
| admin-dashboard | React + shadcn/ui | NOT_STARTED |

### Shared Packages
| Package | Status |
|---------|--------|
| ui-kit | NOT_STARTED |
| domain-types | NOT_STARTED |
| client-core | NOT_STARTED |

### Infrastructure
| Component | Status |
|-----------|--------|
| Traefik Gateway | NOT_CONFIGURED |
| Keycloak Realm | NOT_CONFIGURED |
| Redis Cache | NOT_CONFIGURED |

### CI Pipeline
| Stage | Status |
|-------|--------|
| ci_guard.sh | NOT_CREATED |
| validate_identity.sh | NOT_CREATED |
| validate_deps.sh | NOT_CREATED |
| validate_analytics_gate.sh | NOT_CREATED |
| validate_schema.sh | NOT_CREATED |
| sqlx_policy_check.sh | NOT_CREATED |

---

## Documentation Status

| Document | Status |
|----------|--------|
| Constitution v1.15.2 | ✅ CREATED |
| SpecKit Enforcement v1.1 | ✅ CREATED |
| Guardrails | ✅ CREATED |
| Architecture | ✅ CREATED |
| SYSTEM_STATE.md | ✅ CURRENT |
| Roadmap Status | ✅ CURRENT |
| Sprint 0 State | ✅ CREATED |

---

## Known Issues

| ID | Description | Severity |
|----|-------------|----------|
| KNOWN-001 | Test stations leaking — must filter `is_test = FALSE` | MEDIUM |
| KNOWN-002 | Missing `deleted_at` field — required for soft delete | MEDIUM |
| KNOWN-003 | Duplicate nearby endpoint — driver-service owns | LOW |
| KNOWN-004 | CI grep brittle — needs regex-safe enforcement | LOW |

---

## Next Milestone

**Sprint 0 — System Bootstrap & Enforcement Kernel** (IN PROGRESS)

Exit criteria:
- [ ] 3 services exist exactly
- [ ] CI passes end-to-end
- [ ] All schemas created + ownership locked
- [ ] UUID vs nanoid separation enforced in CI
- [ ] ONLY driver-service can write analytics
- [ ] No illegal imports detected
