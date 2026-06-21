# Follow-Up — BorneMap

**Sprint**: 0
**Generated**: Sprint 0

---

## Action Items

| Priority | Item | Owner | Due |
|----------|------|-------|-----|
| CRITICAL | Implement `tools/ci_guard.sh` with all 9 stages | team | Sprint 0 |
| CRITICAL | Create `tools/01_validate_identity.sh` | team | Sprint 0 |
| CRITICAL | Create `tools/02_validate_deps.sh` | team | Sprint 0 |
| CRITICAL | Create `tools/03_validate_analytics_gate.sh` | team | Sprint 0 |
| HIGH | Bootstrap database schemas (platform_db + analytics_db) | team | Sprint 0 |
| HIGH | Create 3 service skeletons with health endpoints | team | Sprint 0 |
| HIGH | Scaffold frontend packages with boundary enforcement | team | Sprint 0 |
| MEDIUM | Set up GitHub Actions workflow | team | Sprint 0 |
| MEDIUM | Create Docker Compose for local development | team | Sprint 0 |
| MEDIUM | Configure Traefik gateway routing | team | Sprint 0 |

---

## Open Questions

| Question | Owner | Status |
|----------|-------|--------|
| Exact Rust web framework (Actix-web vs Axum)? | team | PENDING |
| Frontend routing strategy (React Router vs TanStack Router)? | team | PENDING |
| OSM data source for Tunisia EV stations? | team | PENDING |

---

## Risks to Track

| Risk | Impact | Owner |
|------|--------|-------|
| CI script complexity may delay Sprint 1 start | Medium | team |
| SQLx offline data generation requires DB connection | Medium | team |
| Keycloak realm configuration may need iteration | Low | team |

---

## Next Checkpoint

**Sprint 0 completion review** — after CI pipeline is operational and all exit criteria met.
