# Sprint Backlog
## BorneMap Deferred Work & Outstanding Items

**Last Updated:** June 2026  
**Status:** Pre-Sprint Phase  
**Version:** 1.0

---

## 1. Outstanding Tasks (Pre-Sprint)

### Infrastructure Setup

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| INFRA-001 | Configure docker-compose.dev.yml | HIGH | 📋 Pending | Database, Redis, Keycloak, services |
| INFRA-002 | Setup PostgreSQL with PostGIS extension | HIGH | 📋 Pending | platform_db, keycloak_db, analytics_db |
| INFRA-003 | Initialize Keycloak realm (bornemap) | HIGH | 📋 Pending | Realm export, clients, roles |
| INFRA-004 | Configure Traefik routing rules | HIGH | 📋 Pending | JWT middleware, service routing |
| INFRA-005 | Setup Redis cache cluster | MEDIUM | 📋 Pending | Single instance for dev |
| INFRA-006 | Configure environment variables | HIGH | 📋 Pending | .env.dev, .env.prod templates |

### Database Migrations

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| DB-001 | Create gis schema migrations | MEDIUM | 📋 Pending | OSM import tier |
| DB-002 | Create inventory schema migrations | HIGH | 📋 Pending | partners, stations, chargers, views |
| DB-003 | Create users schema migrations | HIGH | 📋 Pending | user_profiles table |
| DB-004 | Create analytics schema migrations | MEDIUM | 📋 Pending | Event logging tables |
| DB-005 | Setup materialized views refresh schedule | MEDIUM | 📋 Pending | nightly / hourly |
| DB-006 | Create database indexes for performance | HIGH | 📋 Pending | Spatial indexes, foreign keys |

### OpenAPI Specification

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| API-001 | Finalize api/openapi/auth.yaml | HIGH | 📋 Pending | Auth Service endpoints |
| API-002 | Finalize api/openapi/driver.yaml | HIGH | 📋 Pending | Driver Service endpoints |
| API-003 | Finalize api/openapi/admin.yaml | HIGH | 📋 Pending | Admin Service endpoints |
| API-004 | Finalize api/openapi/shared.yaml | HIGH | 📋 Pending | Common DTOs, errors, pagination |
| API-005 | Generate TypeScript client from OpenAPI | HIGH | 📋 Pending | @packages/api-client |
| API-006 | Generate Rust types from OpenAPI | MEDIUM | 📋 Pending | Validation during build |

### Shared Code Libraries

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| SHARED-001 | Implement Rust crate: auth-core | HIGH | 📋 Pending | JWT, auth primitives |
| SHARED-002 | Implement Rust crate: db-models | HIGH | 📋 Pending | SQLx models, shared types |
| SHARED-003 | Implement Rust crate: validation | HIGH | 📋 Pending | Input validation rules |
| SHARED-004 | Implement Rust crate: geo | MEDIUM | 📋 Pending | GIS utilities |
| SHARED-005 | Implement Rust crate: error | HIGH | 📋 Pending | Error types, Result trait |
| SHARED-006 | Implement TypeScript package: shared-types | HIGH | 📋 Pending | Type definitions |
| SHARED-007 | Implement TypeScript package: shared-ui | MEDIUM | 📋 Pending | Reusable React components |
| SHARED-008 | Implement TypeScript package: shared-hooks | MEDIUM | 📋 Pending | Custom React hooks |
| SHARED-009 | Implement TypeScript package: auth-client | HIGH | 📋 Pending | Keycloak wrapper |
| SHARED-010 | Implement TypeScript package: config | MEDIUM | 📋 Pending | Shared configuration |

### CI/CD Pipeline

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| CI-001 | Setup GitHub Actions: backend.yml | HIGH | 📋 Pending | Rust build, tests, clippy |
| CI-002 | Setup GitHub Actions: frontend.yml | HIGH | 📋 Pending | TypeScript build, tests, lint |
| CI-003 | Setup GitHub Actions: tests.yml | HIGH | 📋 Pending | Unit, integration, E2E |
| CI-004 | Setup SpecKit CI validator | HIGH | 📋 Pending | Architecture enforcement |
| CI-005 | Setup code coverage reporting | MEDIUM | 📋 Pending | Codecov or similar |
| CI-006 | Setup security scanning | MEDIUM | 📋 Pending | Dependabot, SAST |

---

## 2. Deferred Features (Post-Validation)

### OCPP Integration (Charger Communication)
**Status:** 🔴 OUT OF SCOPE  
**Reason:** Validation phase does not include hardware communication  
**Estimated Sprint:** Post-validation (not planned)  

**Features:**
- Charger status updates via OCPP protocol
- Real-time availability synchronization
- Charger firmware updates
- Energy consumption metrics

---

### Payment Processing
**Status:** 🔴 OUT OF SCOPE  
**Reason:** Validation phase excludes billing systems  
**Estimated Sprint:** Post-validation  

**Features:**
- Payment gateway integration
- Booking & charging sessions
- Invoice generation
- Refund handling
- Multiple payment methods

---

### Smart Charging Optimization
**Status:** 🔴 OUT OF SCOPE  
**Reason:** Requires real-time grid data & optimization algorithms  

**Features:**
- Grid load balancing
- Demand-side response
- Optimization algorithms
- Predictive scheduling

---

### Real-Time Telemetry
**Status:** 🔴 OUT OF SCOPE  
**Reason:** Requires streaming infrastructure  

**Features:**
- Real-time charger updates
- WebSocket connections
- Streaming events
- Live dashboards

---

### Event-Driven Architecture
**Status:** 🔴 OUT OF SCOPE  
**Reason:** Validation uses simple request-response model  

**Components:**
- Kafka / RabbitMQ / NATS
- Event streaming pipelines
- Distributed transactions
- Event sourcing

---

### Advanced Analytics
**Status:** 🔴 OUT OF SCOPE (PARTIAL)  
**Reason:** Basic analytics in Admin Service, advanced BI excluded  

**Deferred Features:**
- Custom reports builder
- BI integration (Tableau, Looker)
- Predictive analytics
- ML-based recommendations

---

### Service Mesh & Observability
**Status:** 🔴 OUT OF SCOPE  
**Reason:** Not required for 3-service architecture  

**Deferred:**
- Istio / Linkerd service mesh
- Distributed tracing (Jaeger)
- APM monitoring (New Relic, DataDog)
- Custom metrics

---

### Kubernetes Deployment
**Status:** 🔴 OUT OF SCOPE  
**Reason:** Docker Compose is the only allowed orchestration  

**Note:** Kubernetes support explicitly forbidden per constitution.

---

### Native Mobile Features
**Status:** 🔴 OUT OF SCOPE (PARTIAL)  
**Reason:** Validation uses Expo SDK 54, no native modules  

**Deferred:**
- Custom native modules
- Advanced camera/location features
- Background tasks
- App store deployment

---

### LDAP/AD User Federation
**Status:** 🔴 OUT OF SCOPE  
**Reason:** Validation phase uses simple Keycloak setup  

**Deferred:**
- LDAP integration
- Active Directory sync
- Enterprise SSO
- Multi-realm federation

---

### Multi-Language Support (i18n)
**Status:** 🔴 OUT OF SCOPE  
**Reason:** English-only for validation  

**Deferred:**
- Arabic localization (Tunisia market)
- French localization
- Dynamic language switching

---

### Advanced Caching Strategies
**Status:** 🔴 OUT OF SCOPE (PARTIAL)  
**Reason:** Single Redis instance for validation  

**Deferred:**
- Redis cluster / Sentinel
- Cache warming strategies
- Distributed cache invalidation
- Cache coherency protocols

---

## 3. Known Technical Debt

| Debt Item | Severity | Addressed In | Notes |
|-----------|----------|--------------|-------|
| No distributed tracing | LOW | Post-validation | Add Jaeger/Tempo if needed |
| Single database instance | LOW | Post-validation | Add read replicas, HA |
| No API rate limiting (advanced) | MEDIUM | SPRINT-001 | Basic rate limit sufficient |
| No circuit breakers | LOW | Post-validation | Add if inter-service calls increase |
| Limited error retry logic | MEDIUM | SPRINT-001+ | Implement exponential backoff |

---

## 4. Documentation Gaps (Not Blocking)

| Gap | Priority | Addressed By |
|-----|----------|--------------|
| Deployment runbook | LOW | Ops team post-validation |
| Disaster recovery plan | LOW | Post-validation |
| Performance tuning guide | MEDIUM | SPRINT-007 |
| Troubleshooting guide | LOW | Post-validation |
| API SDK documentation | MEDIUM | SPRINT-001 |

---

## 5. Testing Gaps (Pre-Sprint)

| Gap | Priority | Sprint |
|-----|----------|--------|
| Load testing framework setup | MEDIUM | SPRINT-007 |
| Security testing plan | HIGH | SPRINT-007 |
| Chaos engineering tests | LOW | Post-validation |
| Contract testing setup | HIGH | SPRINT-001 |

---

## 6. Backlog Items by Category

### High Priority (Blocking Sprints)

```
INFRA-001  ← Block SPRINT-001
INFRA-003  ← Block SPRINT-001
INFRA-004  ← Block SPRINT-001
DB-002     ← Block SPRINT-002
DB-003     ← Block SPRINT-001
API-001    ← Block SPRINT-001
API-002    ← Block SPRINT-002
API-003    ← Block SPRINT-003
```

### Medium Priority (Can Proceed In Parallel)

```
INFRA-005
INFRA-006
DB-001
DB-004
API-004
CI-001 through CI-005
```

### Low Priority (Can Defer)

```
INFRA-007 (monitoring)
CI-006 (advanced security)
Documentation refinements
```

---

## 7. Sprint Readiness Checklist

Before starting SPRINT-001, verify:

- [ ] docker-compose.dev.yml is configured
- [ ] PostgreSQL running with PostGIS
- [ ] Keycloak realm (bornemap) provisioned
- [ ] Traefik routing configured
- [ ] Environment variables templated (.env.dev)
- [ ] OpenAPI schemas (auth.yaml) finalized
- [ ] GitHub Actions workflows (backend.yml) created
- [ ] Rust workspace structure in place
- [ ] Database migrations framework setup
- [ ] SpecKit CI validator configured

---

## 8. Risk Assessment

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|-----------|
| Keycloak misconfiguration | HIGH | MEDIUM | Pre-test realm setup |
| PostGIS query performance | MEDIUM | LOW | Test with realistic data |
| Redis cache coherency | MEDIUM | LOW | Implement cache versioning |
| JWT token size | LOW | LOW | Monitor token size |

---

## 9. External Dependencies

| Dependency | Version | Status | Risk |
|-----------|---------|--------|------|
| PostgreSQL | 16 | Available | LOW |
| Redis | 7 | Available | LOW |
| Keycloak | 24+ | Available | MEDIUM (config) |
| Rust | 1.70+ | Available | LOW |
| Node.js | 18+ | Available | LOW |
| Docker | 20+ | Available | LOW |

---

## 10. Future Enhancements (Not Scheduled)

**Potential Improvements (Post-Validation):**
- [ ] GraphQL API option
- [ ] gRPC services for inter-service communication
- [ ] Async job queue (Bull, Celery)
- [ ] Multi-region deployment
- [ ] A/B testing framework
- [ ] Feature flags system
- [ ] Custom mobile app (not Expo)
- [ ] Progressive Web App (PWA) mode
- [ ] Advanced search (Elasticsearch)
- [ ] Full-text search improvements

---

## 11. References

- See `roadmap_status.md` for sprint planning
- See `SYSTEM_STATE.md` for architecture status
- See `GUARDRAILS.md` for execution standards
- See `architecture.md` for system design

---

**Tracking:**
- Update this file at end of each sprint
- Move completed items to respective SPRINT docs
- Reassess deferred items quarterly
- Review external dependencies monthly
