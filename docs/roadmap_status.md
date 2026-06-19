# Roadmap Status
## BorneMap Sprint Progress Tracking

**Last Updated:** June 2026  
**Status:** Pre-Sprint Phase (Foundation)  
**Current Phase:** Sprint Planning

---

## 1. Overall Progress

```
Foundation Phase (Current)
├── ✅ Architecture defined (rigid 3-service topology)
├── ✅ Governance established (constitution, guardrails, lifecycle)
├── ✅ OpenAPI contracts designed
├── ✅ Authentication flow documented
├── ✅ Documentation generated
└── 🔄 Sprints pending (ready to execute)

Validation Phase (Target)
├── Sprint 1-7: Core implementation
├── Full E2E testing
└── Production readiness

Expansion Phase (Post-Validation)
└── Future features (not in scope)
```

---

## 2. Sprint Planning

### SPRINT-001: Auth Service Core Implementation
**Status:** 📋 Pending  
**Estimated Duration:** 5-7 days  
**Priority:** CRITICAL (gating sprint)

**Scope:**
- Auth Service API implementation (Rust/Actix)
- Keycloak integration
- User registration & login flows
- JWT token management
- Role assignment (driver, partner, admin)
- SQLx database access (users schema)

**Blockers:** None  
**Dependencies:** None  

**Acceptance Criteria:**
- ✅ Auth Service running on :3000
- ✅ POST /api/v1/auth/register works
- ✅ POST /api/v1/auth/login works
- ✅ GET /api/v1/auth/me works
- ✅ JWT validation in Traefik works
- ✅ Users stored in users.user_profiles (USR_* IDs)
- ✅ All unit tests pass (90%+ coverage)
- ✅ Integration tests pass
- ✅ OpenAPI contract verified

---

### SPRINT-002: Driver Service Spatial API
**Status:** 📋 Pending  
**Estimated Duration:** 5-7 days  
**Priority:** HIGH
**Depends On:** SPRINT-001 (Auth Service)

**Scope:**
- Driver Service API implementation (Rust/Actix)
- Station discovery endpoints
- Geospatial queries (PostGIS)
- Favorites management
- Reviews & ratings
- Redis caching strategy
- Materialized views for performance

**Blockers:**
- Requires SPRINT-001 (Auth Service for JWT validation)

**Acceptance Criteria:**
- ✅ Driver Service running on :3001
- ✅ GET /api/v1/driver/stations works
- ✅ GET /api/v1/driver/search (spatial) works
- ✅ GET /api/v1/driver/favorites works
- ✅ POST /api/v1/driver/favorites works
- ✅ Redis cache hits working
- ✅ 300ms+ debounce on map queries
- ✅ Materialized views refreshing
- ✅ All unit tests pass
- ✅ Integration tests with database pass

---

### SPRINT-003: Admin Service Partner Management
**Status:** 📋 Pending  
**Estimated Duration:** 5-7 days  
**Priority:** HIGH
**Depends On:** SPRINT-001

**Scope:**
- Admin Service API implementation (Rust/Actix)
- Partner CRUD endpoints
- Station management
- Charger management
- Analytics event logging
- Cache invalidation (Redis)
- Audit trail

**Blockers:**
- Requires SPRINT-001 (Auth Service + role:admin validation)

**Acceptance Criteria:**
- ✅ Admin Service running on :3002
- ✅ POST /api/v1/admin/partners works
- ✅ PUT /api/v1/admin/stations works
- ✅ DELETE /api/v1/admin/chargers works
- ✅ Cache invalidation working
- ✅ Analytics events logged
- ✅ Audit trail created
- ✅ All unit tests pass
- ✅ Integration tests pass

---

### SPRINT-004: Frontend Mobile App (Expo)
**Status:** 📋 Pending  
**Estimated Duration:** 6-8 days  
**Priority:** HIGH
**Depends On:** SPRINT-001 + SPRINT-002

**Scope:**
- Mobile app setup (Expo SDK 54)
- Station discovery screens
- Map view (react-native-maps)
- Station detail screen
- Favorites feature
- User profile
- Authentication (Keycloak client)
- AsyncStorage offline mode
- Token storage (Secure Storage)

**Blockers:**
- Requires SPRINT-001 (Auth Service API)
- Requires SPRINT-002 (Driver Service API)

**Acceptance Criteria:**
- ✅ App runs on iOS & Android (Expo Go)
- ✅ Login flow works
- ✅ Station list loads
- ✅ Map renders correctly
- ✅ Favorites save/load
- ✅ Offline mode works
- ✅ No direct API calls (uses api-client)
- ✅ All unit tests pass
- ✅ E2E tests pass

---

### SPRINT-005: Frontend Web Driver App (React)
**Status:** 📋 Pending  
**Estimated Duration:** 5-7 days  
**Priority:** HIGH
**Depends On:** SPRINT-001 + SPRINT-002

**Scope:**
- React app setup (Vite)
- Leaflet map integration
- Station discovery
- Advanced search filters
- Favorites management
- User authentication
- Responsive design
- Token storage (memory only)

**Blockers:**
- Requires SPRINT-001 (Auth Service API)
- Requires SPRINT-002 (Driver Service API)

**Acceptance Criteria:**
- ✅ App runs on localhost:3000
- ✅ Leaflet map renders
- ✅ Stations visible on map
- ✅ Search filters work
- ✅ Favorites feature works
- ✅ No localStorage tokens
- ✅ Responsive design (mobile, tablet, desktop)
- ✅ All unit tests pass
- ✅ E2E tests pass

---

### SPRINT-006: Dashboard Admin Portal (React)
**Status:** 📋 Pending  
**Estimated Duration:** 6-8 days  
**Priority:** MEDIUM
**Depends On:** SPRINT-001 + SPRINT-003

**Scope:**
- React dashboard setup (Vite + shadcn/ui)
- Partner management interface
- Station/charger management
- Analytics dashboard
- Audit logs viewer
- User management
- Role assignment
- Reports generation

**Blockers:**
- Requires SPRINT-001 (Auth Service API)
- Requires SPRINT-003 (Admin Service API)

**Acceptance Criteria:**
- ✅ Dashboard runs on localhost:3001
- ✅ Admin can create partners
- ✅ Admin can manage stations
- ✅ Analytics display works
- ✅ Audit logs visible
- ✅ Role-based UI rendering
- ✅ shadcn/ui components used
- ✅ All unit tests pass
- ✅ E2E tests pass

---

### SPRINT-007: Integration & E2E Testing
**Status:** 📋 Pending  
**Estimated Duration:** 5-7 days  
**Priority:** CRITICAL

**Scope:**
- Full system integration testing
- E2E test suite (Playwright)
- Database integration tests
- Redis cache integration
- Keycloak integration validation
- Performance benchmarks
- Security scanning
- Load testing

**Blockers:**
- Requires SPRINT-001 through SPRINT-006 completed

**Acceptance Criteria:**
- ✅ All E2E tests pass
- ✅ 90%+ code coverage
- ✅ Zero CRITICAL bugs
- ✅ Zero security vulnerabilities
- ✅ Performance targets met
- ✅ Documentation complete
- ✅ CI/CD pipelines passing

---

## 3. Feature Completion Matrix

| Feature | SPRINT | Status | Notes |
|---------|--------|--------|-------|
| Auth Service | 001 | 📋 Pending | Gating all others |
| User Registration | 001 | 📋 Pending | |
| User Login (OAuth2) | 001 | 📋 Pending | |
| JWT Validation | 001 | 📋 Pending | Traefik integration |
| Driver Service | 002 | 📋 Pending | Depends: SPRINT-001 |
| Station Discovery | 002 | 📋 Pending | Spatial queries |
| Favorites (Driver) | 002 | 📋 Pending | |
| Reviews & Ratings | 002 | 📋 Pending | |
| Admin Service | 003 | 📋 Pending | Depends: SPRINT-001 |
| Partner Management | 003 | 📋 Pending | CRUD operations |
| Station Management | 003 | 📋 Pending | Admin-only |
| Analytics Logging | 003 | 📋 Pending | |
| Mobile App | 004 | 📋 Pending | Depends: 001, 002 |
| Web Driver App | 005 | 📋 Pending | Depends: 001, 002 |
| Dashboard Portal | 006 | 📋 Pending | Depends: 001, 003 |
| Integration Tests | 007 | 📋 Pending | Full stack |
| E2E Tests | 007 | 📋 Pending | Browser-based |

---

## 4. Known Blockers & Risks

| Blocker | Risk Level | Mitigation |
|---------|-----------|-----------|
| Keycloak setup | MEDIUM | Pre-provision realm in docker-compose.dev |
| PostGIS configuration | MEDIUM | Test PostGIS docker image early |
| Expo SDK 54 compatibility | LOW | Already locked, tested |
| Redis cluster setup | LOW | Single instance for validation phase |

---

## 5. Dependency Graph

```
SPRINT-001 (Auth Service) [GATING]
    │
    ├─ SPRINT-002 (Driver Service)
    │   │
    │   ├─ SPRINT-004 (Mobile App)
    │   │
    │   └─ SPRINT-005 (Web App)
    │
    ├─ SPRINT-003 (Admin Service)
    │   │
    │   └─ SPRINT-006 (Dashboard)
    │
    └─ SPRINT-007 (Integration & E2E)
        └─ Depends on: 002, 003, 004, 005, 006
```

---

## 6. Testing Strategy

### Unit Tests
- Each service: 80%+ code coverage
- All shared crates: 85%+ coverage
- Frontend packages: 75%+ coverage

### Integration Tests
- Database transactions
- API contract compliance
- Service-to-service communication
- Cache invalidation flows

### E2E Tests (Playwright)
- Full registration flow
- Login flow
- Station discovery (web & mobile)
- Partner management workflow
- Analytics dashboard

### Performance Tests
- Station search < 200ms (p95)
- Map queries < 300ms (p95)
- Database queries < 100ms (p95)
- Cache hit ratio > 80%

---

## 7. Timeline Estimate

| Phase | Duration | Start | End |
|-------|----------|-------|-----|
| SPRINT-001 | 5-7 days | Week 1 | Week 1-2 |
| SPRINT-002 | 5-7 days | Week 2 | Week 2-3 |
| SPRINT-003 | 5-7 days | Week 2 | Week 3 |
| SPRINT-004 | 6-8 days | Week 3 | Week 4 |
| SPRINT-005 | 5-7 days | Week 3-4 | Week 4 |
| SPRINT-006 | 6-8 days | Week 4 | Week 5 |
| SPRINT-007 | 5-7 days | Week 5 | Week 5-6 |
| **Total** | **~42 days** | **Week 1** | **Week 6** |

---

## 8. Success Metrics

### Functionality
- ✅ All 7 sprints completed
- ✅ All acceptance criteria met
- ✅ Zero high/critical bugs

### Quality
- ✅ 85%+ code coverage
- ✅ Zero security vulnerabilities
- ✅ All tests passing (unit, integration, E2E)

### Performance
- ✅ Response times < 300ms (p95)
- ✅ Cache hit ratio > 80%
- ✅ Database queries optimized

### Documentation
- ✅ All systems documented
- ✅ API contracts verified
- ✅ Deployment guides complete

---

## 9. Post-Validation Phase (Future)

Once validation phase completes:

**Expansion Candidates:**
- [ ] OCPP integration (charger communication)
- [ ] Payment processing
- [ ] Smart charging optimization
- [ ] Real-time telemetry
- [ ] Event streaming (Kafka/RabbitMQ)
- [ ] Advanced analytics
- [ ] Mobile app native features

**Note:** These are explicitly OUT OF SCOPE for validation phase.

---

## 10. References

- See `SYSTEM_STATE.md` for current state
- See `sprint_backlog.md` for deferred items
- See `architecture.md` for system design
- See `GUARDRAILS.md` for execution standards
