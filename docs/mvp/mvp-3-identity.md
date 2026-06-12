# MVP-3: Identity & RBAC

**Status:** Planning  
**Timeline:** 2-3 weeks (after MVP-2)  
**Goal:** Integrate Keycloak for authentication and role-based access control

---

## Scope

MVP-3 wires up Keycloak for user identity and permissions:

1. **Keycloak realms** — bm-drivers (public), bm-control (partners)
2. **JWT validation** — all services validate tokens
3. **Role-based access** — driver, partner, admin roles
4. **Mobile auth flow** — login/signup in driver app
5. **Dashboard auth** — partner login
6. **Auth gateway middleware** — Traefik validates JWTs

### Out of Scope (MVP-4+)
- Social login (Google, GitHub)
- Biometric authentication
- Advanced IAM policies
- User self-service password reset

---

## Key Features

| Feature | Priority | Description |
|---------|----------|-------------|
| Keycloak setup | P0 | Docker Compose, two realms |
| Driver login/signup | P0 | Mobile app auth flow |
| Partner login | P0 | Dashboard auth flow |
| JWT validation in services | P0 | All endpoints check token |
| Partner scoping | P0 | Partner sees only their stations |
| Admin role enforcement | P1 | Admins see all stations |
| Token refresh | P1 | Handle expired JWTs gracefully |
| Logout | P1 | Clear tokens, end session |

---

## Work Breakdown

### Phase 1: Keycloak Setup (Week 1)

- Keycloak Docker container (compose)
- bm-drivers realm (public drivers)
- bm-control realm (partners)
- Default roles (public_driver, registered_driver, partner, admin)
- Test users (driver, partner, admin)

### Phase 2: Backend Integration (Week 1)

- JWT validation middleware (Actix)
- Partner scoping query builder
- Admin role checks
- Error handling (401, 403)
- Token refresh endpoint

### Phase 3: Mobile Auth (Week 2)

- Login screen (email/password)
- Signup flow
- Token storage (secure enclave)
- Auto-refresh on app launch
- Logout action

### Phase 4: Dashboard Auth (Week 2-3)

- Partner login page
- Session management
- Protected routes (partner dashboard)
- Admin view (all stations)

### Phase 5: Integration & Testing (Week 3)

- E2E auth flows
- Permission checks verified
- Token expiration handled
- Multi-device login tested

---

## Definition of Done

- [ ] Keycloak running in Docker Compose
- [ ] Mobile driver can login/logout
- [ ] Partner can login to dashboard
- [ ] Admin can see all stations
- [ ] Partner sees only their stations
- [ ] All JWTs validated in services
- [ ] Token refresh works
- [ ] 401/403 errors handled gracefully

---

## Success Metrics

- Auth flow <2 seconds start-to-finish
- Login form accessible, mobile-responsive
- Zero unauthorized access to partner data
- All role checks enforced server-side
- Token refresh transparent (no user interruption)
