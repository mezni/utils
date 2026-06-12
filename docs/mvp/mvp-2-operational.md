# MVP-2: Operational Control

**Status:** Planning  
**Timeline:** 3-4 weeks (after MVP-1 stabilizes)  
**Goal:** Enable partners to manage their charging infrastructure

---

## Scope

MVP-2 adds operational management capabilities:

1. **Partner portal** — web interface for infrastructure partners
2. **Station management** — CRUD stations and chargers
3. **Real-time availability** — update charger status
4. **Analytics dashboard** — view usage metrics
5. **Alert system** — notifications for station health
6. **Rate limiting** — protect APIs from abuse

### Out of Scope (MVP-3+)
- User authentication (Keycloak integration)
- Multi-tenant RBAC
- Advanced analytics
- Mobile partner app

---

## Key Features

| Feature | Priority | Description |
|---------|----------|-------------|
| Web dashboard | P0 | React + shadcn/ui, authenticated partners only |
| Station CRUD | P0 | Create, read, update, delete stations and chargers |
| Charger status updates | P1 | Real-time status: online, offline, faulted, busy |
| Usage analytics | P1 | Sessions, revenue, availability graphs |
| Alert rules | P2 | Trigger notifications on station/charger events |
| Rate limiting | P2 | Per-partner API limits (100 req/min) |
| CSV export | P3 | Export station data, usage reports |

---

## Work Breakdown

### Phase 1: Dashboard Setup (Week 1)

- React app scaffold (source/front/dashboard/)
- shadcn/ui component library
- Authentication placeholder (MVP-3 integration)
- Routing setup (React Router)
- API client (React Query)

### Phase 2: Station Management (Week 1-2)

- Station list with pagination
- Create station form (name, address, lat/lng, hours)
- Edit station form
- Delete station with confirmation
- Charger CRUD within station

### Phase 3: Analytics (Week 2-3)

- Sessions graph (daily/weekly)
- Revenue calculation
- Availability heatmap
- Top stations by usage
- Charger type distribution

### Phase 4: Integration (Week 3-4)

- Connect to admin-service endpoints
- Authentication flow setup (JWT placeholder)
- E2E testing
- Performance optimization
- Stabilization sprint

---

## Definition of Done

- [ ] Dashboard runs locally via `npm run dev`
- [ ] Partner can create/edit/delete stations
- [ ] Usage analytics displayed
- [ ] No auth errors (JWT handling ready for MVP-3)
- [ ] 80%+ component test coverage
- [ ] All endpoints called with correct parameters
- [ ] Rate limiting tested (mock)

---

## Success Metrics

- Dashboard load time: <2 seconds
- Form submissions: <500ms response time
- Analytics queries: <1 second
- Mobile responsive (tablet view)
- 0 console errors on real dashboard usage
