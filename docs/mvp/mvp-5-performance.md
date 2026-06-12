# MVP-5: Performance Hardening

**Status:** Planning  
**Timeline:** 2-3 weeks (after MVP-4)  
**Goal:** Optimize for scale, cost, and reliability

---

## Scope

MVP-5 hardens the system for real-world scale:

1. **Caching strategy** — Redis for queries, CDN for static assets
2. **Database optimization** — partitioning, read replicas
3. **Service optimization** — profiling, resource limits
4. **Monitoring & observability** — Prometheus, Grafana, logging
5. **Cost optimization** — reduce resource consumption
6. **Disaster recovery** — backups, PITR, failover

### Out of Scope (MVP-6+)
- Geographic distribution (multi-region)
- Active-active failover
- Kubernetes migration

---

## Key Features

| Feature | Priority | Description |
|---------|----------|-------------|
| Redis caching | P0 | Cache frequent queries (nearby, stations) |
| CDN setup | P0 | Static assets (JS, CSS, images) |
| Database read replica | P1 | Scale read-heavy queries |
| Table partitioning | P1 | analytics_db partitioned by month |
| Metrics & monitoring | P1 | Prometheus, Grafana, alerts |
| Structured logging | P1 | JSON logs, centralized aggregation |
| Query optimization | P2 | Index tuning, slow query logs |
| Load testing | P2 | Sustained 10k RPS testing |

---

## Work Breakdown

### Phase 1: Caching Layer (Week 1)

- Redis container (Docker Compose)
- Query result caching (nearby, stations list)
- Cache invalidation strategy
- Connection pooling
- Cache hit/miss metrics

### Phase 2: Observability (Week 1)

- Prometheus scrape config
- Grafana dashboards (request latency, error rate)
- Structured logging (JSON format)
- Log aggregation (ELK or similar)
- Error tracking (Sentry)

### Phase 3: Database Optimization (Week 2)

- Create indexes for slow queries
- Analyze query execution plans
- Implement read replica
- Test PITR (point-in-time recovery)
- Backup automation

### Phase 4: Load Testing & Tuning (Week 2-3)

- Load test runner (k6 or Locust)
- Sustained load: 10k RPS for 10 minutes
- Identify bottlenecks
- Tune based on metrics
- Document capacity limits

---

## Definition of Done

- [ ] Nearby search with cache: <50ms p95
- [ ] Station detail with cache: <30ms p95
- [ ] Sustained 10k RPS for 10 minutes
- [ ] Error rate <0.1%
- [ ] CPU usage <60% at peak load
- [ ] Memory usage <4GB per service
- [ ] Cache hit rate >80% for popular queries
- [ ] All queries logged and analyzed

---

## Success Metrics

- Latency reduced by 50% (before/after caching)
- Error rate <0.01%
- CPU utilization <50% at 10k RPS
- Cost reduction: 30% fewer resources
- 99.9% uptime over 2 weeks
