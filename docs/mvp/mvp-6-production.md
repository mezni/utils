# MVP-6: Production Hardening & Public Launch

**Status:** Planning  
**Timeline:** 2-3 weeks (after MVP-5)  
**Goal:** Deploy to production with TLS, high availability, and public access

---

## Scope

MVP-6 takes BorneMap from local development to public production:

1. **TLS/HTTPS** — Traefik with Let's Encrypt certificates
2. **High availability** — Multi-instance services, load balancing
3. **Database HA** — Streaming replication, automatic failover
4. **CDN integration** — Global static asset delivery
5. **DNS & DDoS protection** — Cloudflare or equivalent
6. **Monitoring & alerts** — 24/7 platform health
7. **Security hardening** — Secrets management, network policies
8. **Public API documentation** — API docs, SDKs, examples

### Out of Scope
- Geographic multi-region deployment
- On-premise support

---

## Key Features

| Feature | Priority | Description |
|---------|----------|-------------|
| TLS certificates | P0 | Let's Encrypt via Traefik, auto-renewal |
| Secrets management | P0 | Vault or environment-based secrets |
| Load balancing | P0 | Multiple service instances (Traefik) |
| Database HA | P0 | Streaming replication, failover |
| Rate limiting | P1 | Global 100 req/sec per IP |
| DDoS protection | P1 | Cloudflare or WAF |
| Monitoring | P1 | Prometheus, Grafana, PagerDuty |
| Security scanning | P1 | OWASP ZAP, container scanning |
| API documentation | P2 | OpenAPI spec, Swagger UI |
| Mobile app distribution | P2 | App Store, Play Store |

---

## Work Breakdown

### Phase 1: TLS & Secrets (Week 1)

- Traefik ACME setup (Let's Encrypt)
- Secrets management (Vault or env vars)
- Database password rotation
- Service-to-service mTLS (optional)

### Phase 2: High Availability (Week 1)

- Multi-instance orchestration (Docker Compose or simple multi-host)
- Load balancer configuration
- Database replication setup
- Connection pooling optimization
- Graceful shutdown handling

### Phase 3: Security Hardening (Week 1-2)

- Network policies (firewall rules)
- Security scanning (OWASP ZAP)
- Dependency scanning (npm audit, cargo audit)
- Rate limiting implementation
- Input validation audit

### Phase 4: Monitoring & Alerts (Week 2)

- Prometheus scrape config (prod)
- Grafana dashboards (production)
- Alert rules (SLOs, error budgets)
- PagerDuty integration
- Log aggregation setup

### Phase 5: Documentation & Launch (Week 2-3)

- API documentation (OpenAPI spec)
- Deployment playbooks
- Incident response procedures
- Mobile app submission (App Store, Play Store)
- Launch communication plan

---

## Definition of Done

- [ ] HTTPS working on all endpoints
- [ ] Multiple service instances running
- [ ] Database failover tested
- [ ] 99.99% uptime target (4 nines)
- [ ] All secrets encrypted at rest
- [ ] Security scanning green
- [ ] Monitoring alerts configured
- [ ] API docs published
- [ ] Mobile app in stores
- [ ] Runbook for common incidents

---

## Success Metrics

- HTTPS everywhere (0 insecure connections)
- 99.99% uptime (52 minutes downtime/year)
- P95 latency <100ms globally
- RTO (Recovery Time Objective): <5 minutes
- RPO (Recovery Point Objective): <1 minute
- Zero successful security incidents
- Mobile app ratings: >4.5 stars
