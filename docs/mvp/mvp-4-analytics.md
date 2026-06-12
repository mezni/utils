# MVP-4: Analytics Intelligence

**Status:** Planning  
**Timeline:** 3-4 weeks (after MVP-3)  
**Goal:** Transform raw events into actionable business intelligence

---

## Scope

MVP-4 builds on the event foundation (raw_events) to deliver analytics:

1. **Real-time dashboards** — live metrics (sessions, searches, views)
2. **Cohort analysis** — user retention, feature adoption
3. **Search funnel** — drop-off analysis (search → view → detail)
4. **Station analytics** — popularity, peak hours, revenue
5. **Event streaming** — Kafka for real-time processing (optional)
6. **Data warehouse** — aggregated tables for fast queries

### Out of Scope (MVP-5+)
- ML-driven recommendations
- Predictive analytics
- Real-time alerting
- Advanced cohort segmentation

---

## Key Features

| Feature | Priority | Description |
|---------|----------|-------------|
| Session analytics | P0 | Duration, conversion funnel, retention |
| Station popularity | P0 | View counts, peak times, charger usage |
| Search analysis | P1 | Query patterns, empty results tracking |
| Revenue dashboard | P1 | Estimated earnings by station/partner |
| Cohort retention | P2 | Day 1, Day 7, Day 30 return rates |
| Real-time metrics | P2 | Live session count, current searches |
| Scheduled reports | P3 | Email weekly/monthly summaries |

---

## Work Breakdown

### Phase 1: Aggregated Tables (Week 1)

Create efficient summary tables:
- `analytics_db.daily_sessions` — aggregated by day
- `analytics_db.hourly_events` — aggregated by hour
- `analytics_db.station_metrics` — popularity, revenue
- Scheduled materialized view refresh

### Phase 2: Analytics Dashboard (Week 1-2)

- Query aggregated tables (fast)
- Real-time metrics (live counters)
- Cohort analysis page
- Station popularity ranking
- Charts and graphs (Recharts or similar)

### Phase 3: Event Streaming (Week 2-3, optional)

- Kafka producer (admin-service)
- Stream processors (Kafka Streams / Flink)
- Real-time aggregations
- Backpressure handling

### Phase 4: Reporting (Week 3-4)

- Scheduled batch jobs (cron)
- Email reports to partners
- PDF generation
- Data export (CSV, Parquet)

---

## Definition of Done

- [ ] Aggregated tables created and populated
- [ ] Analytics dashboard displays real-time metrics
- [ ] Session funnel shows accurate conversion rates
- [ ] Station popularity ranking correct
- [ ] Cohort retention calculated
- [ ] Scheduled report job runs daily
- [ ] All queries execute in <2 seconds

---

## Success Metrics

- Dashboard load: <2 seconds
- Real-time metrics update: <5 second latency
- Cohort analysis: 90%+ accuracy
- Report generation: <30 seconds per partner
- Query cost: <0.5 second per 1M events
