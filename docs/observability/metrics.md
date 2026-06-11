# Metrics

**Goal:** Track system health and UX performance.

---

## Key Metrics

| Metric | Target | Service |
|---|---|---|
| station_nearby_latency_ms | < 200 | driver-service |
| station_detail_latency_ms | < 150 | driver-service |
| event_ingest_latency_ms | < 50 | clickstream-service |
| api_error_rate | < 1% | all services |
| map_load_time_ms | < 2000 | mobile app |

---

## Future (MVP-5+)

- Prometheus metric export
- Grafana dashboards
- Request rate per endpoint
- Database connection pool usage
- Event throughput (events/sec)
