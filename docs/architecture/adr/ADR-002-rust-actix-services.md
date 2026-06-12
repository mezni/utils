# ADR-002: Rust + Actix-web for Backend Services

**Status:** Accepted  
**Date:** 2026-06-10  
**Authors:** Claude Code, Claude (chat)

---

## Context

BorneMap requires two backend services:
1. **Driver service** (:8080) — geospatial station discovery (high throughput)
2. **Admin service** (:8081) — partner management + event ingestion

We need:
- **Low latency** for map interactions (geospatial queries)
- **High throughput** for event ingestion (clickstream)
- **Type safety** (production reliability)
- **Fast startup** (containerized deployments)

Language options evaluated:
1. **Node.js** — fast dev cycle, async by default, but GC pauses hurt latency
2. **Python** — expressive, but slower cold starts, harder to scale events
3. **Go** — lightweight, but limited type system, stdlib verbosity
4. **Rust + Actix** — zero-cost abstractions, async/await, blazing speed, minimal overhead

---

## Decision

**Implement both services in Rust using Actix-web framework.**

Why Actix-web specifically:
- **Performance:** Top-tier in TechEmpower benchmarks (competitive with Go, C#)
- **Async/await:** Native, no callback hell
- **Type safety:** Rust's type system catches errors at compile-time
- **Ecosystem:** sqlx (compile-time SQL checks), tokio (async runtime), serde (JSON)
- **Resource footprint:** Single binary, minimal memory, fast cold starts
- **Concurrency model:** Actor-based (Actix) + async (tokio) hybrid

---

## Rationale

### Geospatial Performance (Driver Service)
PostGIS spatial queries (ST_DWithin for nearby search) must run <100ms. Rust's zero-cost abstractions + Actix's scalability make this achievable.

### Event Throughput (Admin Service)
Dashboard clickstream + map events generate high volume. Actix's actor model handles backpressure gracefully. Batch endpoints (`/api/v1/events/batch`) enable efficient aggregation.

### Operational Reliability
- Single binary per service (no runtime/VM dependency)
- Compile-time type checking prevents many prod issues
- Minimal container overhead → faster deployments
- sqlx compile-time SQL verification → prevent SQL errors

### Team Capability
Rust has a learning curve, but the constitution mandates a single implementation agent (Claude Code). The investment pays off in reliability + performance.

---

## Consequences

### Positive
- **Latency:** Sub-10ms API responses (even with PostGIS queries)
- **Throughput:** Handle 10k+ concurrent requests per service
- **Reliability:** Type system catches bugs at compile-time
- **Deployment:** Single binary, container-friendly
- **Cost:** Minimal resources → lower infra costs

### Negative
- **Compilation time:** ~30-60 seconds (local dev, full rebuild)
- **Rust learning curve:** Steep for unfamiliar teams (mitigated by single agent model)
- **Binary size:** ~20-40MB per service (acceptable)

---

## Implementation Notes

1. **Project structure:**
   ```
   source/driver-service/
   ├── Cargo.toml
   ├── src/
   │   ├── main.rs
   │   ├── handlers/
   │   ├── models/
   │   ├── db/
   │   └── error.rs
   source/admin-service/
   ├── Cargo.toml
   ├── src/ (same structure)
   ```

2. **Dependencies:**
   - `actix-web` — HTTP framework
   - `tokio` — async runtime
   - `sqlx` — compile-time SQL + connection pooling
   - `serde` + `serde_json` — serialization
   - `uuid` + `nanoid` — ID generation
   - `chrono` — timestamp handling
   - `env_logger` — logging

3. **Coding standards:**
   - All endpoints return JSON
   - Error responses follow common shape: `{ error: { code, message } }`
   - All database code uses sqlx (no ORM)
   - All timestamps ISO 8601 UTC

4. **Testing:**
   - Unit tests alongside code (`#[cfg(test)]`)
   - Integration tests in `tests/` directory
   - Contract tests for API shapes

---

## Related ADRs

- ADR-001: Traefik as gateway (routes to these services)
- ADR-005: PostGIS spatial indexes (critical for driver-service performance)

---

## References

- [Actix-web documentation](https://actix.rs)
- [sqlx documentation](https://github.com/launchbadge/sqlx)
- [Tokio async runtime](https://tokio.rs)
- [TechEmpower benchmarks](https://www.techempower.com/benchmarks/)
