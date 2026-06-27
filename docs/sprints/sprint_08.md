# Sprint 08 — Observability & Metrics

**Title:** Observability & Middleware Refinement  
**Sprint:** 08  
**Scope:** `services/auth-service/`  
**Status:** Complete  

## Goal

Build a production-grade observability layer by introducing request tracing, structured logging, metrics, request correlation, and hardened HTTP middleware without violating Clean Architecture boundaries.

## Deliverables

### New Files
| File | Purpose |
|------|---------|
| `src/http/middleware/mod.rs` | Module declarations for all middleware |
| `src/http/middleware/request_id.rs` | Request ID middleware + extractor |
| `src/http/middleware/tracing.rs` | per-request tracing spans |
| `src/http/middleware/logging.rs` | Structured request logging middleware |
| `src/http/metrics.rs` | Prometheus metrics registry, middleware, and `/metrics` handler |

### Modified Files
| File | Changes |
|------|---------|
| `Cargo.toml` | Added `prometheus = "0.14"` dependency |
| `src/main.rs` | Middleware pipeline: RequestId → Tracing → Metrics → RateLimit → Logging |
| `src/http/mod.rs` | Added `/metrics` route, fixed module resolution |
| `src/middleware.rs` | Re-exports from new `http::middleware::request_id` |
| `tests/middleware_tests.rs` | 8 integration tests for middleware and metrics |

### Removed Files
| File | Reason |
|------|--------|
| `src/http/middleware.rs` | Replaced by `middleware/mod.rs` directory (Rust 2024 path resolution) |

## Architecture

### Middleware Pipeline
```
RequestId → Tracing → Metrics → RateLimit → Logging → Routes
     ↓          ↓         ↓          ↓          ↓
  request_id  spans     counters  429 if      logs all
  in headers            & gauge   exceeded   fields
```

### Ordering Rationale
1. **RequestId** (outermost): Generates/accepts request ID before any processing
2. **Tracing**: Creates span with request ID for the full lifecycle
3. **Metrics**: Tracks ALL requests (including rate-limited) for observability
4. **RateLimit**: Enforces limits before processing
5. **Logging** (innermost): Logs only requests that pass rate limiting

### Observability Fields Contract
| Field | Required | Source |
|-------|----------|--------|
| `request_id` | ✅ | `X-Request-ID` header or UUID generation |
| `method` | ✅ | `req.method()` |
| `path` | ✅ | `req.path()` |
| `status` | ✅ | `res.status()` |
| `duration_ms` | ✅ | `Instant::now()` diff |
| `service` | ✅ | Hardcoded `"auth-service"` |
| `client_ip` | Optional | `req.peer_addr()` |
| `error` | On failure | `err.to_string()` |

### Prometheus Metrics
| Metric | Type | Labels |
|--------|------|--------|
| `http_requests_total` | Counter | `method`, `path`, `status` |
| `http_request_duration_seconds` | Histogram | `method`, `path` |
| `http_active_requests` | Gauge | (none) |

The `/metrics` endpoint is excluded from metrics self-collection (must never modify counters).

## Key Decisions

### Metrics Middleware vs Per-Handler Metrics
- Metrics are collected in middleware, not handlers
- Clean Architecture preserved: domain/application layers know nothing about metrics
- Middleware can be tested independently

### Unified RequestId Module
- Single `RequestId` type in `http/middleware/request_id.rs`
- Middleware stores ID in request extensions
- Extractor checks extensions first, then headers, then generates UUID
- Old `src/middleware.rs` now re-exports from the new location

### Fixed-Window Rate Limiting
- Existing `RateLimitMiddlewareFactory` reused as-is
- Redis-backed atomic operations for distributed rate limiting
- Configurable via environment variables

## Tests
```
8 passed; 0 failed; 0 warnings
```
- Request ID generation and propagation
- Request ID preservation from incoming headers
- Extractor integration with middleware
- Metrics endpoint returns Prometheus-format output
- Exposed counter/histogram/gauge names
- `/metrics` self-exclusion (must never modify counters)
- Unique request IDs across requests
- Active requests gauge presence

## Verification
- `cargo check --workspace`: clean (0 errors, 0 warnings)
- `cargo check --workspace --tests`: clean
- `cargo test --test middleware_tests`: 8/8 pass
