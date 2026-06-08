# Research: API Versioning Strategy

**Phase**: 0 (Outline & Research)  
**Feature**: API Versioning (`001-backend-and-database`)  
**Created**: 2026-06-08

## Overview

This research document resolves all technical unknowns from the feature specification and implementation plan. All findings are consolidated below for reference during Phase 1 design.

---

## Research 1: URL-Based vs Header-Based Versioning

**Decision**: URL-based versioning (`/api/v1/stations`)

**Rationale**:
- More discoverable: version is visible in URL path without inspecting headers
- Better caching: CDN and proxies can cache different versions separately based on URL
- Simpler routing: API gateway and load balancer can route based on path prefix
- Industry standard: GitHub, Stripe, Twitter all use URL-based versioning
- Explicit for clients: no ambiguity about which version is being called

**Alternatives considered**:
- Header-based versioning (`Accept: application/vnd.bornemap.v1+json`): More RESTful but less discoverable, harder for developers to test manually in browser
- Parameter-based (`/api/stations?version=1`): Query params are for filters/options, not structural versioning
- Content negotiation only: Would require clients to know about versioning; not suitable for initial implementation

**Adopted**: URL-based. All endpoints under `/api/v<number>/<resource>`.

---

## Research 2: Version Number Format

**Decision**: Integer versioning (v1, v2, v3...)

**Rationale**:
- Simple to implement and reason about
- No ambiguity (v1.2 could be confusing: is it version 1.2 or 1 with an implicit .0?)
- Industry standard (GitHub uses v3, v4, etc.)
- Aligns with MVP cycles: each major service rewrite gets a new number

**Alternatives considered**:
- Semantic versioning in URL (`/api/v1.2.3/`): Overkill for API versioning; encourages minor/patch releases that break API contracts
- Date-based versioning (`/api/2026-06-08/`): Hard to track; no clear deprecation timeline

**Adopted**: Integer versioning. v1 in MVP-1, v2 introduced in MVP-2 when Rust services replace Python.

---

## Research 3: Version Deprecation & Support Lifespan

**Decision**: 12-month support window per version

**Rationale**:
- Industry standard: AWS, Google Cloud, major SaaS platforms use 12-24 month windows
- Provides clear migration path: partners have 1 year to upgrade before v1 is retired
- Operationally sustainable: supporting 2 versions (current + previous) is manageable for small team (Principle 4)
- Aligns with quarterly release cycles: by the time v3 is planned, v1 is already deprecated

**Timeline**:
- v1 released: Sprint 1.5 (end of MVP-1)
- v2 released: MVP-2 (estimated 6+ months later)
- v1 support ends: 12 months after v2 release
- v1 deprecated: documented in API reference immediately when v2 launches

**Alternatives considered**:
- Indefinite support: Unbounded maintenance burden; violates Principle 4 (simple operations)
- 6 months: Too short for partners to plan upgrades
- Per-request negotiation: Would require complex versioning logic per endpoint

**Adopted**: 12 months. Documented in FR-004 and platform API documentation.

---

## Research 4: Handling Unversioned Endpoint Requests

**Decision**: Return HTTP 404 for unversioned paths

**Rationale**:
- Clean break: no ambiguity about whether `/api/stations` works or not
- Forces clients to be explicit: they must choose a version, not rely on defaults
- Prevents accidental calls to wrong version
- Makes future deprecation simpler: unversioned routes don't need gradual migration

**Example responses**:
- Request: `GET /api/stations` → Response: `HTTP 404 Not Found` with message: `"API endpoints require version prefix. Use /api/v1/stations instead."`
- Request: `GET /api/v999/stations` → Response: `HTTP 404 Not Found` with message: `"API version v999 not found. Available versions: v1. See /api/docs for details."`

**Alternatives considered**:
- Alias to latest version: `/api/stations` → maps to `/api/v1/` internally. Problem: clients don't know which version they're using; confusing during v1→v2 migration
- Redirect: `301` to `/api/v1/stations`. Problem: adds latency; clients must handle redirects
- Deprecation period: Support unversioned in v1, remove in v2. Problem: extends migration burden; violates Principle 1 (MVP-first)

**Adopted**: Strict 404 for unversioned paths. Documented in edge cases and assumptions.

---

## Research 5: Version Identifier in Responses

**Decision**: Implicit in URL path only. No version field or header in responses.

**Rationale**:
- Response schema is immutable per version: if client calls `/api/v1/stations`, they know they're getting v1 schema
- Reduces payload size: no redundant metadata in every response
- Simplifies schema: no version field to test, parse, or document
- Clients verify version by URL they called, not by inspecting response

**Example**:
```json
// Request: GET /api/v1/stations
// Response 200 OK:
{
  "data": [
    { "id": "uuid", "name": "Station A", ... }
  ],
  "count": 15
}
// No "version": "1" field needed
```

**Alternatives considered**:
- Response body: `{ "version": "1", "data": [...] }`. Problem: bloats every response; schema must be documented twice (URL version + response field)
- HTTP header: `X-API-Version: 1`. Problem: clients must parse headers; less discoverable than URL
- Content-Type: `application/vnd.bornemap.v1+json`. Problem: requires custom MIME types; no CDN support for versioning; same complexity as header

**Adopted**: Implicit versioning. Version is a contract of the URL path, not the response body.

---

## Research 6: Version Routing Implementation (FastAPI)

**Decision**: Router-based versioning in FastAPI

**Approach**:
```python
# app/main.py
from fastapi import FastAPI
from app.routers import v1

app = FastAPI()

# Register v1 routes under /api/v1 prefix
app.include_router(v1.health_router, prefix="/api/v1", tags=["v1"])
app.include_router(v1.partners_router, prefix="/api/v1", tags=["v1"])
app.include_router(v1.stations_router, prefix="/api/v1", tags=["v1"])
app.include_router(v1.chargers_router, prefix="/api/v1", tags=["v1"])

# When v2 added in MVP-2:
# from app.routers import v2
# app.include_router(v2.routers..., prefix="/api/v2", tags=["v2"])
```

**Rationale**:
- Clean separation: v1 code is isolated, cannot bleed into v2
- Safe migration: v2 can be tested independently before v1 is removed
- Documented: FastAPI OpenAPI spec shows both versions separately
- Scalable: adding v3, v4 is trivial

**Alternatives considered**:
- Single router with version parameter: `@app.get("/api/{version}/stations")`. Problem: bloats route logic; hard to lock v1 schema
- Blueprint per version: Django-style. Problem: overkill for FastAPI; more boilerplate

**Adopted**: Router-based versioning. Each version gets its own router module under `app/routers/v<number>/`.

---

## Research 7: OpenAPI/Swagger Documentation

**Decision**: Separate OpenAPI spec per version, merged at root `/docs`

**Approach**:
```
GET /api/docs → Shows both v1 and v2 endpoints (when available)
             → Each section tagged [v1], [v2] for clarity
GET /api/v1/docs → Shows v1 only (optional)
GET /api/v2/docs → Shows v2 only (optional)
```

**Rationale**:
- Single source of truth: developers see all versions in one place
- Clear deprecation info: v1 endpoints can be marked "Deprecated" in OpenAPI
- Works out-of-the-box: FastAPI auto-generates OpenAPI from routers tagged with version

**Alternatives considered**:
- Separate Swagger instances: `/docs/v1`, `/docs/v2`. Problem: fragmented; developers miss deprecation info
- Custom documentation site: Markdown files. Problem: out of sync with code; requires manual updates

**Adopted**: FastAPI auto-generated OpenAPI with version tags. Deprecation notes added as endpoint descriptions.

---

## Research 8: API Version Announcement & Deprecation Timeline

**Decision**: Announce new version when released; deprecation planned at announcement

**Timeline example for v1→v2**:
- **v2 release day**: 
  - v2 endpoints available at `/api/v2/...`
  - v1 endpoints marked "Deprecated in API docs; support ends [date 12 months from now]"
  - Notification sent to all API consumers (via email, in-app message, blog post)
  
- **Month 1-6**: Partners start using v2; v1 requests monitored in analytics
- **Month 6**: Reminder email sent: "v1 support ends in 6 months"
- **Month 11**: Final reminder email: "v1 support ends in 1 month"
- **Month 12**: v1 endpoints removed; requests to `/api/v1/...` return 410 Gone

**Rationale**:
- Respect for client time: 12 months is ample for migration
- Communication: no surprise deprecations
- Analytics-driven: can track v1 usage to extend deadline if needed

**Adopted**: Public deprecation timeline. Documented in API changelog and OpenAPI.

---

## Summary of Decisions

| Topic | Decision | Rationale |
|-------|----------|-----------|
| Versioning method | URL-based (`/api/v1/`) | Discoverable, cacheable, industry standard |
| Version format | Integer (v1, v2, v3) | Simple, clear, aligns with MVP cycles |
| Support window | 12 months per version | Industry standard, sustainable operations |
| Unversioned endpoints | Return 404 | Forces explicit version; prevents confusion |
| Version in response | No (implicit in URL) | Immutable schema per URL; reduces payload |
| FastAPI routing | Router-based per version | Clean separation, safe migration |
| Documentation | Auto-generated OpenAPI | Single source of truth, version tags |
| Deprecation | Announced + 12-month window | Respect for clients, clear timeline |

---

## Phase 1 Ready

All research questions resolved. No ambiguities remain. Proceed to Phase 1 (Design & Contracts).
