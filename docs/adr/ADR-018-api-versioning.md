# ADR-018: API Versioning Strategy

**Status**: Accepted  
**Date**: 2026-06-08  
**Sprint**: 1.1  
**Affected Services**: bornemap-service (MVP-1), all future services (MVP-2+)

---

## Problem

BorneMap requires a sustainable approach to evolving the API without breaking existing client integrations. Clients will be deployed across many devices (mobile, web, dashboards) with different update cycles. The platform will evolve across multiple MVPs (Python → Rust migration in MVP-2, new services in MVP-3+).

**Constraints**:
- Must support multiple independent client versions simultaneously
- Must allow backend migration (Python to Rust) without breaking clients
- Must clearly communicate deprecation and upgrade paths
- Must be discoverable (developers should see version without inspecting headers)

---

## Decision

**Implement URL-based API versioning with the following rules**:

1. **Version in URL Path**: All endpoints under `/api/v<number>/` prefix
   - v1: `/api/v1/stations`, `/api/v1/partners`, etc.
   - v2: `/api/v2/stations`, `/api/v2/partners`, etc. (introduced MVP-2)

2. **Integer Versioning**: Simple numbering (v1, v2, v3...), not semantic versioning
   - v1 released Sprint 1.1 (MVP-1)
   - v2 released MVP-2 (Rust migration)
   - v3+ released as needed for breaking changes

3. **Version Immutability**: Once released, a version's API contract is frozen
   - v1 schemas locked in `specs/001-backend-and-database/contracts/api-v1.md`
   - No breaking changes to v1 endpoints during MVP-1, MVP-2 transition
   - v1 supported for 12 months after v2 release (then deprecated)

4. **No Version Field in Responses**: Version implicit in URL, no response field
   - Keeps responses lean and prevents version confusion
   - Clients know version from request URL they called

5. **Unversioned Paths Return 404**: No aliases or defaults
   - `/api/stations` → 404 (must use `/api/v1/stations`)
   - `/api/v999/stations` → 404 (invalid version)
   - Forces clients to be explicit about version

6. **Single Service per Version**: Each service implements one version
   - Python service in MVP-1: implements v1
   - Rust service in MVP-2: implements v1 AND v2 (v1 routers unchanged, v2 routers alongside)

---

## Rationale

### Why URL-Based Over Alternatives?

| Option | Pros | Cons | Decision |
|--------|------|------|----------|
| **URL-based** (/api/v1/...) | Discoverable, easy caching, explicit | Longer URLs | ✅ Chosen |
| Header-based (Accept: vnd.bornemap.v1+json) | RESTful, short URLs | Non-discoverable, requires header inspection | Rejected |
| Parameter-based (?version=1) | Explicit | Query params suggest filtering | Rejected |
| Content negotiation | Elegant | Requires pre-agreement with clients | Rejected |

### Why Integer Versioning?

- **Simplicity**: v1, v2, v3 are unambiguous
- **MVP alignment**: Each MVP cycle introduces major rewrite (Python → Rust → services split)
- **Industry standard**: GitHub, Stripe, Twitter use integer versioning
- **Avoids minor versions**: Prevents "should I update to 1.1?" confusion

### Why Version Immutability?

**Scenario**: Dashboard was built against v1 in Sprint 1.1. MVP-2 ships with v2. Dashboard can keep calling v1 indefinitely without code change. When Dashboard team is ready, they migrate to v2 at their own pace.

- Eliminates surprise breaking changes
- Allows async client upgrades
- Simplifies testing (v1 tests never change)

### Why 12-Month Deprecation Window?

- Long enough for slow-moving clients (mobile apps in stores, public drivers updating quarterly)
- Short enough to prevent API rot (maintaining 3+ versions is costly)
- Aligned with typical mobile app support lifecycle

### Why No Version Field in Responses?

- Version is already in URL (path-based versioning)
- Adding version field creates redundancy
- Reduces response payload
- Prevents "header vs body" conflicts

---

## Router-Based Implementation (FastAPI)

**Structure** (Python MVP-1):
```
app/
├── main.py                    # Register routers with prefixes
├── routers/
│   └── v1/
│       ├── health.py          # GET /api/v1/health
│       ├── partners.py        # GET|POST|PUT|DELETE /api/v1/partners*
│       ├── stations.py        # GET|POST|PUT|DELETE /api/v1/stations*
│       └── chargers.py        # GET|POST|PUT|DELETE /api/v1/chargers*
└── models/
    └── inventory.py           # Shared SQLAlchemy models
```

**Registration** (main.py):
```python
app.include_router(
    health.router,
    prefix="/api/v1",
    tags=["v1"],
)
```

**Benefits**:
- v1 code is isolated in separate module (no version flag spaghetti)
- v2 can be added alongside: `routers/v2/` without touching v1
- Schemas locked in `contracts/api-v1.md` (immutability enforced by review)
- Clear separation: each version in own file

---

## Deployment Strategy

### MVP-1 (Python, Sprint 1.1)

Single service, v1 only:
```
bornemap-service (Python FastAPI)
  ├── /api/v1/health
  ├── /api/v1/partners
  ├── /api/v1/stations
  └── /api/v1/chargers
```

### MVP-2 (Rust Migration)

Rust service replaces Python, serves both v1 and v2:
```
bornemap-service (Rust Actix-web)
  ├── /api/v1/...           # Identical to MVP-1 v1 (routers/ never modified)
  └── /api/v2/...           # New Rust-native implementation
```

**Key**: v1 routers are unchanged. Only additions allowed.

### MVP-3+ (Services Split)

Additional services added for specific domains, each supporting relevant versions:
```
partners-service (Rust)
  ├── /api/v1/partners/...
  └── /api/v2/partners/...

stations-service (Rust)
  ├── /api/v1/stations/...
  └── /api/v2/stations/...
```

**API Gateway** routes to appropriate service:
- `/api/v1/*` → routes to service with v1 implementation
- `/api/v2/*` → routes to service with v2 implementation

---

## Validation & Testing

### Schema Stability Tests

For MVP-2 migration (Python → Rust):

```python
def test_v1_health_schema():
    """v1 health schema unchanged between Python and Rust."""
    response = client.get("/api/v1/health")
    assert "status" in response.json()
    assert "service" in response.json()
    assert "db" in response.json()
```

**Run in MVP-1**: Capture baseline responses in `specs/001-backend-and-database/contracts/api-v1.md`

**Run in MVP-2**: After Rust deployment, compare Rust responses to Python baseline. Must match exactly.

### Version Enforcement Tests

```python
def test_unversioned_returns_404():
    """Unversioned endpoints must return 404."""
    assert client.get("/api/stations").status_code == 404

def test_invalid_version_returns_404():
    """Invalid versions must return 404."""
    assert client.get("/api/v999/stations").status_code == 404
```

**Run in every MVP**: Ensures versioning rules enforced.

---

## Documentation

### For Developers

**When**: Immediately after v1 API exists (Sprint 1.1)
- Create `/docs/api/bornemap-service.md` documenting all v1 endpoints
- Add `/api/docs` (Swagger UI) generated from FastAPI docstrings
- Include deprecation policy and migration guide template

**When v2 released**: 
- Add `/docs/guides/api-migration-v1-to-v2.md`
- Add deprecation notice to `/api/docs` and README
- Schedule v1 sunset date (12 months out)

### For Clients

**OpenAPI/Swagger**: http://localhost:8000/api/docs
- Auto-generated from FastAPI docstrings
- Shows all v1 endpoints clearly
- Allows interactive API testing

**Changelog**: `/docs/api/bornemap-service.md` updated for each version

---

## Migration Path (MVP-1 → MVP-2)

### Phase 1: Baseline (MVP-1, Sprint 1.1)

- v1 API implemented in Python
- All 16 endpoints under `/api/v1/`
- Response schemas frozen in `contracts/api-v1.md`
- Tests capture v1 behavior

### Phase 2: Rust Implementation (MVP-2)

1. Rust service implements v1 routers (identical contracts)
2. Deploy Rust service behind API gateway
3. Gateway routes `/api/v1/*` → Rust service (or Python, gradual migration)
4. Run compatibility tests: v1 responses must match Python baseline
5. Once confident, fully switch to Rust (API-layer no functional change)

### Phase 3: v2 Introduction (MVP-2+)

1. Rust service adds v2 routers with new features
2. New clients call `/api/v2/*`
3. Old clients keep calling `/api/v1/*` (routed to Rust)
4. Documentation updated with v2 endpoints

### Phase 4: v1 Deprecation (12 months post-v2)

1. 30-day notice: `Deprecation: true` header added to v1 responses
2. Sunset date reached: v1 endpoints return 404
3. All clients migrated to v2

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| **Version proliferation** (10+ versions) | Maintenance burden | Strict deprecation policy, maximum 2 active versions |
| **Client non-compliance** (ignoring versions) | API breakage | Enforce 404 for unversioned paths, document heavily |
| **Schema drift** (v1 modified) | Migration failure | Code review, frozen contracts file, immutability tests |
| **Wrong API version called** | Silent failures | Include version in response docs, error messages suggest v1 |

---

## Future Considerations

### Rate Limiting by Version (MVP-3+)

Deprecating versions may have lower rate limits:
```
/api/v1/* → 100 req/sec (legacy)
/api/v2/* → 1000 req/sec (current)
```

### Version-Specific Headers

In future, could add:
```
X-API-Version: v1
X-Sunset: 2027-06-08
```

But not required for MVP-1.

---

## Approval

**Product**: Sprint 1.1 includes full v1 API, supports 12-month deprecation window  
**Engineering**: Router-based implementation in FastAPI, tested schema immutability  
**Operations**: API Gateway can route by path prefix

---

## References

- **Specification**: `specs/001-backend-and-database/spec.md`
- **Implementation Plan**: `specs/001-backend-and-database/plan.md`
- **API Contract**: `specs/001-backend-and-database/contracts/api-v1.md`
- **Design Docs**: `specs/001-backend-and-database/quickstart.md`
- **API Documentation**: `docs/api/bornemap-service.md`
