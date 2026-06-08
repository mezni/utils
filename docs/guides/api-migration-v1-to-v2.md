# API Migration Guide: v1 to v2

**Status**: DRAFT (v2 will be released in MVP-2)  
**Created**: 2026-06-08  
**Target Version**: v2.0 (MVP-2 Rust migration)

---

## Overview

This guide is prepared for the v2 API release, expected in MVP-2 when the Python FastAPI service is migrated to Rust. Client developers should follow this guide when upgrading from v1 to v2.

**Timeline**:
- v2 released: TBD (MVP-2)
- v1 support ends: 12 months after v2 release
- Migration deadline: Coordinated with v1 sunset (30-day notice before deadline)

---

## Quick Start

### Before Migration

1. Review v1 endpoints you're using in `docs/api/bornemap-service.md` (v1 section)
2. Check v2 endpoint list in `/api/docs` (after v2 released)
3. Note any behavior changes in "Breaking Changes" section below

### During Migration

1. Update base URL: `http://api.bornemap.tn/api/v2/` (note `v2`)
2. Review response schema changes (if any)
3. Test with v2 sandbox endpoint (if available)
4. Deploy to production

### After Migration

1. Verify v2 endpoints respond correctly
2. Monitor error rates for 24 hours
3. Mark v1 code as deprecated in your codebase
4. Schedule v1 removal after v2 is stable

---

## What's Changing in v2?

### Endpoints Overview

All v1 endpoints have v2 equivalents under `/api/v2/` prefix.

**v2 new features** (to be documented when v2 released):
- TBD

**v2 improvements** (to be documented when v2 released):
- TBD

### URL Changes

Simply change the version prefix:

```python
# Before (v1)
requests.get("http://api.bornemap.tn/api/v1/stations")

# After (v2)
requests.get("http://api.bornemap.tn/api/v2/stations")
```

### Response Schema Changes

**TBD when v2 released** — this guide will be updated with specific schema changes.

---

## Breaking Changes

To be documented when v2 released. Expected areas:

- Response fields (additions, removals, renames)
- Query parameters (new, removed, renamed)
- Error responses (new error codes)
- Behavior changes (sorting, filtering, pagination)

---

## Migration Path

### Step 1: Update Configuration

```python
# Before
API_BASE_URL = "https://api.bornemap.tn/api/v1"

# After
API_BASE_URL = "https://api.bornemap.tn/api/v2"
```

### Step 2: Review Response Handling

Check your code for hardcoded field assumptions:

```python
# Before (assumes v1 response structure)
response = requests.get(f"{API_BASE_URL}/stations")
station = response.json()["data"][0]
print(station["charger_count"])

# After (verify field names match v2)
response = requests.get(f"{API_BASE_URL}/stations")
station = response.json()["data"][0]
# Verify "charger_count" exists in v2, or use new field name
print(station["charger_count"])  # or new field name
```

### Step 3: Test Thoroughly

```python
import requests

def test_v2_endpoints():
    """Test all v2 endpoints your app uses."""
    base_url = "https://api.bornemap.tn/api/v2"
    
    # Test each endpoint
    endpoints = [
        "/health",
        "/stations",
        "/partners",
        "/chargers",
    ]
    
    for endpoint in endpoints:
        response = requests.get(f"{base_url}{endpoint}")
        assert response.status_code == 200, f"{endpoint} failed"
        print(f"✓ {endpoint}")

test_v2_endpoints()
```

### Step 4: Deploy

1. Verify v2 is stable in production
2. Merge v2 code changes to `main` branch
3. Deploy to staging, then production
4. Monitor error rates for 24 hours

### Step 5: Cleanup

1. Remove v1 fallback code
2. Remove v1 endpoint references from documentation
3. Update README to show v2 examples

---

## Fallback Strategy

### During Transition Period

If v2 has issues, you can temporarily fallback to v1:

```python
import requests

def get_stations(use_v2=True):
    """Fetch stations from v1 or v2 based on flag."""
    version = "v2" if use_v2 else "v1"
    url = f"https://api.bornemap.tn/api/{version}/stations"
    return requests.get(url).json()

# Use v2
stations = get_stations(use_v2=True)

# Fallback to v1 if needed
if error_detected:
    stations = get_stations(use_v2=False)
```

### Support Windows

- **v1 active**: From Sprint 1.1 → 12 months after v2 release
- **v2 active**: From MVP-2 release → TBD
- **Both active**: 12-month window (recommended for gradual migration)
- **v1 only**: Before MVP-2 (current state)

---

## FAQ

### Q: Can I keep using v1 forever?

**A**: No. v1 is deprecated 12 months after v2 release. Plan your migration during that window.

### Q: What if v2 has a bug?

**A**: v1 is still available during the 12-month window. Fallback to v1 temporarily while the bug is fixed in v2.

### Q: How do I know if I'm on v1 or v2?

**A**: Look at the URL path:
- `/api/v1/...` = v1
- `/api/v2/...` = v2

Version is NOT included in response body.

### Q: Will v1 endpoints return 404 when v2 is released?

**A**: No. Both v1 and v2 are active for 12 months after v2 release. After 12 months, v1 returns 404.

### Q: Do I need to change my database or models?

**A**: Only if v2 introduces new fields or endpoints that your app needs to store. v1 compatibility is API-level, not database-level.

---

## Common Issues

### Issue: 404 Not Found on v2 Endpoints

**Cause**: v2 not yet released, or API gateway not configured for v2

**Fix**: 
1. Check v2 release date (TBD)
2. Verify URL: should be `/api/v2/...`
3. Confirm with operations team that v2 is deployed

### Issue: v2 Response Fields Different

**Cause**: v2 schema changed from v1

**Fix**:
1. Compare v1 and v2 schemas in `/api/docs`
2. Update response handling code
3. Update unit tests with new field names

### Issue: v2 Slower Than v1

**Cause**: Rust migration may have performance differences

**Fix**:
1. Report to engineering team
2. Monitor for improvements in patch releases
3. Optimize your app's API call patterns

---

## Support

### Before v2 Released

- v1 is current, stable version
- Use v1 for new projects
- Bug reports: `/docs/project/bugs.md`

### After v2 Released

- v2 is current, recommended version
- v1 still supported for 12 months
- Migration questions: See this guide
- Bug reports: `/docs/project/bugs.md`

### Escalation

If you encounter issues:

1. Check `docs/api/bornemap-service.md` for v2 documentation
2. Review this migration guide
3. Report in GitHub issues: https://github.com/anomalyco/opencode/issues

---

## Changelog (to be filled when v2 released)

### v2.0.0 (MVP-2)

**New Features**:
- TBD

**Breaking Changes**:
- TBD

**Deprecations**:
- v1 deprecated after 12 months (sunset date: TBD)

**Migration Guide**: See this document

---

## Template: Your App's Migration Checklist

Use this checklist to track your v2 migration:

```
v2 Migration Checklist for [YOUR_APP]
=====================================

[ ] Read v2 API documentation
[ ] Review breaking changes
[ ] Update base URL to /api/v2/
[ ] Test all v1 endpoints on v2
[ ] Update response field handling
[ ] Update unit/integration tests
[ ] Deploy to staging
[ ] Test in staging for 1 week
[ ] Deploy to production
[ ] Monitor errors for 24 hours
[ ] Remove v1 fallback code
[ ] Update app documentation
[ ] Archive v1 code (keep in git history)
```

---

## References

- **v1 API Docs**: `docs/api/bornemap-service.md` (v1 section)
- **v2 API Docs**: `docs/api/bornemap-service.md` (v2 section, after release)
- **API Versioning ADR**: `docs/adr/ADR-018-api-versioning.md`
- **Live Documentation**: http://api.bornemap.tn/api/docs (Swagger UI)
- **OpenAPI Spec**: http://api.bornemap.tn/api/openapi.json

---

**Last Updated**: 2026-06-08  
**Status**: DRAFT (will be finalized when v2 released)
