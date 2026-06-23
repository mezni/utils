# VERSIONING POLICY

**Last Updated**: 2026-06-23  
**Status**: Active  
**Maintained By**: Release Manager  
**Related**: [Change Policy](./change-policy.md), [Decision Records](./decision-records.md)

---

## Overview

This document defines how versions are assigned and managed across the BorneMap EV Dashboard Platform. The platform uses Semantic Versioning (SemVer) for system and service versions, with independent versioning for APIs and epics.

### Core Principle

Version numbers reflect the **type of change** being released:
- **MAJOR**: Breaking changes (require client updates)
- **MINOR**: New features, backward compatible
- **PATCH**: Bug fixes, backward compatible

---

## 1. Versioning Scopes

The BorneMap platform has three independent versioning scopes:

### 1.1 System Version (MAJOR.MINOR.PATCH)

Global version of the entire platform.

**Format**: `1.0.0`

**Increment**:
- MAJOR: Breaking changes to multiple services
- MINOR: New non-breaking features/epics
- PATCH: Bug fixes and patches

**Example Timeline**:
```
1.0.0     → Initial release (E001 complete)
1.1.0     → E002 added (new features)
1.2.0     → E003 added (new features)
1.2.1     → Security patch applied
2.0.0     → System redesign (breaking changes)
```

**Responsibility**: Release Manager

### 1.2 Epic Version (MAJOR.MINOR.PATCH)

Independent version for each epic (E001, E002, etc.).

**Format**: `E001-1.0.0`

**Increment**:
- MAJOR: Epic redesign or breaking API changes
- MINOR: New features added to epic
- PATCH: Bug fixes within epic

**Example Timeline (E001)**:
```
E001-1.0.0    → Initial release (MVP dashboard)
E001-1.1.0    → Add charger status updates
E001-1.2.0    → Add real-time monitoring
E001-1.2.1    → Fix UI rendering bug
```

**Responsibility**: Epic Lead

### 1.3 API Version (URL Path)

API version is in the request path, NOT in headers.

**Format**: `/api/v1`, `/api/v2`, etc.

**Rules**:
- `/api/v1` is immutable once released
- New major versions go to `/api/v2`, `/api/v3`, etc.
- Old versions supported for 2+ releases
- Each version has independent deprecation timeline

**Example Timeline**:
```
/api/v1           → Initial API (E001)
/api/v1.1         → NOT USED (API versions don't use minor/patch)
/api/v2           → Breaking API changes (new major)
/api/v3           → Further breaking changes
```

**Responsibility**: API Owner

---

## 2. Semantic Versioning Rules

### 2.1 MAJOR Version Increment

Increment MAJOR when making **backward-incompatible changes**:

✅ **Examples of breaking changes**:
- Remove API endpoint
- Change request parameter (required to optional, vice versa)
- Change response field type (string → integer)
- Change HTTP status code
- Remove entity field from response
- Change database schema incompatibly

✅ **System MAJOR increment**:
- Major service redesign
- Multiple epics with breaking changes
- Significant architectural shift

✅ **Epic MAJOR increment**:
- Complete redesign of epic's API
- Incompatible entity structure changes
- Removal of features

### 2.2 MINOR Version Increment

Increment MINOR when adding **backward-compatible features**:

✅ **Examples of backward-compatible additions**:
- Add new API endpoint
- Add new optional request parameter
- Add new response field
- Add new entity status value
- Extend entity with new optional fields
- Add new database table

✅ **System MINOR increment**:
- Complete epic (E002, E003)
- New system features
- Service enhancements

✅ **Epic MINOR increment**:
- New features within epic
- Extended entity properties
- New endpoints

### 2.3 PATCH Version Increment

Increment PATCH for **bug fixes and maintenance**:

✅ **Examples of patches**:
- Fix UI rendering bug
- Fix database query performance
- Fix validation logic
- Fix error messages
- Update dependencies (minor)
- Security patches

---

## 3. Version Numbering Rules

### 3.1 Format

**System & Epic Versions**:
```
MAJOR.MINOR.PATCH

Examples:
1.0.0
1.2.3
2.0.0
E001-1.0.0
```

**API Versions**:
```
/api/vMAJOR

Examples:
/api/v1
/api/v2
/api/v3

(No MINOR.PATCH in URL)
```

### 3.2 Constraints

- Each component is a non-negative integer
- MAJOR must be >= 1 for production releases
- 0.x.0 is reserved for pre-release (alpha/beta)
- Never use leading zeros (01.0.0 is wrong)

### 3.3 Pre-release Versions

Use pre-release suffixes for early versions:

```
1.0.0-alpha      → First alpha
1.0.0-alpha.1    → Second alpha
1.0.0-beta       → Beta release
1.0.0-rc.1       → Release candidate
1.0.0            → Official release
```

**Rules**:
- Pre-release versions sort alphabetically before release
- Use in development/testing only
- Don't deploy pre-release to production

---

## 4. Version Bump Process

### 4.1 Determine New Version

```
Current: 1.2.3

If breaking change (incompatible)      → 2.0.0
If new feature (backward compatible)   → 1.3.0
If bug fix (no new features)           → 1.2.4
```

### 4.2 Create Release Branch

```bash
git checkout -b release/v1.3.0
```

### 4.3 Update Version Numbers

Update version in all files:

**Rust**:
```toml
# Cargo.toml
[package]
version = "1.3.0"
```

**Package.json (if applicable)**:
```json
{
  "version": "1.3.0"
}
```

**CHANGELOG.md**:
```markdown
## [1.3.0] - 2026-06-25

### Added
- New endpoint: GET /api/v1/stations/{id}/metrics
- Feature: Real-time charger status

### Fixed
- Bug: Partner list pagination off by one
```

**Version file**:
```bash
# Create: .version or VERSION file
1.3.0
```

### 4.4 Update CHANGELOG

Every release must update `CHANGELOG.md`:

```markdown
# Changelog

All notable changes to this project are documented here.

## [1.3.0] - 2026-06-25

### Added
- New endpoint: GET /api/v1/stations/{id}/metrics
- Feature: Real-time charger status updates
- Config option: ENABLE_REAL_TIME_UPDATES

### Changed
- Improved charger status response time
- Updated Station entity schema

### Fixed
- Bug: Partner list pagination calculation
- Bug: Null reference in charger update

### Security
- Updated dependencies for security patches

### Deprecated
- Endpoint: GET /api/v1/stations (use /api/v2/stations)

## [1.2.3] - 2026-06-20

### Fixed
- Database connection pool exhaustion

## [1.2.2] - 2026-06-18

### Fixed
- UI rendering on mobile devices
```

### 4.5 Commit and Tag

```bash
# Commit version updates
git add .
git commit -m "chore: bump version to 1.3.0"

# Create git tag
git tag -a v1.3.0 -m "Release version 1.3.0"

# Push changes and tag
git push origin release/v1.3.0
git push origin v1.3.0
```

### 4.6 Create Release

```bash
# Create GitHub Release
gh release create v1.3.0 \
  --title "v1.3.0 - Real-time Monitoring" \
  --notes-file CHANGELOG.md
```

### 4.7 Merge to Main

```bash
# Create PR
gh pr create --base main --head release/v1.3.0

# Merge after review
git checkout main
git merge release/v1.3.0
git push origin main
```

---

## 5. API Version Management

### 5.1 API Version Lifecycle

```
Time →

/api/v1         Active (stable)
                    ↓
            Deprecation announced
                    ↓ (2 releases later)
            Supported (deprecated)
                    ↓ (2+ releases)
            Sunset (removed)

/api/v2         Introduced at system v1.3.0
            ...
```

### 5.2 Deprecation Timeline

1. **Announce** (Release N):
   - Document in release notes
   - Update API docs
   - Email clients

2. **Support** (Release N+1, N+2):
   - Keep endpoint functional
   - Accept requests to old endpoint
   - Respond with deprecation header

3. **Sunset** (Release N+3+):
   - Remove endpoint
   - Return 410 Gone status
   - No new clients accepted

### 5.3 Deprecation Header

Include in responses to deprecated endpoints:

```http
HTTP/1.1 200 OK
Deprecation: true
Sunset: Sun, 25 Dec 2026 00:00:00 GMT
Link: </api/v2/partners>; rel="successor-version"
X-API-Warn: "API v1 is deprecated, use /api/v2"

{
  "success": true,
  "data": [...],
  "error": null
}
```

---

## 6. Backward Compatibility Promise

### 6.1 What We Guarantee

Within a version (e.g., all 1.x.x releases):

✅ **Guaranteed stable**:
- Existing endpoints remain
- Existing request parameters stay
- Response structure doesn't break

### 6.2 What We Don't Guarantee

❌ **Can change between versions**:
- New versions may break APIs
- Response fields may change type
- New required parameters may be added

### 6.3 What Counts as Breaking

The following require a major version bump:

- Remove endpoint
- Change required parameter
- Change response field type
- Change status code meaning
- Remove response field
- Rename endpoint

---

## 7. Release Schedule

### 7.1 Release Cadence

- **Patch releases** (1.x.z): As needed (security fixes)
- **Minor releases** (1.y.0): Every 2-4 weeks (new features)
- **Major releases** (2.0.0): Every 3-6 months (architectural changes)

### 7.2 Release Checklist

Before releasing:

- ✅ All tests passing
- ✅ CHANGELOG.md updated
- ✅ Version numbers bumped
- ✅ Release notes written
- ✅ Backward compatibility verified (if minor/patch)
- ✅ Security review completed
- ✅ Documentation updated
- ✅ Team notified

---

## 8. Version Support Timeline

The platform supports:

- **Current release**: Full support
- **1 prior release**: Security patches only
- **2+ releases old**: No support

**Example**:
```
Current: 2.1.0          → Full support (features + fixes)
Support: 2.0.x          → Security patches only
Support: 1.x.x          → None (encourage upgrade)
```

---

## 9. Emergency Hotfix Releases

For critical security/data issues:

```bash
# Create hotfix branch from main
git checkout -b hotfix/v1.2.4
git checkout main

# Make fix
# ...
# Test thoroughly
# ...

# Release
git tag v1.2.4
git push origin v1.2.4
```

**Rules**:
- Hotfixes ONLY for critical issues
- Hotfixes increment PATCH version
- Announce to all users immediately

---

## 10. Example Version Timeline

### E001 (Dashboard Kernel) Timeline

```
System    | Epic      | API    | Status
----------|-----------|--------|------------------
0.1.0     | E001-0.1  | /api/v1| Development
1.0.0-rc1 | E001-1.0  | /api/v1| Release candidate
1.0.0     | E001-1.0  | /api/v1| ✅ Release
1.1.0     | E001-1.1  | /api/v1| ✅ Release (E002 added)
1.1.1     | E001-1.1  | /api/v1| ✅ Patch release
1.2.0     | E001-1.2  | /api/v1| ✅ Release (E003 added)
1.3.0     | E001-1.2  | /api/v2| ✅ Release (API redesign)
```

---

## 11. Current Versions

### System Version

**Current**: `0.1.0` (in development)

**Next**: `1.0.0` (when E001 complete)

### Epic Versions

| Epic | Version | Status | Next Milestone |
|------|---------|--------|----------------|
| E001 | 0.1.0   | In Development | 1.0.0 (MVP) |

### API Version

**Current**: `/api/v1` (in development)

**Status**: Pre-release

---

## 12. Checklist for Maintainers

When releasing a new version:

- ✅ Branch created from correct base
- ✅ Version numbers updated consistently
- ✅ CHANGELOG.md entry added
- ✅ Git tag created with `v` prefix
- ✅ Release notes written
- ✅ Backward compatibility verified
- ✅ Team notified
- ✅ Clients notified (if breaking)
- ✅ Documentation updated

---

## 13. See Also

- [Change Policy](./change-policy.md) - How to propose changes
- [Decision Records](./decision-records.md) - Architectural decisions
- [CHANGELOG.md](../../CHANGELOG.md) - Release history
- [Constitution](../core/constitution.md) - Core principles
