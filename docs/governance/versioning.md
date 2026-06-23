# VERSIONING POLICY

---

## 1. System Versioning Model

The platform uses independent versioning scopes:

- System Version (global platform)
- Epic Version (E001, E002, etc.)
- API Version (/api/v1)

---

## 2. Rules

- Breaking changes MUST increment major version
- API changes MUST never break backward compatibility within a version
- Epics are versioned independently

---

## 3. Version Format

Format:
MAJOR.MINOR.PATCH

Example:
1.0.0 → initial system
1.1.0 → new non-breaking feature
2.0.0 → breaking change

---

## 4. API Versioning Rule

- /api/v1 is immutable once released
- new API versions must be /api/v2, etc.
