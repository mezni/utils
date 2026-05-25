# BorneMap — Identity, Security & Geospatial Constraints

## 1. Semantic Identifier Invariant

Raw binary UUIDs are **rejected**. Every primary and foreign key generated across the three applications must utilize a strict, human-readable prefix pattern combined with a URL-safe, alphanumeric Nanoid (12 characters, lowercase + numeric alphabet).

### Prefix System Registry

| Prefix | Domain | Description |
|--------|--------|-------------|
| `USR-` | User Identity | User profiles (Drivers, Partners, Admins) |
| `PRT-` | Partner Operator | Multi-tenant Partner corporate or individual private nodes |
| `STN-` | Station | Spatial Charging Station locations |
| `CHG-` | Charger | Physical Charger ports/hardware records |
| `CNT-` | Connector Type | Dynamic Configuration Connector Types |
| `REV-` | Review | Driver user reviews and platform engagement loops |

### Format Example

```
USR-m1k9p2v4x7q3
│    │
│    └── 12-character lowercase alphanumeric Nanoid
└── Domain prefix (3 chars + hyphen)
```

## 2. Geospatial Standards

All coordinate elements must be stored, computed, and passed via network APIs using the canonical PostGIS **Longitude-First** geography point mapping scheme:

- **Storage format**: `[longitude, latitude]` with SRID 4326
- **API transport**: Longitude first, latitude second
- **PostGIS construction**: `ST_MakePoint(lng, lat)`

## 3. Multi-Tenant Isolation Guardrail

Multi-tenancy boundaries are **strictly enforced at the database extraction tier**. Partner Dashboard API requests inject the verified partner's user context key (`owner_id` mapping back to a verified `USR-` token) automatically into all queries.

- Partners can **only** see stations, chargers, and data they own
- The `owner_id` filter is applied at the repository/query layer, not the client layer
- No partner-scoped endpoint may omit the `owner_id` constraint

## 4. Environment-Aware Testing Flags

The `is_test` boolean flag operates universally across all 3 applications.

### Behavior Matrix

| Flag State | Admin Portal | Partner Dashboard | Mobile App |
|-----------|-------------|-------------------|------------|
| `is_test = true` | Visible with blue top border indicator | Excluded from analytics | **Hidden** |
| `is_test = false` | Normal display | Normal display | Normal display |

### Sandbox Workspace Selector

When toggled in the Admin UI, a persistent **blue top border indicator** (`border-t-4 border-sky-500`) illuminates to visually separate test views from production data.

### Purpose

Allows safe developer deployments in a shared sandbox database environment where test and production records coexist without cross-contamination.
