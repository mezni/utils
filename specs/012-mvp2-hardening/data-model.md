# Data Model: MVP-2 Hardening (Verification Scope)

**Branch**: `012-mvp2-hardening` | **Date**: 2026-06-09

## Overview

This sprint introduces no new entities, fields, or relationships. All verification targets existing entities from Sprint 2.2 (Database Schema).

## Entities Under Verification

### Partner

| Attribute | Sprint Introduced | Verification Target |
|-----------|-------------------|---------------------|
| `is_active` | 2.2 | Flag toggling via Admin Service; exclusion from driver results when false |
| `is_verified` | 2.2 | Verify action via Admin Service; exclusion from driver results when false |
| `is_live` | 2.2 | Set live via Admin Service; exclusion from driver results when false |
| `partner_type` (business/personal) | 2.2 | CRUD via Admin Service |
| `created_by` / `updated_by` | 2.2 | Written correctly by Admin Service (default "admin") |

### Station

| Attribute | Sprint Introduced | Verification Target |
|-----------|-------------------|---------------------|
| `latitude` / `longitude` | 2.2 | Spatial index scan confirmed by EXPLAIN ANALYZE |
| `partner_id` | 2.2 | Visibility JOIN on partner flags | 

### Charger

| Attribute | Sprint Introduced | Verification Target |
|-----------|-------------------|---------------------|
| `station_id` | 2.2 | Inclusion/exclusion based on parent station visibility |
| `connector_type` / `power_kw` | 2.2 | CRUD via Admin Service |

### Station Availability

| Attribute | Sprint Introduced | Verification Target |
|-----------|-------------------|---------------------|
| `status` | 2.2 | Append-only via Admin Service |
| `station_id` | 2.2 | FK constraint verified |

## State Transitions Under Test

### Partner Lifecycle

```text
Created (is_active=true, is_verified=false, is_live=false)
  │
  ├── Verify ──────────────────> is_verified=true
  │                               │
  │                               └── Set Live ────────> is_live=true
  │                                                        │
  │                                                        ├──> Driver sees stations
  │                                                        │
  │                                                        └── Deactivate ──> is_active=false
  │                                                                              │
  │                                                                              └──> Driver hides stations
  │
  └── Delete ──> Soft delete (is_active=false via CASCADE)
```

### Visibility Rule

```sql
-- The JOIN that enforces visibility in all driver-facing queries:
JOIN inventory.partner p ON s.partner_id = p.id
WHERE p.is_active = true 
  AND p.is_verified = true 
  AND p.is_live = true
```

## Index Verification

| Index | Created In | Type | Verified By |
|-------|-----------|------|-------------|
| `idx_station_coordinates` | Migration 0003 | GIST on `geometry(Point, 4326)` | EXPLAIN ANALYZE on ST_DWithin query |

## Seed Data for Verification

| Seed | Partners | Stations | Chargers | Availability |
|------|----------|----------|----------|-------------|
| 001-004 | 3 (2 valid, 1 unverified) | 15 | 24 | 15 |

Partner 3 (`is_verified=false`) should be excluded from all driver-facing queries.
