# Data Model: Cargo Workspace and Shared Crates

**Phase**: Phase 1 — Entity definitions for Sprint 2.1

**Date**: 2026-06-09

## ev-core — Shared Enums and NanoID

### NanoId

| Property | Type | Description |
|----------|------|-------------|
| `prefix` | `&str` | Alphanumeric prefix prepended to the random ID (may be empty) |
| `length` | `usize` | Number of random characters after prefix (must be > 0) |
| `alphabet` | `&str` | Characters to draw from (defaults to URL-safe `A-Za-z0-9`) |
| `output` | `String` | `{prefix}{random_chars}` e.g., `PRT_A3bX9kQm` |

**Validation rules**:
- `length` must be >= 1 (panic if 0 — programmer error)
- `prefix` may be empty (purely random ID)
- `alphabet` must contain at least 2 characters (panic if < 2)

### ConnectorType

| Variant | Serialized | Description |
|---------|-----------|-------------|
| `Type2` | `"type2"` | Type 2 AC connector (IEC 62196) |
| `Type3` | `"type3"` | Type 3 AC connector (deprecated/scala) |
| `CCS` | `"ccs"` | Combined Charging System (DC) |
| `CHAdeMO` | `"chademo"` | CHAdeMO DC connector |

### ChargerStatus

| Variant | Serialized | Description |
|---------|-----------|-------------|
| `Available` | `"available"` | Charger is idle and ready |
| `InUse` | `"in_use"` | Charger is currently occupied |
| `Maintenance` | `"maintenance"` | Charger is under maintenance |
| `Offline` | `"offline"` | Charger is offline/unreachable |

### PartnerType

| Variant | Serialized | Description |
|---------|-----------|-------------|
| `Business` | `"business"` | Commercial/business partner |
| `Personal` | `"personal"` | Individual/personal partner |

### StationStatus

| Variant | Serialized | Description |
|---------|-----------|-------------|
| `Available` | `"available"` | All chargers operational |
| `Partial` | `"partial"` | Some chargers unavailable |
| `Unavailable` | `"unavailable"` | No chargers operational |

## ev-db — Database Pool and Pagination

### PoolConfig

| Property | Type | Description |
|----------|------|-------------|
| `connection_string` | `&str` | PostgreSQL connection URI |
| `max_connections` | `u32` | Maximum pool size (default: 10) |
| `connection_timeout` | `Duration` | Timeout for new connections (default: 30s) |

**Validation rules**:
- `connection_string` must be a valid PostgreSQL URI (`postgres://user:pass@host:port/dbname`)
- Missing or malformed connection string returns `PoolError::InvalidConnectionString(reason)`

### Paginated\<T\>

| Field | Type | Description |
|-------|------|-------------|
| `data` | `Vec<T>` | The items for the current page |
| `total` | `u64` | Total number of items across all pages |
| `page` | `u32` | Current page number (1-indexed) |
| `page_size` | `u32` | Number of items per page |
| `total_pages` | `u32` | Total number of pages (`ceil(total / page_size)`) |

**Validation rules**:
- `page` >= 1 (caller guarantees; panics if 0)
- `page_size` >= 1 (caller guarantees; panics if 0)
- If `total` = 0, then `total_pages` = 0
- If `total` > 0, then `total_pages` = `total.div_ceil(page_size)`
