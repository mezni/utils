# API Contracts: MVP-1 Stabilization Sprint

**Date**: 2026-06-13
**Feature**: MVP-1 Stabilization Sprint
**Reference**: [spec.md](./spec.md), [research.md](./research.md)

## Overview

This document defines API contracts for performance optimizations, event tracking reliability, and monitoring endpoints.

---

## 1. Optimized Stations List

### Endpoint
`GET /api/v1/stations`

### Purpose
Fetch paginated list of stations with optimized payload size and performance.

### Performance Requirements
- **Response Time**: <200ms p95
- **Payload Size**: Reduced by stripping null fields
- **Caching**: ETag support for conditional requests

### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| page | integer | No | Page number (default: 1) |
| per_page | integer | No | Items per page (default: 20, max: 100) |

### Request Example
```bash
curl -H "Accept: application/json" \
     -H "Accept-Encoding: gzip" \
     -H "If-None-Match: "1234567890"" \
     "http://localhost:8080/api/v1/stations?page=1&per_page=20"
```

### Response Headers

| Header | Description |
|--------|-------------|
| Content-Type | application/json |
| ETag | MD5 hash of response body (for caching) |
| Cache-Control | public, max-age=60 (1 minute) |
| X-Response-Time | Response time in milliseconds |

### Response Body
```json
{
  "data": [
    {
      "id": "STA-sfax-borj",
      "name": "STEG Sfax Borj",
      "address": "Route de Gabès, Sfax",
      "status": "available",
      "partner_id": "PRT-steg",
      "chargers": null,
      "partner_name": "STEG",
      "distance_km": null,
      "created_at": "2026-06-13T12:52:50.249359Z",
      "updated_at": "2026-06-13T12:52:50.249359Z"
    }
  ],
  "total": 5,
  "page": 1,
  "per_page": 20,
  "total_pages": 1,
  "previous_page": null,
  "next_page": null
}
```

**Note**: Null fields stripped from `lat`, `lng`, `opening_hours`, `chargers`, `partner_id` (replaced with `partner_name`)

### Error Responses

| Status | Description |
|--------|-------------|
| 400 | Invalid page or per_page values |
| 404 | Page out of range |
| 500 | Internal server error |

---

## 2. Optimized Nearby Stations

### Endpoint
`GET /api/v1/stations/nearby`

### Purpose
Find stations within a geographic radius with optimized query and payload.

### Performance Requirements
- **Response Time**: <100ms p95
- **Payload**: Minimal fields only (id, name, address, distance_km, status)
- **Spatial Index**: GIST index on location column

### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| lat | float | Yes | Latitude (-90 to 90) |
| lng | float | Yes | Longitude (-180 to 180) |
| radius | float | Yes | Search radius in kilometers (0.1 to 100) |

### Request Example
```bash
curl -H "Accept: application/json" \
     -H "Accept-Encoding: gzip" \
     "http://localhost:8080/api/v1/stations/nearby?lat=36.8&lng=10.1&radius=50"
```

### Response Headers

| Header | Description |
|--------|-------------|
| Content-Type | application/json |
| X-Response-Time | Response time in milliseconds |
| X-Cache-Status | hit/miss (for caching) |

### Response Body
```json
{
  "data": [
    {
      "id": "STA-tunis-centre",
      "name": "TotalEnergies Tunis Centre",
      "address": "Avenue Habib Bourguiba, Tunis",
      "status": "available",
      "distance_km": 7.19,
      "lat": 36.7995,
      "lng": 10.1806
    }
  ],
  "total": 2
}
```

**Note**: Only essential fields returned (no partner_id, chargers, opening_hours)

### Error Responses

| Status | Description |
|--------|-------------|
| 400 | Invalid lat/lng/radius (out of bounds) |
| 404 | No stations found in radius |
| 500 | Internal server error |

### Performance Profile
```bash
# Profile this endpoint
curl -w "@curl-format.txt" "http://localhost:8080/api/v1/stations/nearby?lat=36.8&lng=10.1&radius=50"
```

**curl-format.txt**:
```
time_namelookup:  %{time_namelookup}s
time_connect:  %{time_connect}s
time_appconnect:  %{time_appconnect}s
time_pretransfer:  %{time_pretransfer}s
time_redirect:  %{time_redirect}s
time_starttransfer:  %{time_starttransfer}s
time_total:  %{time_total}s
```

---

## 3. Event Batch Ingestion (Single Event)

### Endpoint
`POST /api/v1/events`

### Purpose
Log a single user interaction event for analytics.

### Performance Requirements
- **Response Time**: <100ms p95
- **Reliability**: Immediate acknowledgment, background processing
- **Error Handling**: Log errors, don't block UI

### Request Headers

| Header | Description |
|--------|-------------|
| Content-Type | application/json |
| X-Device-Info | JSON string (model, os, screen_size) |

### Request Body
```json
{
  "event_type": "station_view",
  "user_action": "marker_press",
  "station_id": "STA-tunis-centre",
  "timestamp": "2026-06-13T10:30:00Z"
}
```

**Event Type Enums**:
- station_view
- station_list_view
- search_nearby
- map_pan
- marker_press
- station_detail_view
- settings_toggle
- theme_change
- error_occurred

### Response Headers

| Header | Description |
|--------|-------------|
| Content-Type | application/json |
| X-Event-ID | Event ID for correlation |

### Response Body
```json
{
  "success": true,
  "event_id": "uuid-here",
  "timestamp": "2026-06-13T10:30:00Z"
}
```

### Error Responses

| Status | Description |
|--------|-------------|
| 400 | Invalid event_type or user_action |
| 429 | Rate limited (batch queue full) |
| 500 | Internal server error (database unavailable) |

### Failure Handling
- If analytics database unreachable, log error locally with retry queue
- Do not block UI (asynchronous processing)
- Return 202 Accepted for all requests (asynchronous)

---

## 4. Event Batch Ingestion (Batch)

### Endpoint
`POST /api/v1/events/batch`

### Purpose
Ingest multiple events in a single request (max 100 events).

### Performance Requirements
- **Batch Size**: Max 100 events
- **Timeout**: 500ms for batch processing
- **Response Time**: <500ms p95 for successful batches
- **Reliability**: Retry on transient failures, drop on permanent failures

### Request Headers

| Header | Description |
|--------|-------------|
| Content-Type | application/json |
| X-Device-Info | JSON string (model, os, screen_size) |
| X-Batch-Timeout | Timeout in milliseconds (default: 500) |

### Request Body
```json
{
  "events": [
    {
      "event_type": "station_view",
      "user_action": "marker_press",
      "station_id": "STA-tunis-centre",
      "timestamp": "2026-06-13T10:30:00Z"
    },
    {
      "event_type": "station_list_view",
      "user_action": "load_stations",
      "timestamp": "2026-06-13T10:30:05Z"
    }
  ],
  "batch_id": "uuid-here",
  "retry_count": 0
}
```

### Response Headers

| Header | Description |
|--------|-------------|
| Content-Type | application/json |
| X-Processed-Count | Number of events successfully processed |
| X-Failed-Count | Number of events failed (0 if all succeeded) |

### Response Body
```json
{
  "success": true,
  "batch_id": "uuid-here",
  "processed_count": 2,
  "failed_count": 0,
  "timestamp": "2026-06-13T10:30:00Z"
}
```

### Error Responses

| Status | Description |
|--------|-------------|
| 400 | Invalid events (null, empty array, wrong structure) |
| 429 | Batch size > 100 (max 100) |
| 429 | Timeout exceeded (batch processing > 500ms) |
| 500 | Internal server error (database unreachable) |

### Retry Logic

**Transient Failures** (retry):
- Network timeout
- Temporary database unavailability
- Rate limiting (temporary)

**Permanent Failures** (drop):
- Event type validation error
- Invalid event structure
- Analytics database permanently unavailable

**Retry Strategy**:
- Max retries: 3
- Exponential backoff: 2s, 5s, 10s
- After 3 retries, drop event and log error

### Batch Timeout
- Default: 500ms
- If batch processing exceeds timeout, return 429 (Too Many Requests)
- Client should retry after delay

### Response Example (Timeout)
```json
{
  "success": false,
  "error": "batch_timeout",
  "message": "Batch processing exceeded timeout threshold",
  "processed_count": 50,
  "failed_count": 50,
  "timestamp": "2026-06-13T10:30:00Z"
}
```

---

## 5. Performance Metrics Endpoint (Admin Only)

### Endpoint
`GET /api/v1/admin/performance/metrics`

### Purpose
Retrieve performance metrics for monitoring and analysis (admin-only endpoint).

### Performance Requirements
- **Response Time**: <200ms p95
- **Auth**: JWT required (MVP-3 scope)

### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| metric_type | string | No | Filter by metric type (response_time, frame_rate, etc.) |
| user_action | string | No | Filter by user action |
| start_date | string | No | ISO 8601 start date (default: 24 hours ago) |
| end_date | string | No | ISO 8601 end date (default: now) |
| limit | integer | No | Max results (default: 100, max: 1000) |

### Request Example
```bash
curl -H "Authorization: Bearer <token>" \
     -H "Content-Type: application/json" \
     "http://localhost:8081/api/v1/admin/performance/metrics?metric_type=response_time&user_action=station_list_view&start_date=2026-06-12T00:00:00Z"
```

### Response Headers

| Header | Description |
|--------|-------------|
| Content-Type | application/json |
| X-Total-Count | Total number of matching metrics |

### Response Body
```json
{
  "data": [
    {
      "id": "uuid-1",
      "metric_type": "response_time",
      "value_ms": 185.3,
      "user_action": "station_list_view",
      "device_info": {
        "model": "iPhone 14",
        "os": "iOS 17",
        "screen_size": "6.1"
      },
      "timestamp": "2026-06-13T10:30:00Z",
      "environment": "production"
    }
  ],
  "total": 1000,
  "page": 1,
  "per_page": 100
}
```

### Error Responses

| Status | Description |
|--------|-------------|
| 401 | Unauthorized (missing or invalid JWT) |
| 403 | Forbidden (role not authorized) |
| 400 | Invalid query parameters |
| 500 | Internal server error |

### Authentication

Requires JWT token with `admin` role:
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "role": "admin"
}
```

---

## 6. Error Log Endpoint (Admin Only)

### Endpoint
`GET /api/v1/admin/errors`

### Purpose
Retrieve error logs for debugging and monitoring (admin-only endpoint).

### Performance Requirements
- **Response Time**: <200ms p95
- **Auth**: JWT required (MVP-3 scope)

### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| event_type | string | No | Filter by event type (network_error, server_error, etc.) |
| user_action | string | No | Filter by user action |
| start_date | string | No | ISO 8601 start date |
| end_date | string | No | ISO 8601 end date |
| retry_count | integer | No | Filter by retry count |
| limit | integer | No | Max results (default: 100, max: 1000) |

### Request Example
```bash
curl -H "Authorization: Bearer <token>" \
     -H "Content-Type: application/json" \
     "http://localhost:8081/api/v1/admin/errors?event_type=network_error&start_date=2026-06-12T00:00:00Z"
```

### Response Headers

| Header | Description |
|--------|-------------|
| Content-Type | application/json |
| X-Total-Count | Total number of matching error logs |

### Response Body
```json
{
  "data": [
    {
      "id": "uuid-1",
      "event_type": "network_error",
      "user_action": "load_stations",
      "error_message": "Connection timeout after 30000ms",
      "device_info": {
        "model": "Pixel 7",
        "os": "Android 13",
        "screen_size": "6.3"
      },
      "timestamp": "2026-06-13T10:35:00Z",
      "recovery_action": "retry_button_pressed",
      "recovery_success": true,
      "retry_count": 2,
      "metadata": {
        "error_code": "ETIMEDOUT",
        "request_id": "req-123"
      }
    }
  ],
  "total": 50,
  "page": 1,
  "per_page": 100
}
```

### Error Responses

| Status | Description |
|--------|-------------|
| 401 | Unauthorized |
| 403 | Forbidden (role not admin) |
| 400 | Invalid query parameters |
| 500 | Internal server error |

---

## Summary of Contracts

| Endpoint | Method | Performance | Auth | Purpose |
|----------|--------|-------------|------|---------|
| `/api/v1/stations` | GET | <200ms p95 | No | Paginated stations list (optimized) |
| `/api/v1/stations/nearby` | GET | <100ms p95 | No | Radius search (minimal payload) |
| `/api/v1/events` | POST | <100ms p95 | No | Single event logging |
| `/api/v1/events/batch` | POST | <500ms p95 (successful) | No | Batch event ingestion (max 100) |
| `/api/v1/admin/performance/metrics` | GET | <200ms p95 | Yes (JWT) | Performance metrics monitoring |
| `/api/v1/admin/errors` | GET | <200ms p95 | Yes (JWT) | Error log analysis |

**Key Optimization Points**:
- Strip null fields from JSON responses
- Use GIST index for geospatial queries
- Batch event ingestion with retry logic
- ETag support for caching
- Minimal field projection for nearby search
- Performance profiling headers (X-Response-Time)
- Append-only analytics with integrity rules
