# BorneMap — Security Review

## 1. Security Principles

1. **Validate all inputs** — every API endpoint must validate its parameters
2. **Sanitize all external data** — no raw user input in SQL queries or responses
3. **Never expose stack traces** — all errors return sanitized messages
4. **Rate limit public endpoints** — Driver Service (public) must be rate-limited
5. **No sensitive leakage** — passwords, internal IDs, stack traces never exposed

## 2. Input Validation Rules

| Endpoint | Field | Rule |
|----------|-------|------|
| POST /partners | name | 1–100 chars, non-empty |
| POST /stations | name | 1–150 chars |
| POST /stations | address | 1–250 chars |
| POST /stations | latitude | -90 to 90 |
| POST /stations | longitude | -180 to 180 |
| POST /stations | partner_id | Must exist in ev.partners |
| POST /connectors | power_kw | > 0 and < 1000, numeric |
| POST /connectors | type | Must be valid enum |
| GET /stations/nearby | lat | -90 to 90 |
| GET /stations/nearby | lng | -180 to 180 |
| GET /stations/nearby | radius | > 0, < 100000 (100km) |

## 3. SQL Injection Prevention

- Use **SQLx compile-time verified queries** (`query!`, `query_as!`)
- All parameters are bound, never interpolated
- No raw SQL string building in application code

## 4. GIS Function Security

- `gis.nearby_stations()` is defined with `SECURITY INVOKER` (default)
- No dynamic SQL inside the function
- Parameters are typed (DOUBLE PRECISION), preventing injection

## 5. API Security Headers (Phase 2)

```
Content-Security-Policy: default-src 'self'
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Strict-Transport-Security: max-age=31536000
```

## 6. Authentication (Phase 2)

- JWT-based auth for Admin Service
- Password hashing via bcrypt/argon2
- Role-based access (admin, partner, driver)
- Token expiry and refresh mechanism

## 7. Rate Limiting (Phase 2)

| Endpoint | Rate |
|----------|------|
| GET /stations/nearby | 100 requests/min per IP |
| POST /auth/login | 5 requests/min per IP |
| Admin endpoints | 30 requests/min per user |

## 8. Error Handling

```json
// SAFE — no internal info leaked
{
  "data": null,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid latitude value"
  }
}

// NEVER return:
// - Stack traces
// - Internal file paths
// - Database error details
// - SQL queries
```

## 9. Phase 1 Security (No Auth)

During Phase 1 (no auth), the following apply:
- Admin Service runs on localhost/internal network only
- Driver Service is public but read-only
- Rate limiting applies to all public endpoints
- Input validation is strictly enforced
