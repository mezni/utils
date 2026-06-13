# Security Evolution Skill — BorneMap

## Purpose
Prevent insecure patterns in Rust backend and APIs.

---

## 🎯 Core Philosophy

**Security is not an afterthought. Security is architecture.**

Security patterns evolve with MVP progression.

---

## 🚦 Core Rules

### 1. MVP-Aware Security

**Security only introduced when needed:**

```rust
// ❌ WRONG - Security in MVP-1
pub async fn handle_get_stations(/*...*/) -> Result<ApiResponse<StationDto>, ApiError> {
  // ❌ No auth in MVP-1
  let stations = stations_service::get_all(db).await?;
  Ok(ApiResponse::new(stations))
}

// ✅ CORRECT - MVP-1 (no security needed)
pub async fn handle_get_stations(/*...*/) -> Result<ApiResponse<StationDto>, ApiError> {
  // ✅ No auth in MVP-1
  let stations = stations_service::get_all(db).await?;
  Ok(ApiResponse::new(stations))
}

// ❌ WRONG - Security in MVP-1 (not yet introduced)
pub async fn handle_get_stations(/*...*/) -> Result<ApiResponse<StationDto>, ApiError> {
  // ❌ Security not available yet
  // ❌ Breaks MVP-1 functionality
  let stations = auth_service::check_token(/*...*/).await?;
  let stations = stations_service::get_all(db).await?;
  Ok(ApiResponse::new(stations))
}
```

**Security Evolution:**

| MVP | Security Features |
|-----|------------------|
| MVP-1 | No security (public API) |
| MVP-2+ | Rate limiting, basic validation |
| MVP-3+ | Authentication, JWT, RBAC |
| MVP-4+ | Advanced authorization, encryption |

---

### 2. API Abuse Prevention

**Rate limiting and abuse protection:**

```rust
// ❌ WRONG - No rate limiting
pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
  // ❌ No rate limiting
  stations_service::get_all(params).await
    .map_err(|e| map_error(e))
}

// ✅ CORRECT - Rate limiting (MVP-2+)
use std::collections::HashMap;
use std::time::Instant;

static mut RATE_LIMITS: HashMap<String, Vec<Instant>> = HashMap::new();

pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
  // ✅ Rate limiting (MVP-2+)
  let client_ip = "unknown"; // Would come from headers
  let now = Instant::now();
  let rate_limits = RATE_LIMITS.entry(client_ip).or_insert_with(Vec::new);

  rate_limits.retain(|t| now.duration_since(*t) < std::time::Duration::from_secs(60));

  if rate_limits.len() >= 100 {
    return Err(ApiError::RateLimited);
  }
  rate_limits.push(now);

  stations_service::get_all(params).await
    .map_err(|e| map_error(e))
}
```

**Rate Limiting Rules (MVP-2+):**
- Public endpoints: 100 req/min per IP
- Auth endpoints: 1000 req/min per user
- Admin endpoints: 500 req/min per user
- All limits enforceable with rate limiting

---

### 3. Input Sanitization Consistency

**All inputs must be sanitized:**

```rust
// ❌ WRONG - No input validation
pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
  // ❌ No validation
  stations_service::get_all(params).await
    .map_err(|e| map_error(e))
}

// ✅ CORRECT - Input validation
#[derive(Deserialize)]
struct GetStationsQuery {
    #[serde(default = "default_limit")]
    limit: u32,
    #[serde(default = "default_offset")]
    offset: u32,
}

fn default_limit() -> u32 { 20 }
fn default_offset() -> u32 { 0 }

pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
  // ✅ Validate limits
  if params.limit > 100 {
    return Err(ApiError::BadRequest("Limit too high".into()));
  }
  if params.limit < 1 {
    return Err(ApiError::BadRequest("Limit too low".into()));
  }

  stations_service::get_all(params).await
    .map_err(|e| map_error(e))
}
```

**Input Validation Rules:**

1. **Limits and ranges:**
   - Limit: 1-100
   - Offset: 0+
   - Radius: 100m-10km
   - Coordinates: -90-90 latitude, -180-180 longitude

2. **Data types:**
   - Strings: max length, format validation
   - Numbers: range validation
   - Enums: allowed values only

3. **Format validation:**
   - Email validation
   - Phone validation
   - URL validation (if needed)

---

### 4. No Leaking Internal Errors

**Never expose internal errors to users:**

```rust
// ❌ WRONG - Exposes internal info
pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
  let stations = stations_service::get_all(params).await
    .map_err(|e| {
      // ❌ Exposes internal error details
      eprintln!("Database error: {:?}", e);  // ❌ Don't log like this in production
      ApiError::InternalServerError("Something went wrong".into())
    })?;

  Ok(ApiResponse::new(stations))
}

// ✅ CORRECT - Hide internal errors
pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
  let stations = stations_service::get_all(params).await
    .map_err(|e| {
      // ✅ Don't expose internal details
      eprintln!("Database error: {:?}", e);  // Only in dev
      ApiError::InternalServerError("Something went wrong".into())
    })?;

  Ok(ApiResponse::new(stations))
}

// ✅ CORRECT - Proper error handling
pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
  stations_service::get_all(params).await
    .map_err(|e| match e {
      DomainError::DatabaseError(_) => ApiError::InternalServerError("Internal server error".into()),
      DomainError::StationNotFound(_) => ApiError::NotFound,
      e => ApiError::InternalServerError("Internal server error".into()),
    })
}
```

**Error Message Rules:**
- Internal errors: Generic messages
- Database errors: Generic messages
- Security errors: Clear messages
- User errors: Clear, actionable messages

---

### 5. Strict Logging Boundaries

**Logging only at appropriate layers:**

```rust
// ❌ WRONG - Too much logging
pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
  eprintln!("Handling get_stations request");
  eprintln!("Params: {:?}", params);
  eprintln!("Processing request...");

  let stations = stations_service::get_all(params).await
    .map_err(|e| {
      eprintln!("ERROR: {:?}", e);
      ApiError::InternalServerError("Something went wrong".into())
    })?;

  eprintln!("Returning {} stations", stations.len());
  Ok(ApiResponse::new(stations))
}

// ✅ CORRECT - Strategic logging
pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
  // ✅ Minimal logging
  let stations = stations_service::get_all(params).await
    .map_err(|e| {
      // ✅ Log only critical errors
      if cfg!(debug) {
        eprintln!("Database error: {:?}", e);
      }
      ApiError::InternalServerError("Internal server error".into())
    })?;

  Ok(ApiResponse::new(stations))
}
```

**Logging Rules:**
- Production: Critical errors only
- Debug: Detailed debugging info
- No sensitive data in logs
- No internal error messages

---

## 📋 Security Checklist

### Pre-Implementation

**Before ANY implementation:**

1. **MVP Context:**
   - [ ] Check if security is required for this MVP
   - [ ] Verify no premature security
   - [ ] Check MVP security requirements

2. **Input Validation:**
   - [ ] All inputs validated
   - [ ] Limits and ranges checked
   - [ ] Data types validated
   - [ ] Format validation implemented

3. **Error Handling:**
   - [ ] Internal errors hidden
   - [ ] User errors clear
   - [ ] Security errors informative
   - [ ] No sensitive data exposed

4. **Rate Limiting (MVP-2+):**
   - [ ] Rate limiting implemented
   - [ ] Limits appropriate
   - [ ] Rate limiting enforced
   - [ ] Abuse prevention in place

---

## 🚫 Security Anti-Patterns

### 1. Premature Security

```rust
// ❌ WRONG
pub async fn handle_get_stations(/*...*/) -> Result<ApiResponse<StationDto>, ApiError> {
  // ❌ No security in MVP-1
  // ❌ No rate limiting
  // ❌ No input validation
  stations_service::get_all(db).await?;
}
```

### 2. Insufficient Validation

```rust
// ❌ WRONG
pub async fn handle_update_station(
    Path(id): Path<String>,
    UpdateRequest { name }: UpdateRequest,
) -> Result<ApiResponse<()>, ApiError> {
  // ❌ No validation
  sqlx::query("UPDATE stations SET name = $1 WHERE id = $2")
    .bind(&name)
    .execute(&pool)
    .await?;
}
```

### 3. Leaking Internal Errors

```rust
// ❌ WRONG
pub async fn handle_delete_station(
    Path(id): Path<String>,
) -> Result<ApiResponse<()>, ApiError> {
  let _ = sqlx::query("DELETE FROM stations WHERE id = $1")
    .bind(&id)
    .execute(&pool)
    .await
    .map_err(|e| {
      // ❌ Exposes internal error details
      ApiError::InternalServerError(format!("Database error: {:?}", e))
    })?;
  Ok(ApiResponse::new(()))
}
```

### 4. Excessive Logging

```rust
// ❌ WRONG
pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
  eprintln!("Processing request...");
  eprintln!("Query params: {:?}", params);
  eprintln!("Fetching data...");
  eprintln!("Returning results...");
  // ❌ Too much logging
}
```

---

## 🎯 Security Evolution Checklist

### MVP-1 Security

- [ ] No security features
- [ ] Public API
- [ ] No rate limiting
- [ ] No input validation (basic)
- [ ] Generic error messages

### MVP-2 Security

- [ ] Rate limiting implemented
- [ ] Input validation comprehensive
- [ ] Rate limits appropriate
- [ ] Error handling secure

### MVP-3 Security

- [ ] Authentication implemented
- [ ] JWT tokens
- [ ] Authorization system
- [ ] RBAC
- [ ] Keycloak integration

### MVP-4+ Security

- [ ] Advanced security features
- [ ] Encryption
- [ ] Advanced authorization
- [ ] Security monitoring
- [ ] Regular security audits

---

*This skill enforces security patterns that evolve with MVP progression.*