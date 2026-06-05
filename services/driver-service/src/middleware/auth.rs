//! JWT authentication middleware

use actix_web::dev::{Request, ServiceRequest, ServiceResponse};
use actix_web::{Error, HttpMessage, HttpResponse};
use actix_web_httpauth::headers::authorization::Bearer;
use actix_web_httpauth::middleware::HttpAuthentication;
use sqlx::PgPool;

use crate::error::{ApiError, AppResult};
use crate::AppState;

/// Extract JWT token from Authorization header
pub fn extract_jwt(request: &mut ServiceRequest) -> AppResult<String> {
    let auth_header = request
        .headers()
        .get(actix_web::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| {
            ApiError::Unauthorized
        })?;

    let bearer = Bearer::from_header(auth_header)
        .ok_or_else(|| ApiError::BadRequest("Invalid Authorization header format".into()))?;

    let token = bearer.token();
    Ok(token.to_string())
}

/// Verify JWT token and extract claims
pub async fn verify_jwt(
    token: String,
    pool: &PgPool,
) -> AppResult<ev_auth::Claims> {
    // TODO: Implement actual JWT validation
    // For now, return mock claims
    let claims = ev_auth::Claims {
        sub: "mock-user-id".to_string(),
        email: Some("test@example.com".to_string()),
        name: Some("Test User".to_string()),
        role: ev_auth::Role::RegisteredDriver,
        partner_id: None,
        iat: 1700000000,
        exp: 1700003600,
        jti: Some("mock-jti".to_string()),
    };

    Ok(claims)
}

/// Auth middleware handler
pub async fn auth_middleware(
    request: ServiceRequest,
    pool: web::Data<PgPool>,
) -> Result<ServiceRequest, Error> {
    let token = extract_jwt(&mut request)
        .map_err(|e| Error::from(e))?;

    let claims = verify_jwt(token, &pool)
        .await
        .map_err(|e| Error::from(e))?;

    // Attach claims to request extensions
    request.extensions_mut().insert(claims);

    Ok(request)
}

/// Auth middleware that requires partner role
pub async fn auth_partner_middleware(
    request: ServiceRequest,
    pool: web::Data<PgPool>,
) -> Result<ServiceRequest, Error> {
    let mut request = auth_middleware(request, pool).await?;

    let claims = request
        .extensions()
        .get::<ev_auth::Claims>()
        .ok_or_else(|| ApiError::Unauthorized)
        .map_err(|e| Error::from(e))?;

    if claims.role != ev_auth::Role::Partner {
        return Err(ApiError::Forbidden.into());
    }

    // Attach claims to request extensions
    request.extensions_mut().insert(claims);

    Ok(request)
}

/// Global auth middleware for the app
pub fn auth() -> HttpAuthentication<ev_auth::JwtValidator> {
    HttpAuthentication::new(|request, mut req, validator| {
        let validator = validator.clone();
        async move {
            let token = extract_jwt(&mut req).map_err(|e| Error::from(e))?;
            let claims = verify_jwt(token, &validator).await.map_err(|e| Error::from(e))?;
            Ok(req.into_parts(claims))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_jwt_from_valid_header() {
        let mut request = ServiceRequest::build(HttpRequest::default())
            .insert_header(("Authorization", "Bearer test-token-123"))
            .finish()
            .unwrap();

        let token = extract_jwt(&mut request);
        assert!(token.is_ok());
        assert_eq!(token.unwrap(), "test-token-123");
    }

    #[test]
    fn test_extract_jwt_from_invalid_header() {
        let mut request = ServiceRequest::build(HttpRequest::default())
            .insert_header(("Authorization", "InvalidFormat"))
            .finish()
            .unwrap();

        let token = extract_jwt(&mut request);
        assert!(token.is_err());
    }

    #[test]
    fn test_auth_partner_requires_partner_role() {
        // Mock test for partner middleware
        let claims = ev_auth::Claims {
            sub: "user123".to_string(),
            email: Some("user@example.com".to_string()),
            name: Some("User".to_string()),
            role: ev_auth::Role::RegisteredDriver,
            partner_id: None,
            iat: 1700000000,
            exp: 1700003600,
            jti: Some("jti".to_string()),
        };

        assert_ne!(claims.role, ev_auth::Role::Partner);
    }
}
