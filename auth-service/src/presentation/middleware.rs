use axum::{
    extract::Request,
    http::{header, StatusCode},
    middleware::Next,
    response::Response,
};
use tracing::{error, info, warn};

use shared_jwt::JwtService;
use shared_contracts::{UserWithoutSensitive, JwtClaims};

#[derive(Clone)]
pub struct AuthMiddleware {
    jwt_service: JwtService,
}

impl AuthMiddleware {
    pub fn new(jwt_service: JwtService) -> Self {
        AuthMiddleware { jwt_service }
    }

    pub async fn verify_token(
        &self,
        request: Request,
    ) -> Result<(UserWithoutSensitive, String), MiddlewareError> {
        // Extract Authorization header
        let auth_header = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .ok_or(MiddlewareError::MissingAuthorizationHeader)?;

        // Validate format: "Bearer <token>"
        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or(MiddlewareError::InvalidTokenFormat)?;

        // Verify and decode JWT
        let claims = self.jwt_service
            .validate(token)
            .map_err(|e| {
                error!("JWT validation failed: {}", e);
                MiddlewareError::InvalidToken(e.to_string())
            })?;

        // Validate required claims
        if claims.sub.is_empty() {
            return Err(MiddlewareError::MissingClaim("sub".to_string()));
        }

        if claims.email.is_empty() {
            return Err(MiddlewareError::MissingClaim("email".to_string()));
        }

        // Create UserWithoutSensitive from claims
        let user = UserWithoutSensitive {
            id: uuid::Uuid::parse_str(&claims.sub).map_err(|e| {
                error!("Invalid user ID in claims: {}", e);
                MiddlewareError::InvalidClaim("sub".to_string())
            })?,
            email: claims.email,
            email_verified: claims.email_verified,
            status: claims.status,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        info!("Token validated successfully for user: {}", claims.email);

        Ok((user, claims.email))
    }
}

#[derive(Debug)]
pub enum MiddlewareError {
    MissingAuthorizationHeader,
    InvalidTokenFormat,
    InvalidToken(String),
    InvalidClaim(String),
    MissingClaim(String),
}

impl IntoResponse for MiddlewareError {
    fn into_response(self) -> Response {
        let status = match self {
            MiddlewareError::MissingAuthorizationHeader => StatusCode::UNAUTHORIZED,
            MiddlewareError::InvalidTokenFormat => StatusCode::BAD_REQUEST,
            MiddlewareError::InvalidToken(_) => StatusCode::UNAUTHORIZED,
            MiddlewareError::InvalidClaim(_) => StatusCode::BAD_REQUEST,
            MiddlewareError::MissingClaim(_) => StatusCode::UNAUTHORIZED,
        };

        (status, axum::Json(serde_json::json!({
            "success": false,
            "error": {
                "code": "AUTH_ERROR",
                "message": self.to_string()
            }
        }))).into_response()
    }
}

impl std::fmt::Display for MiddlewareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MiddlewareError::MissingAuthorizationHeader => write!(f, "Missing authorization header"),
            MiddlewareError::InvalidTokenFormat => write!(f, "Invalid token format"),
            MiddlewareError::InvalidToken(msg) => write!(f, "Invalid token: {}", msg),
            MiddlewareError::InvalidClaim(field) => write!(f, "Missing required claim: {}", field),
            MiddlewareError::MissingClaim(field) => write!(f, "Missing required claim: {}", field),
        }
    }
}

pub async fn auth_middleware(
    State(middleware): State<AuthMiddleware>,
    request: Request,
    next: Next,
) -> Result<Response, MiddlewareError> {
    middleware.verify_token(request).await?;
    Ok(next.run(request).await)
}