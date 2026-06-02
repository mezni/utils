use crate::errors::AuthError;
use crate::CurrentUser;
use axum::{
    extract::Request,
    http::HeaderValue,
    middleware::Next,
    response::Response,
};
use common_types::Role;

// ---------------------------------------------------------------------------
// Global configuration (set once at startup)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AuthConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
}

static AUTH_CONFIG: once_cell::sync::OnceCell<AuthConfig> = once_cell::sync::OnceCell::new();

pub fn set_auth_config(config: AuthConfig) {
    AUTH_CONFIG.set(config).ok();
}

// ---------------------------------------------------------------------------
// Auth middleware layer
// ---------------------------------------------------------------------------

pub async fn auth_middleware(
    mut req: Request,
    next: Next,
) -> Result<Response, AuthError> {
    let config = AUTH_CONFIG
        .get()
        .ok_or_else(|| AuthError::ValidationError("Auth not configured".into()))?;

    let token = extract_bearer_token(req.headers().get("authorization"))
        .ok_or(AuthError::Unauthenticated)?;

    let claims = crate::jwt::validate_token(token, &config.issuer, &config.audience).await?;

    let role_str = claims
        .role()
        .ok_or_else(|| AuthError::InsufficientRole)?;
    let role = match role_str {
        "registered_driver" => Role::RegisteredDriver,
        "partner" => Role::Partner,
        "admin" => Role::Admin,
        _ => return Err(AuthError::InsufficientRole),
    };

    let provisioned = crate::provisioning::provision_user(
        &claims.sub,
        claims.email.as_deref(),
        role,
    )
    .await;

    let current_user: CurrentUser = provisioned.into();
    req.extensions_mut().insert(current_user);

    Ok(next.run(req).await)
}

fn extract_bearer_token(header: Option<&HeaderValue>) -> Option<&str> {
    let header = header?.to_str().ok()?;
    header.strip_prefix("Bearer ")
}

// ---------------------------------------------------------------------------
// Auth guard helpers
// ---------------------------------------------------------------------------

/// Middleware that rejects requests with unauthenticated users.
pub async fn require_authenticated(
    req: Request,
    next: Next,
) -> Result<Response, AuthError> {
    if req.extensions().get::<CurrentUser>().is_none() {
        return Err(AuthError::Unauthenticated);
    }
    Ok(next.run(req).await)
}

/// Helper: access current user from request in a handler.
/// Handler must have `req: Request` as the last parameter.
pub fn extract_current_user(req: &Request) -> Result<CurrentUser, AuthError> {
    req.extensions()
        .get::<CurrentUser>()
        .cloned()
        .ok_or(AuthError::Unauthenticated)
}

fn role_sufficient(actual: &Role, required: &Role) -> bool {
    let rank = |r: &Role| -> u8 {
        match r {
            Role::RegisteredDriver => 1,
            Role::Partner => 2,
            Role::Admin => 3,
        }
    };
    rank(actual) >= rank(required)
}
