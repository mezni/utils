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
// Token extraction + CurrentUser construction (shared by all auth layers)
// ---------------------------------------------------------------------------

fn extract_bearer_token(header: Option<&HeaderValue>) -> Option<String> {
    let header = header?.to_str().ok()?;
    header.strip_prefix("Bearer ").map(|s| s.to_string())
}

/// Validate an optional bearer token and provision/populate a `CurrentUser`.
/// Returns `Ok(None)` when no `Authorization` header is present.
/// Returns `Err` only when a token is present but invalid/expired.
async fn try_authenticate(
    token: Option<String>,
) -> Result<Option<CurrentUser>, AuthError> {
    let config = AUTH_CONFIG
        .get()
        .ok_or_else(|| AuthError::ValidationError("Auth not configured".into()))?;

    let token = match token {
        Some(t) => t,
        None => return Ok(None),
    };

    let claims = crate::jwt::validate_token(&token, &config.issuer, &config.audience).await?;

    let role = claims.role().ok_or(AuthError::InsufficientRole)?;

    let provisioned =
        crate::provisioning::provision_user(&claims.sub, claims.email.as_deref(), role).await;

    Ok(Some(provisioned.into()))
}

// ---------------------------------------------------------------------------
// Auth layers
// ---------------------------------------------------------------------------

/// Mandatory authentication layer.
///
/// Rejects requests without a valid bearer token, then populates `CurrentUser`
/// in the request extensions. Apply this to routers whose routes all require a
/// signed-in user. `/health` and other public routes MUST NOT be placed behind
/// this layer (mount them on a separate, unauthenticated router).
pub async fn auth_middleware(mut req: Request, next: Next) -> Result<Response, AuthError> {
    let token = extract_bearer_token(req.headers().get("authorization"));
    let user = try_authenticate(token).await?.ok_or(AuthError::Unauthenticated)?;
    req.extensions_mut().insert(user);
    Ok(next.run(req).await)
}

/// Optional authentication layer.
///
/// Populates `CurrentUser` when a valid token is present, but allows anonymous
/// requests through with no `CurrentUser` extension. A present-but-invalid token
/// is still rejected (so callers can't pass garbage credentials). Used for
/// public/anonymous-capable endpoints such as clickstream ingestion.
pub async fn optional_auth_middleware(
    mut req: Request,
    next: Next,
) -> Result<Response, AuthError> {
    let token = extract_bearer_token(req.headers().get("authorization"));
    if let Some(user) = try_authenticate(token).await? {
        req.extensions_mut().insert(user);
    }
    Ok(next.run(req).await)
}

// ---------------------------------------------------------------------------
// Guard helpers
// ---------------------------------------------------------------------------

/// Middleware that rejects requests without an authenticated `CurrentUser`.
/// Use downstream of [`optional_auth_middleware`] when a specific route needs auth.
pub async fn require_authenticated(req: Request, next: Next) -> Result<Response, AuthError> {
    if req.extensions().get::<CurrentUser>().is_none() {
        return Err(AuthError::Unauthenticated);
    }
    Ok(next.run(req).await)
}

/// Build a role-gating middleware closure for the given required role.
///
/// The current user must already be populated by [`auth_middleware`]; the user's
/// role must satisfy `required` per the hierarchy `admin >= partner >= registered_driver`.
///
/// ```ignore
/// Router::new()
///     .route("/api/v1/admin/check", get(handler))
///     .layer(axum::middleware::from_fn(require_role(Role::Admin)))
///     .layer(axum::middleware::from_fn(auth_middleware));
/// ```
pub fn require_role(
    required: Role,
) -> impl Fn(
    Request,
    Next,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Response, AuthError>> + Send>,
> + Clone {
    move |req: Request, next: Next| {
        Box::pin(async move {
            let satisfied = req
                .extensions()
                .get::<CurrentUser>()
                .map(|u| u.role.satisfies(required))
                .ok_or(AuthError::Unauthenticated)?;
            if satisfied {
                Ok(next.run(req).await)
            } else {
                Err(AuthError::InsufficientRole)
            }
        })
    }
}

/// Helper: access current user from request in a handler.
pub fn extract_current_user(req: &Request) -> Result<CurrentUser, AuthError> {
    req.extensions()
        .get::<CurrentUser>()
        .cloned()
        .ok_or(AuthError::Unauthenticated)
}
