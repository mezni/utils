use axum::{
    extract::{FromRequestParts, Request},
    http::{request::Parts, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use common_auth::{AuthErrorResponse, AuthMiddleware, JwtValidator};
use std::sync::Arc;

struct AppState {
    auth: AuthMiddleware,
}

#[derive(Clone)]
struct AuthCtx(Option<common_auth::UserContext>);

impl<S: Sync + Send> FromRequestParts<S> for AuthCtx {
    type Rejection = (StatusCode, Json<AuthErrorResponse>);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let user = parts.extensions.get::<common_auth::UserContext>().cloned();
        Ok(AuthCtx(user))
    }
}

async fn auth_mw(
    state: Arc<AppState>,
    request: Request,
    next: Next,
) -> Result<impl IntoResponse, (StatusCode, Json<AuthErrorResponse>)> {
    let path = request.uri().path().to_string();
    let auth_header = request
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok());

    match state.auth.authenticate_request(&path, auth_header).await {
        Ok(ctx) => {
            let mut req = request;
            if let Some(user) = ctx {
                req.extensions_mut().insert(user);
            }
            Ok(next.run(req).await)
        }
        Err(err) => {
            let code = if err.error_code == "UNAUTHORIZED" || err.error_code == "TOKEN_EXPIRED" {
                StatusCode::UNAUTHORIZED
            } else if err.error_code == "FORBIDDEN" {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            Err((code, Json(err)))
        }
    }
}

async fn health() -> &'static str {
    "OK"
}

async fn events_handler(auth: AuthCtx) -> impl IntoResponse {
    match auth.0 {
        Some(ref user) => {
            Json(serde_json::json!({ "events": [], "user_id": user.user_id, "roles": user.roles }))
        }
        None => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error_code": "UNAUTHORIZED",
                "message": "Authentication required"
            })),
        ),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::init();

    let validator = JwtValidator::new(
        "http://keycloak:8080/realms/ev-platform/protocol/openid-connect/certs".into(),
        "https://keycloak:8080/realms/ev-platform".into(),
        "backend-service".into(),
    );

    if let Err(e) = validator.refresh_jwks().await {
        tracing::warn!("Initial JWKS fetch failed (will retry on first request): {e}");
    }

    let state = Arc::new(AppState {
        auth: AuthMiddleware::new(validator),
    });

    let app = Router::new()
        .route("/api/v1/events", get(events_handler))
        .route("/health", get(health))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_mw))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    println!("clickstream-service — listening at /api/v1/events/*");
    axum::serve(listener, app).await.unwrap();
}
