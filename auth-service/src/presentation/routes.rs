use axum::{
    extract::{State, Path, Request},
    http::{header, HeaderMap},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

use shared_errors::{handle_error, AuthServiceError};
use shared_contracts::{ErrorResponse, AuthError};

use crate::application::use_cases::{RegisterUserUseCase, LoginUserUseCase, RefreshTokenUseCase, LogoutUseCase};
use crate::infrastructure::{DatabaseInfrastructure, CacheInfrastructure, JwtInfrastructure};
use crate::domain::services::{PasswordService, TokenPolicyService};

pub struct AppState {
    pub database: DatabaseInfrastructure,
    pub cache: CacheInfrastructure,
    pub jwt: JwtInfrastructure,
    pub register_use_case: RegisterUserUseCase,
    pub login_use_case: LoginUserUseCase,
    pub refresh_use_case: RefreshTokenUseCase,
    pub logout_use_case: LogoutUseCase,
}

pub async fn root() -> &'static str {
    "Auth Service - Running"
}

pub fn create_router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(root))
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        .route("/auth/refresh", post(refresh))
        .route("/auth/logout", post(logout))
        .route("/.well-known/jwks.json", get(get_jwks))
        .route("/.well-known/openid-configuration", get(get_openid_config))
        .with_state(state)
}

pub async fn register(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::extract::DefaultJson<shared_contracts::RegisterInput>,
) -> Result<shared_contracts::RegisterResponse, ApiResponse> {
    let client_ip = headers
        .get(header::FORWARDED)
        .and_then(|h| h.to_str().ok())
        .or_else(|| headers.get(header::X_FORWARDED_FOR).and_then(|h| h.to_str().ok()))
        .or_else(|| headers.get(header::X_REAL_IP).and_then(|h| h.to_str().ok()))
        .or_else(|| {
            headers
                .get(header::REMOTE_ADDR)
                .and_then(|h| h.to_str().ok())
        })
        .map(|s| s.to_string())
        .or_else(|| "unknown".to_string());

    let user_agent = headers
        .get(header::USER_AGENT)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let input = body.0;

    let response = state.register_use_case
        .execute(
            input.email,
            input.password,
            Some(client_ip),
            user_agent,
        )
        .await
        .map_err(|e| map_error(e))?;

    Ok(response)
}

pub async fn login(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::extract::DefaultJson<shared_contracts::LoginInput>,
) -> Result<shared_contracts::TokenResponse, ApiResponse> {
    let client_ip = headers
        .get(header::FORWARDED)
        .and_then(|h| h.to_str().ok())
        .or_else(|| headers.get(header::X_FORWARDED_FOR).and_then(|h| h.to_str().ok()))
        .or_else(|| headers.get(header::X_REAL_IP).and_then(|h| h.to_str().ok()))
        .or_else(|| {
            headers
                .get(header::REMOTE_ADDR)
                .and_then(|h| h.to_str().ok())
        })
        .map(|s| s.to_string())
        .or_else(|| "unknown".to_string());

    let user_agent = headers
        .get(header::USER_AGENT)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let input = body.0;

    let response = state.login_use_case
        .execute(
            input.email,
            input.password,
            Some(client_ip),
            user_agent,
        )
        .await
        .map_err(|e| map_error(e))?;

    Ok(response)
}

pub async fn refresh(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::extract::DefaultJson<shared_contracts::RefreshTokenInput>,
) -> Result<shared_contracts::TokenResponse, ApiResponse> {
    let client_ip = headers
        .get(header::FORWARDED)
        .and_then(|h| h.to_str().ok())
        .or_else(|| headers.get(header::X_FORWARDED_FOR).and_then(|h| h.to_str().ok()))
        .or_else(|| headers.get(header::X_REAL_IP).and_then(|h| h.to_str().ok()))
        .or_else(|| {
            headers
                .get(header::REMOTE_ADDR)
                .and_then(|h| h.to_str().ok())
        })
        .map(|s| s.to_string())
        .or_else(|| "unknown".to_string());

    let user_agent = headers
        .get(header::USER_AGENT)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let input = body.0;

    let response = state.refresh_use_case
        .execute(input.refresh_token, Some(client_ip), user_agent)
        .await
        .map_err(|e| map_error(e))?;

    Ok(response)
}

pub async fn logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<shared_contracts::LogoutResponse, ApiResponse> {
    let client_ip = headers
        .get(header::FORWARDED)
        .and_then(|h| h.to_str().ok())
        .or_else(|| headers.get(header::X_FORWARDED_FOR).and_then(|h| h.to_str().ok()))
        .or_else(|| headers.get(header::X_REAL_IP).and_then(|h| h.to_str().ok()))
        .or_else(|| {
            headers
                .get(header::REMOTE_ADDR)
                .and_then(|h| h.to_str().ok())
        })
        .map(|s| s.to_string())
        .or_else(|| "unknown".to_string());

    let user_agent = headers
        .get(header::USER_AGENT)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    // Extract user ID from JWT (this would be done in middleware)
    let user_id = uuid::Uuid::new_v4(); // Placeholder - should be extracted from JWT

    let response = state.logout_use_case
        .execute(user_id, Some(client_ip), user_agent)
        .await
        .map_err(|e| map_error(e))?;

    Ok(response)
}

pub async fn get_jwks() -> Result<serde_json::Value, ApiResponse> {
    let jwt_service = JwtInfrastructure::new("dummy-secret-for-test-only").unwrap();
    let jwks = jwt_service.generate_jwks().map_err(|e| {
        error!("Failed to generate JWKS: {}", e);
        ApiResponse::error(AuthError::new("INTERNAL_ERROR", "Failed to generate JWKS".to_string()))
    })?;

    Ok(jwks)
}

pub async fn get_openid_config() -> Result<serde_json::Value, ApiResponse> {
    let jwt_service = JwtInfrastructure::new("dummy-secret-for-test-only").unwrap();
    let config = jwt_service.generate_openid_config().map_err(|e| {
        error!("Failed to generate OpenID config: {}", e);
        ApiResponse::error(AuthError::new("INTERNAL_ERROR", "Failed to generate OpenID config".to_string()))
    })?;

    Ok(config)
}

pub async fn error_handler(
    error: axum::http::StatusCode,
    message: String,
) -> impl IntoResponse {
    (error, ApiResponse::error(AuthError::new("API_ERROR", message)))
}

pub struct ApiResponse {
    success: bool,
    data: Option<serde_json::Value>,
    error: Option<AuthError>,
}

impl ApiResponse {
    pub fn success(data: serde_json::Value) -> Self {
        ApiResponse {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(error: AuthError) -> Self {
        ApiResponse {
            success: false,
            data: None,
            error: Some(error),
        }
    }

    pub fn into_response(self) -> Response {
        let (status, error_response) = match self.error {
            Some(error) => {
                let (auth_error, _details) = handle_error(error);
                let error_body = ErrorResponse::auth_error(auth_error);
                (axum::http::StatusCode::BAD_REQUEST, error_body)
            }
            None => (axum::http::StatusCode::OK, self.data.unwrap()),
        };

        (status, error_response).into_response()
    }
}

impl IntoResponse for ApiResponse {
    fn into_response(self) -> Response {
        self.into_response()
    }
}

fn map_error(error: AuthServiceError) -> ApiResponse {
    let (auth_error, _details) = handle_error(error);
    ApiResponse::error(auth_error)
}