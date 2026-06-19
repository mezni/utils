use serde::{Deserialize, Serialize};

/// Login request with email and password.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

/// Refresh request with a refresh token.
#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

/// Logout request with a refresh token.
#[derive(Debug, Deserialize)]
pub struct LogoutRequest {
    pub refresh_token: String,
}

/// Logout response.
#[derive(Debug, Serialize)]
pub struct LogoutResponse {
    pub message: String,
}

/// Token response containing access and refresh tokens.
#[derive(Debug, Serialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: u64,
    pub refresh_expires_in: u64,
    pub token_type: String,
}

/// Standard error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Vec<ErrorDetail>>,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    pub field: String,
    pub message: String,
}
