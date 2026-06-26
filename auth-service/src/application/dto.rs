use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterInput {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginInput {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshTokenInput {
    pub refresh_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub access_token_expires_in: i64,
    pub refresh_token_expires_in: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub user_id: Uuid,
    pub email: String,
    pub email_verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogoutResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaimsResponse {
    pub sub: String,
    pub email: String,
    pub user_id: String,
    pub status: String,
    pub email_verified: bool,
    pub permissions: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_input_serialization() {
        let input = RegisterInput {
            email: "test@example.com".to_string(),
            password: "SecurePassword123!".to_string(),
        };

        let serialized = serde_json::to_string(&input).unwrap();
        let deserialized: RegisterInput = serde_json::from_str(&serialized).unwrap();

        assert_eq!(input.email, deserialized.email);
        assert_eq!(input.password, deserialized.password);
    }

    #[test]
    fn test_login_input_serialization() {
        let input = LoginInput {
            email: "test@example.com".to_string(),
            password: "SecurePassword123!".to_string(),
        };

        let serialized = serde_json::to_string(&input).unwrap();
        let deserialized: LoginInput = serde_json::from_str(&serialized).unwrap();

        assert_eq!(input.email, deserialized.email);
        assert_eq!(input.password, deserialized.password);
    }

    #[test]
    fn test_token_response_serialization() {
        let response = TokenResponse {
            access_token: "test_token".to_string(),
            refresh_token: "test_refresh_token".to_string(),
            access_token_expires_in: 300,
            refresh_token_expires_in: 2592000,
        };

        let serialized = serde_json::to_string(&response).unwrap();
        let deserialized: TokenResponse = serde_json::from_str(&serialized).unwrap();

        assert_eq!(response.access_token, deserialized.access_token);
        assert_eq!(response.refresh_token, deserialized.refresh_token);
        assert_eq!(response.access_token_expires_in, deserialized.access_token_expires_in);
        assert_eq!(response.refresh_token_expires_in, deserialized.refresh_token_expires_in);
    }
}