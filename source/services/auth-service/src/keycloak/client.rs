use crate::error::AuthError;
use crate::models::auth::{LoginRequest, LogoutRequest, RefreshRequest, TokenResponse, LogoutResponse};
use crate::models::user::UserProfile;
use crate::validation::token::validate_required;
use claims::Claims;
use reqwest::Client;
use serde_json::json;

const KEYCLOAK_REALM: &str = "bornemap";
const KEYCLOAK_TOKEN_ENDPOINT: &str = "/realms/bornemap/protocol/openid-connect/token";

/// Keycloak HTTP client for token operations.
#[derive(Clone)]
pub struct KeycloakClient {
    http_client: Client,
    base_url: String,
    client_id: String,
}

impl KeycloakClient {
    /// Create a new Keycloak client.
    pub fn new(base_url: String, client_id: String) -> Self {
        Self {
            http_client: Client::new(),
            base_url,
            client_id,
        }
    }

    /// Authenticate with Keycloak using email and password.
    ///
    /// Returns the access token and refresh token.
    pub async fn login(&self, email: &str, password: &str) -> Result<TokenResponse, AuthError> {
        validate_required("email", email)?;
        validate_required("password", password)?;

        tracing::info!("Authenticating user: {}", email);

        let response = self
            .http_client
            .post(&format!("{}{}", self.base_url, KEYCLOAK_TOKEN_ENDPOINT))
            .form(&[
                ("grant_type", "password"),
                ("username", email),
                ("password", password),
                ("client_id", &self.client_id),
            ])
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Keycloak login error: {}", e);
                AuthError::AuthUnavailable
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(AuthError::InvalidCredentials);
        }

        let token_data: serde_json::Value = response
            .json()
            .await
            .map_err(|e| {
                tracing::error!("Failed to parse Keycloak response: {}", e);
                AuthError::AuthUnavailable
            })?;

        // Extract access and refresh tokens
        let access_token = token_data
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AuthError::ValidationError("access_token missing from response".to_string()))?;

        let refresh_token = token_data
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AuthError::ValidationError("refresh_token missing from response".to_string()))?;

        let expires_in = token_data
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(300);

        let refresh_expires_in = token_data
            .get("refresh_expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(1800);

        Ok(TokenResponse {
            access_token: access_token.to_string(),
            refresh_token: refresh_token.to_string(),
            expires_in,
            refresh_expires_in,
            token_type: "Bearer".to_string(),
        })
    }

    /// Refresh an access token using a refresh token.
    ///
    /// Returns new access and refresh tokens.
    pub async fn refresh(&self, refresh_token: &str) -> Result<TokenResponse, AuthError> {
        validate_required("refresh_token", refresh_token)?;

        tracing::info!("Refreshing token");

        let response = self
            .http_client
            .post(&format!("{}{}", self.base_url, KEYCLOAK_TOKEN_ENDPOINT))
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", &self.client_id),
                ("refresh_token", refresh_token),
            ])
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Keycloak refresh error: {}", e);
                AuthError::AuthUnavailable
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(AuthError::TokenExpired);
        }

        let token_data: serde_json::Value = response
            .json()
            .await
            .map_err(|e| {
                tracing::error!("Failed to parse Keycloak refresh response: {}", e);
                AuthError::AuthUnavailable
            })?;

        // Extract access and refresh tokens
        let access_token = token_data
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AuthError::ValidationError("access_token missing from response".to_string()))?;

        let refresh_token_new = token_data
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AuthError::ValidationError("refresh_token missing from response".to_string()))?;

        let expires_in = token_data
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(300);

        let refresh_expires_in = token_data
            .get("refresh_expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(1800);

        Ok(TokenResponse {
            access_token: access_token.to_string(),
            refresh_token: refresh_token_new.to_string(),
            expires_in,
            refresh_expires_in,
            token_type: "Bearer".to_string(),
        })
    }

    /// Logout a user by revoking their refresh token.
    ///
    /// This calls Keycloak's token revocation endpoint.
    pub async fn logout(&self, refresh_token: &str) -> Result<LogoutResponse, AuthError> {
        validate_required("refresh_token", refresh_token)?;

        tracing::info!("Logging out user");

        let response = self
            .http_client
            .post(&format!(
                "{}{}",
                self.base_url,
                "/realms/bornemap/protocol/openid-connect/logout"
            ))
            .form(&[
                ("client_id", &self.client_id),
                ("refresh_token", refresh_token),
            ])
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Keycloak logout error: {}", e);
                AuthError::AuthUnavailable
            })?;

        let status = response.status();
        if !status.is_success() {
            // Logout can still succeed even if refresh token is already expired
            tracing::warn!("Logout returned non-success status: {}", status);
        }

        Ok(LogoutResponse {
            message: "logged_out".to_string(),
        })
    }

    /// Extract claims from a token string.
    ///
    /// This decodes the token and extracts the payload.
    pub fn extract_claims(&self, token: &str) -> Result<Claims, AuthError> {
        let token_data = crate::validation::token::decode_token(token)?;
        Ok(token_data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, Header, EncodingKey};

    fn create_test_token(secret: &str) -> String {
        let header = Header::default();
        let payload = Claims {
            sub: "test_sub".to_string(),
            email: "test@example.com".to_string(),
            given_name: Some("Test".to_string()),
            family_name: Some("User".to_string()),
            realm_access: Some(RealmAccess {
                roles: vec!["role:admin".to_string()],
            }),
            aud: vec!["bornemap".to_string()],
        };

        encode(&header, &payload, &EncodingKey::from_secret(secret.as_bytes()))
            .unwrap()
    }

    #[test]
    fn test_login_validation_error() {
        let client = KeycloakClient::new("http://localhost:8080".to_string());
        let result = client.login("", "password").await;
        assert!(matches!(result, Err(AuthError::ValidationError(_))));
    }

    #[test]
    fn test_refresh_validation_error() {
        let client = KeycloakClient::new("http://localhost:8080".to_string());
        let result = client.refresh("").await;
        assert!(matches!(result, Err(AuthError::ValidationError(_))));
    }

    #[test]
    fn test_logout_validation_error() {
        let client = KeycloakClient::new("http://localhost:8080".to_string());
        let result = client.logout("").await;
        assert!(matches!(result, Err(AuthError::ValidationError(_))));
    }
}
