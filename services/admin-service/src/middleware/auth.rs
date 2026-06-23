//! Keycloak authentication middleware
//! Validates JWT tokens from Keycloak and sets user information

use actix_web::{dev::{RequestHead, FromRequest}, http::header::AUTHORIZATION, Error, HttpResponse};;
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::Deserialize;
use std::fmt;
use std::sync::Arc;

/// Keycloak configuration
#[derive(Clone)]
pub struct KeycloakConfig {
    pub auth_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub realm: String,
}

impl Default for KeycloakConfig {
    fn default() -> Self {
        Self {
            auth_url: "http://localhost:8080".to_string(),
            client_id: "bornemap".to_string(),
            client_secret: "".to_string(),
            realm: "bornemap".to_string(),
        }
    }
}

/// Keycloak JWKS endpoint
const JWKS_URL: &str = "https://keycloak-borneemap/api/realms/{realm}/protocol/openid-connect/certs";

/// User claims from Keycloak
#[derive(Debug, Deserialize, Clone)]
pub struct UserClaims {
    pub sub: String,           // User UUID (subject)
    pub preferred_username: String,
    pub email: Option<String>,
    pub role: Option<String>,  // admin or manager
    pub iss: String,
    pub aud: String,
    pub exp: usize,
    pub iat: usize,
}

impl fmt::Display for UserClaims {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UserClaims {{ sub: {}, username: {}, email: {:?}, role: {:?} }}",
            self.sub, self.preferred_username, self.email, self.role
        )
    }
}

/// Authenticated user
#[derive(Clone)]
pub struct AuthUser {
    pub user_uuid: String,
    pub username: String,
    pub email: Option<String>,
    pub role: String,
}

impl AuthUser {
    pub fn is_admin(&self) -> bool {
        self.role.to_lowercase() == "admin"
    }

    pub fn is_manager(&self) -> bool {
        self.role.to_lowercase() == "manager"
    }
}

/// Keycloak authentication middleware
pub struct KeycloakAuth;

impl KeycloakAuth {
    /// Validate JWT token
    pub fn validate_token(token: &str, config: &KeycloakConfig) -> Result<UserClaims, AuthError> {
        // Decode the token
        let token_data = decode::<UserClaims>(
            token,
            &DecodingKey::from_secret(config.client_secret.as_bytes()),
            &Validation::default(),
        )?;

        // Verify claims
        let claims = token_data.claims;

        // Check expiration
        if claims.exp < std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
        {
            return Err(AuthError::TokenExpired);
        }

        Ok(claims)
    }
}

impl FromRequest for AuthUser {
    type Error = Error;
    type Future = std::future::Ready<Result<Self, Self::Error>>;

    fn from_request(req: &mut RequestHead, _payload: &mut actix_web::dev::Payload) -> Self::Future {
        // Extract token from Authorization header
        let auth_header = req
            .headers()
            .get(AUTHORIZATION)
            .and_then(|header| header.to_str().ok())
            .ok_or_else(|| {
                actix_web::error::ErrorUnauthorized("Missing authorization header")
            })
            .unwrap();

        // Parse token
        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or_else(|| actix_web::error::ErrorUnauthorized("Invalid token format"))
            .unwrap()
            .to_string();

        // Validate token
        let config = KeycloakConfig::default();
        let claims = match KeycloakAuth::validate_token(&token, &config) {
            Ok(claims) => claims,
            Err(_) => return std::future::ready(Err(actix_web::error::ErrorUnauthorized("Invalid token"))),
        };

        // Create auth user
        let user = AuthUser {
            user_uuid: claims.sub,
            username: claims.preferred_username,
            email: claims.email,
            role: claims.role.unwrap_or_else(|| "user".to_string()),
        };

        std::future::ready(Ok(user))
    }
}

/// Authentication errors
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Token expired")]
    TokenExpired,
    #[error("Invalid token")]
    InvalidToken,
    #[error("Missing authorization header")]
    MissingHeader,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_user_is_admin() {
        let admin = AuthUser {
            user_uuid: "123".to_string(),
            username: "admin".to_string(),
            email: Some("admin@test.com".to_string()),
            role: "admin".to_string(),
        };

        assert!(admin.is_admin());
        assert!(!admin.is_manager());
    }

    #[test]
    fn test_auth_user_is_manager() {
        let manager = AuthUser {
            user_uuid: "456".to_string(),
            username: "manager".to_string(),
            email: Some("manager@test.com".to_string()),
            role: "manager".to_string(),
        };

        assert!(!manager.is_admin());
        assert!(manager.is_manager());
    }
}