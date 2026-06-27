use crate::TokenClaims;
use bornemap_core::AppError;
use chrono::{DateTime, Utc};
use jsonwebtoken::{DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtConfig {
    pub algorithm: String,
    pub issuer: String,
    pub audience: String,
    pub clock_skew: Duration,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            algorithm: "HS256".to_string(),
            issuer: "bornemap".to_string(),
            audience: "bornemap-app".to_string(),
            clock_skew: Duration::from_secs(30),
        }
    }
}

#[derive(Clone)]
pub struct JwtValidator {
    config: JwtConfig,
    decoding_key: DecodingKey,
    secret: String,
}

impl JwtValidator {
    pub fn new(secret: String, config: JwtConfig) -> Result<Self, AppError> {
        if secret.is_empty() {
            return Err(AppError::InvalidConfiguration("JWT secret cannot be empty".to_string()));
        }

        let decoding_key = DecodingKey::from_secret(secret.as_bytes());

        Ok(Self { 
            config, 
            decoding_key,
            secret,
        })
    }

    pub fn validate(&self, token: &str) -> Result<ValidatedClaims, AppError> {
        let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[&self.config.audience]);
        validation.leeway = self.config.clock_skew.as_secs();

        let token_data = decode::<TokenClaims>(token, &self.decoding_key, &validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AppError::TokenExpired,
                jsonwebtoken::errors::ErrorKind::InvalidToken => AppError::InvalidToken,
                jsonwebtoken::errors::ErrorKind::InvalidSignature => AppError::InvalidSignature,
                jsonwebtoken::errors::ErrorKind::InvalidIssuer => AppError::InvalidIssuer,
                jsonwebtoken::errors::ErrorKind::InvalidAudience => AppError::InvalidAudience,
                _ => AppError::InvalidToken,
            })?;

        let claims = token_data.claims;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AppError::InternalError)?;

        let issued_at = UNIX_EPOCH + Duration::from_secs(claims.iat as u64);
        let expires_at = UNIX_EPOCH + Duration::from_secs(claims.exp as u64);

        Ok(ValidatedClaims {
            sub: claims.sub,
            role: claims.role,
            iat: DateTime::from(issued_at),
            exp: DateTime::from(expires_at),
            iss: claims.iss,
            aud: claims.aud,
            jti: claims.jti,
        })
    }

    pub fn config(&self) -> &JwtConfig {
        &self.config
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedClaims {
    pub sub: String,
    pub role: String,
    pub iat: DateTime<Utc>,
    pub exp: DateTime<Utc>,
    pub iss: String,
    pub aud: String,
    pub jti: String,
}

impl ValidatedClaims {
    pub fn is_expired(&self) -> bool {
        self.exp < Utc::now()
    }

    pub fn is_valid(&self) -> bool {
        !self.is_expired()
    }

    pub fn user_id(&self) -> Result<uuid::Uuid, AppError> {
        uuid::Uuid::parse_str(&self.sub)
            .map_err(|_| AppError::InvalidToken)
    }

    pub fn role(&self) -> &str {
        &self.role
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_validator() -> JwtValidator {
        let config = JwtConfig::default();
        JwtValidator::new("test-secret-key".to_string(), config)
            .expect("Failed to create validator")
    }

    #[test]
    fn validator_creation_with_empty_secret_fails() {
        let config = JwtConfig::default();
        let result = JwtValidator::new("".to_string(), config);
        assert!(matches!(result, Err(AppError::InvalidConfiguration(_))));
    }

    #[test]
    fn validator_creation_with_valid_secret_succeeds() {
        let config = JwtConfig::default();
        let result = JwtValidator::new("test-secret-key".to_string(), config);
        assert!(result.is_ok());
    }

    #[test]
    fn expired_token_returns_expired_error() {
        let validator = create_validator();
        let expired_claims = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: 1000,
            exp: 1000,
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &expired_claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator.validate(&token);
        assert!(matches!(result, Err(AppError::TokenExpired)));
    }

    #[test]
    fn invalid_signature_returns_signature_error() {
        let validator = create_validator();
        let claims = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("wrong-secret".as_bytes()),
        ).unwrap();

        let result = validator.validate(&token);
        assert!(matches!(result, Err(AppError::InvalidSignature)));
    }

    #[test]
    fn invalid_issuer_returns_issuer_error() {
        let validator = create_validator();
        let claims = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "wrong-issuer".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator.validate(&token);
        assert!(matches!(result, Err(AppError::InvalidIssuer)));
    }

    #[test]
    fn invalid_audience_returns_audience_error() {
        let validator = create_validator();
        let claims = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "wrong-audience".to_string(),
            jti: "jti-123".to_string(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator.validate(&token);
        assert!(matches!(result, Err(AppError::InvalidAudience)));
    }

    #[test]
    fn valid_token_returns_validated_claims() {
        let validator = create_validator();
        let claims = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator.validate(&token);
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert_eq!(validated.sub, "user-123");
        assert_eq!(validated.role, "ADMIN");
        assert_eq!(validated.iss, "bornemap");
        assert_eq!(validated.aud, "bornemap-app");
    }

    #[test]
    fn user_id_parsing_succeeds_for_valid_uuid() {
        let validator = create_validator();
        let claims = TokenClaims {
            sub: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator.validate(&token);
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert!(validated.user_id().is_ok());
    }

    #[test]
    fn user_id_parsing_fails_for_invalid_uuid() {
        let validator = create_validator();
        let claims = TokenClaims {
            sub: "invalid-uuid".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator.validate(&token);
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert!(validated.user_id().is_err());
    }
}
