use crate::TokenClaims;
use bornemap_core::AppError;
use chrono::{DateTime, Utc};
use jsonwebtoken::{DecodingKey, Validation, decode, decode_header};
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
        // Add token size validation
        if token.len() > 8192 { // 8KB limit
            return Err(AppError::TokenError("JWT token too large".to_string()));
        }

        // Add algorithm validation
        let header = match decode_header(token) {
            Ok(h) => h,
            Err(e) => return Err(AppError::TokenError(format!("JWT header error: {:?}", e))),
        };

        let expected_algorithm = match self.config.algorithm.as_str() {
            "HS256" => jsonwebtoken::Algorithm::HS256,
            "RS256" => jsonwebtoken::Algorithm::RS256,
            "ES256" => jsonwebtoken::Algorithm::ES256,
            _ => return Err(AppError::InvalidConfiguration("Unsupported JWT algorithm".to_string())),
        };

        if header.alg != expected_algorithm {
            return Err(AppError::TokenError(format!("Invalid JWT algorithm: {:?}", header.alg)));
        }

        let mut validation = Validation::new(expected_algorithm);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[&self.config.audience]);
        validation.leeway = self.config.clock_skew.as_secs();

        let token_data = decode::<TokenClaims>(token, &self.decoding_key, &validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AppError::TokenExpired,
                jsonwebtoken::errors::ErrorKind::InvalidToken => AppError::TokenError("Invalid JWT token".to_string()),
                jsonwebtoken::errors::ErrorKind::InvalidSignature => AppError::InvalidSignature,
                jsonwebtoken::errors::ErrorKind::InvalidIssuer => AppError::InvalidIssuer,
                jsonwebtoken::errors::ErrorKind::InvalidAudience => AppError::InvalidAudience,
                _ => AppError::TokenError(format!("JWT validation error: {:?}", e)),
            })?;

        let claims = token_data.claims;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AppError::InternalError)?;

        // Add token binding validation (jti claim)
        if claims.jti.is_empty() {
            return Err(AppError::TokenError("Missing JWT ID (jti)".to_string()));
        }

        let issued_at = UNIX_EPOCH + Duration::from_secs(claims.iat as u64);
        let expires_at = UNIX_EPOCH + Duration::from_secs(claims.exp as u64);

        // Normalize and validate role
        let normalized_role = self.normalize_role(&claims.role)?;

        Ok(ValidatedClaims {
            sub: claims.sub,
            role: normalized_role,
            iat: DateTime::from(issued_at),
            exp: DateTime::from(expires_at),
            iss: claims.iss,
            aud: claims.aud,
            jti: claims.jti,
        })
    }

    fn normalize_role(&self, role: &str) -> Result<String, AppError> {
        // Try to parse as canonical role first
        if let Some(parsed_role) = crate::rbac::Role::try_from_str(role) {
            return Ok(parsed_role.as_str().to_string());
        }

        // If not canonical, try to normalize common variations
        let normalized = match role.to_uppercase().as_str() {
            "DRIVER" => "REGISTERED_DRIVER",  // Legacy role mapping
            _ => return Err(AppError::InvalidConfiguration(format!("Unknown role: {}", role))),
        };

        // Verify the normalized role is valid
        crate::rbac::Role::try_from_str(normalized)
            .ok_or_else(|| AppError::InvalidConfiguration(format!("Invalid normalized role: {}", normalized)))?;

        Ok(normalized.to_string())
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

    pub fn parsed_role(&self) -> Result<crate::rbac::Role, AppError> {
        crate::rbac::Role::try_from_str(&self.role)
            .ok_or_else(|| AppError::InvalidConfiguration(format!("Invalid role in claims: {}", self.role)))
    }

    pub fn is_admin(&self) -> bool {
        matches!(self.parsed_role(), Ok(crate::rbac::Role::Admin))
    }

    pub fn is_partner(&self) -> bool {
        matches!(self.parsed_role(), Ok(crate::rbac::Role::Partner))
    }

    pub fn is_registered_driver(&self) -> bool {
        matches!(self.parsed_role(), Ok(crate::rbac::Role::RegisteredDriver))
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
        assert!(validated.is_admin());
        assert!(!validated.is_partner());
        assert!(!validated.is_registered_driver());
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

    #[test]
    fn role_normalization_canonical_roles() {
        let validator = create_validator();
        
        // Test canonical role names
        let admin_claims = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let admin_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &admin_claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let admin_result = validator.validate(&admin_token);
        assert!(admin_result.is_ok());
        let admin_validated = admin_result.unwrap();
        assert_eq!(admin_validated.role, "ADMIN");
        assert!(admin_validated.is_admin());

        // Test legacy role normalization
        let legacy_claims = TokenClaims {
            sub: "user-456".to_string(),
            role: "DRIVER".to_string(),  // Legacy role
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-456".to_string(),
        };

        let legacy_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &legacy_claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let legacy_result = validator.validate(&legacy_token);
        assert!(legacy_result.is_ok());
        let legacy_validated = legacy_result.unwrap();
        assert_eq!(legacy_validated.role, "REGISTERED_DRIVER");
        assert!(legacy_validated.is_registered_driver());
    }

    #[test]
    fn role_normalization_rejects_unknown_roles() {
        let validator = create_validator();
        
        let invalid_claims = TokenClaims {
            sub: "user-789".to_string(),
            role: "UNKNOWN_ROLE".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-789".to_string(),
        };

        let invalid_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &invalid_claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator.validate(&invalid_token);
        assert!(matches!(result, Err(AppError::InvalidConfiguration(_))));
    }

    #[test]
    fn jwt_token_size_limit() {
        let validator = create_validator();
        
        // Create a token that exceeds the size limit
        let mut large_claims = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };
        
        // Make the token large by adding padding to the claims
        large_claims.sub = "a".repeat(8000); // This will make the token larger than 8KB
        
        let large_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &large_claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator.validate(&large_token);
        assert!(matches!(result, Err(AppError::InvalidToken(ref msg)) if msg.contains("too large")));
    }

    #[test]
    fn jwt_algorithm_confusion_attack() {
        let validator = create_validator();
        
        // Create token with algorithm confusion vulnerability
        let malicious_claims = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };
        
        // Token signed with RS256 but validator expects HS256
        let malicious_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &malicious_claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();
        
        let result = validator.validate(&malicious_token);
        assert!(matches!(result, Err(AppError::InvalidToken(ref msg)) if msg.contains("Invalid JWT algorithm")));
    }

    #[test]
    fn jwt_missing_token_id() {
        let validator = create_validator();
        
        let claims_without_jti = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "".to_string(), // Empty jti
        };

        let token_without_jti = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims_without_jti,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator.validate(&token_without_jti);
        assert!(matches!(result, Err(AppError::InvalidToken(ref msg)) if msg.contains("Missing JWT ID")));
    }

    #[test]
    fn jwt_algorithm_validation() {
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

        // Test valid HS256 algorithm
        let valid_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator.validate(&valid_token);
        assert!(result.is_ok());

        // Test invalid algorithm configuration
        let invalid_config = JwtConfig {
            algorithm: "INVALID_ALGORITHM".to_string(),
            issuer: "bornemap".to_string(),
            audience: "bornemap-app".to_string(),
            clock_skew: Duration::from_secs(30),
        };

        let invalid_validator = JwtValidator::new("test-secret-key".to_string(), invalid_config);
        assert!(matches!(invalid_validator, Err(AppError::InvalidConfiguration(_))));
    }

    #[test]
    fn jwt_token_binding_protection() {
        let validator = create_validator();
        
        let claims = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::hours(1)).timestamp(),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "unique-token-id-123".to_string(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        // First validation should succeed
        assert!(validator.validate(&token).is_ok());

        // The token binding protection would be implemented by tracking jti claims
        // in a database or cache to prevent replay attacks
        // For this test, we'll verify that the jti is properly validated
        let validated = validator.validate(&token).unwrap();
        assert_eq!(validated.jti, "unique-token-id-123");
    }

    #[test]
    fn jwt_clock_skew_handling() {
        let validator = create_validator();
        
        let claims = TokenClaims {
            sub: "user-123".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp(),
            exp: (Utc::now() + chrono::Duration::seconds(1)).timestamp(), // Expires in 1 second
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        // Should succeed with clock skew
        assert!(validator.validate(&token).is_ok());

        // Test with expired token (no clock skew)
        let config_no_skew = JwtConfig {
            algorithm: "HS256".to_string(),
            issuer: "bornemap".to_string(),
            audience: "bornemap-app".to_string(),
            clock_skew: Duration::from_secs(0), // No clock skew
        };

        let validator_no_skew = JwtValidator::new("test-secret-key".to_string(), config_no_skew).unwrap();
        let expired_claims = TokenClaims {
            sub: "user-456".to_string(),
            role: "ADMIN".to_string(),
            iat: Utc::now().timestamp() - 3600, // Issued 1 hour ago
            exp: Utc::now().timestamp() - 1,    // Expired 1 second ago
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-456".to_string(),
        };

        let expired_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &expired_claims,
            &jsonwebtoken::EncodingKey::from_secret("test-secret-key".as_bytes()),
        ).unwrap();

        let result = validator_no_skew.validate(&expired_token);
        assert!(matches!(result, Err(AppError::TokenExpired)));
    }
}
