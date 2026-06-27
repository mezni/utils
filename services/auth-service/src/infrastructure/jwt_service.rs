use crate::infrastructure::jwt::JwtService;
use actix_web::web::Data;
use bornemap_auth::jwt_validator::{JwtConfig, JwtValidator};
use bornemap_core::AppError;
use std::sync::Arc;

pub struct AuthJwtService {
    inner: Arc<JwtService>,
    validator: Arc<JwtValidator>,
}

impl AuthJwtService {
    pub fn new(secret: String, config: JwtConfig) -> Result<Self, AppError> {
        let inner = JwtService::new(
            secret.clone(),
            3600, // Default 1 hour access TTL
            config.issuer.clone(),
            config.audience.clone(),
        );

        let validator = JwtValidator::new(secret, config)?;

        Ok(Self {
            inner: Arc::new(inner),
            validator: Arc::new(validator),
        })
    }

    pub fn validator(&self) -> &JwtValidator {
        &self.validator
    }

    pub fn generate_token(&self, user_id: &str, role: &str) -> Result<String, AppError> {
        self.inner.generate_token(user_id, role)
    }

    pub fn generate_refresh_token() -> (String, String) {
        bornemap_auth::JwtService::generate_refresh_token()
    }

    pub fn hash_refresh_token(token: &str) -> String {
        bornemap_auth::hash_refresh_token(token)
    }

    pub fn data(service: AuthJwtService) -> Data<AuthJwtService> {
        Data::new(service)
    }
}

impl Clone for AuthJwtService {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            validator: self.validator.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bornemap_auth::jwt_validator::JwtConfig;

    #[test]
    fn auth_jwt_service_creation() {
        let config = JwtConfig::default();
        let result = AuthJwtService::new("test-secret-key".to_string(), config);
        assert!(result.is_ok());
    }

    #[test]
    fn auth_jwt_service_creation_with_empty_secret_fails() {
        let config = JwtConfig::default();
        let result = AuthJwtService::new("".to_string(), config);
        assert!(matches!(result, Err(AppError::InvalidConfiguration(_))));
    }

    #[test]
    fn auth_jwt_service_token_generation() {
        let config = JwtConfig::default();
        let service = AuthJwtService::new("test-secret-key".to_string(), config).unwrap();
        
        let token = service.generate_token("user-123", "ADMIN");
        assert!(token.is_ok());
        let token = token.unwrap();
        assert!(!token.is_empty());
    }

    #[test]
    fn auth_jwt_service_token_validation() {
        let config = JwtConfig::default();
        let service = AuthJwtService::new("test-secret-key".to_string(), config).unwrap();
        
        let token = service.generate_token("user-123", "ADMIN").unwrap();
        let claims = service.validator().validate(&token).unwrap();
        
        assert_eq!(claims.sub, "user-123");
        assert_eq!(claims.role, "ADMIN");
        assert_eq!(claims.iss, "bornemap");
        assert_eq!(claims.aud, "bornemap-app");
    }

    #[test]
    fn auth_jwt_service_refresh_token() {
        let (token, hash) = AuthJwtService::generate_refresh_token();
        assert!(!token.is_empty());
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64);
        assert_eq!(token.len(), 64);
    }

    #[test]
    fn auth_jwt_service_hash_refresh_token() {
        let token = "test-refresh-token";
        let hash = AuthJwtService::hash_refresh_token(token);
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64);
    }
}
