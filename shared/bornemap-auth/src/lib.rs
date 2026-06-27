pub mod oauth;

use bornemap_core::AppError;
use chrono::Utc;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenClaims {
    pub sub: String,
    pub role: String,
    pub iat: i64,
    pub exp: i64,
    pub iss: String,
    pub aud: String,
    pub jti: String,
}

#[derive(Clone)]
pub struct JwtService {
    secret: String,
    access_ttl_seconds: i64,
    issuer: String,
    audience: String,
}

impl JwtService {
    pub fn new(secret: String, access_ttl_seconds: i64, issuer: String, audience: String) -> Self {
        Self {
            secret,
            access_ttl_seconds,
            issuer,
            audience,
        }
    }

    pub fn generate_token(&self, sub: &str, role: &str) -> Result<String, AppError> {
        let now = Utc::now().timestamp();
        let claims = TokenClaims {
            sub: sub.to_string(),
            role: role.to_string(),
            iat: now,
            exp: now + self.access_ttl_seconds,
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            jti: Uuid::new_v4().to_string(),
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|e| AppError::TokenError(e.to_string()))
    }

    pub fn validate_token(&self, token: &str) -> Result<TokenClaims, AppError> {
        let mut validation = Validation::default();
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.audience]);

        let token_data = decode::<TokenClaims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &validation,
        )
        .map_err(|e| AppError::TokenError(e.to_string()))?;

        Ok(token_data.claims)
    }

    pub fn generate_refresh_token() -> (String, String) {
        let token_bytes: [u8; 32] = rand::random();
        let token = hex::encode(token_bytes);
        let hash = hash_refresh_token(&token);
        (token, hash)
    }
}

pub fn hash_refresh_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_jwt() -> JwtService {
        JwtService::new(
            "test_secret_key_for_jwt_tests".into(),
            3600,
            "test-issuer".into(),
            "test-audience".into(),
        )
    }

    #[test]
    fn roundtrip_generate_and_validate() {
        let svc = test_jwt();
        let token = svc
            .generate_token("user-abc-123", "REGISTERED_DRIVER")
            .unwrap();
        let claims = svc.validate_token(&token).unwrap();
        assert_eq!(claims.sub, "user-abc-123");
        assert_eq!(claims.role, "REGISTERED_DRIVER");
        assert_eq!(claims.iss, "test-issuer");
        assert_eq!(claims.aud, "test-audience");
        assert!(!claims.jti.is_empty());
    }

    #[test]
    fn wrong_secret_rejected() {
        let svc1 = test_jwt();
        let svc2 = JwtService::new(
            "different_secret_key".into(),
            3600,
            "test-issuer".into(),
            "test-audience".into(),
        );
        let token = svc1.generate_token("user-id", "PARTNER").unwrap();
        let result = svc2.validate_token(&token);
        assert!(matches!(result, Err(AppError::TokenError(_))));
    }

    #[test]
    fn wrong_issuer_rejected() {
        let svc1 = test_jwt();
        let svc2 = JwtService::new(
            "test_secret_key_for_jwt_tests".into(),
            3600,
            "wrong-issuer".into(),
            "test-audience".into(),
        );
        let token = svc1.generate_token("user-id", "PARTNER").unwrap();
        let result = svc2.validate_token(&token);
        assert!(matches!(result, Err(AppError::TokenError(_))));
    }

    #[test]
    fn expired_token_rejected() {
        let svc = JwtService::new(
            "test_secret_key_for_jwt_tests".into(),
            -3600,
            "test-issuer".into(),
            "test-audience".into(),
        );
        let token = svc.generate_token("user-id", "PARTNER").unwrap();
        let result = svc.validate_token(&token);
        assert!(matches!(result, Err(AppError::TokenError(_))));
    }

    #[test]
    fn refresh_token_hash_roundtrip() {
        let (token, hash) = JwtService::generate_refresh_token();
        assert_eq!(hash, hash_refresh_token(&token));
        assert_eq!(hash.len(), 64);
        assert_eq!(token.len(), 64);
    }

    #[test]
    fn refresh_token_unique() {
        let (t1, _) = JwtService::generate_refresh_token();
        let (t2, _) = JwtService::generate_refresh_token();
        assert_ne!(t1, t2);
    }
}
