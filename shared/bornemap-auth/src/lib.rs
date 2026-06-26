use bornemap_core::AuthError;
use chrono::Utc;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenClaims {
    pub sub: String,
    pub role: String,
    pub iat: i64,
    pub exp: i64,
}

#[derive(Clone)]
pub struct JwtService {
    secret: String,
    expiration_seconds: i64,
}

impl JwtService {
    pub fn new(secret: String, expiration_seconds: i64) -> Self {
        Self {
            secret,
            expiration_seconds,
        }
    }

    pub fn generate_token(&self, sub: &str, role: &str) -> Result<String, AuthError> {
        let now = Utc::now().timestamp();
        let claims = TokenClaims {
            sub: sub.to_string(),
            role: role.to_string(),
            iat: now,
            exp: now + self.expiration_seconds,
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|_| AuthError::InternalError)
    }

    pub fn validate_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        let token_data = decode::<TokenClaims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &Validation::default(),
        )
        .map_err(|_| AuthError::Unauthorized)?;

        Ok(token_data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_jwt(expiration: i64) -> JwtService {
        JwtService::new("test_secret_key_for_jwt_tests".into(), expiration)
    }

    #[test]
    fn roundtrip_generate_and_validate() {
        let svc = test_jwt(3600);
        let token = svc
            .generate_token("user-abc-123", "REGISTERED_DRIVER")
            .unwrap();
        let claims = svc.validate_token(&token).unwrap();
        assert_eq!(claims.sub, "user-abc-123");
        assert_eq!(claims.role, "REGISTERED_DRIVER");
    }

    #[test]
    fn wrong_secret_rejected() {
        let svc1 = test_jwt(3600);
        let svc2 = JwtService::new("different_secret_key".into(), 3600);
        let token = svc1.generate_token("user-id", "PARTNER").unwrap();
        let result = svc2.validate_token(&token);
        assert!(matches!(result, Err(AuthError::Unauthorized)));
    }

    #[test]
    fn expired_token_rejected() {
        let svc = test_jwt(-3600);
        let token = svc.generate_token("user-id", "PARTNER").unwrap();
        let result = svc.validate_token(&token);
        assert!(matches!(result, Err(AuthError::Unauthorized)));
    }
}
