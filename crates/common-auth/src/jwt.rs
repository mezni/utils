use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::roles::Role;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: Uuid,
    pub role: Role,
    pub exp: usize,
    pub iat: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("JWT encoding failed: {0}")]
    Encoding(String),
    #[error("JWT decoding failed: {0}")]
    Decoding(String),
    #[error("Invalid token")]
    Invalid,
    #[error("Token expired")]
    Expired,
}

pub fn encode_jwt(user_id: Uuid, role: Role, secret: &str) -> Result<String, JwtError> {
    let now = Utc::now();
    let claims = Claims {
        sub: user_id,
        role,
        iat: now.timestamp() as usize,
        exp: (now + Duration::hours(24)).timestamp() as usize,
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| JwtError::Encoding(e.to_string()))
}

pub fn decode_jwt(token: &str, secret: &str) -> Result<Claims, JwtError> {
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|e| match e.kind() {
        jsonwebtoken::errors::ErrorKind::ExpiredSignature => JwtError::Expired,
        _ => JwtError::Decoding(e.to_string()),
    })?;

    Ok(token_data.claims)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roles::Role;

    #[test]
    fn test_jwt_roundtrip() {
        let secret = "test-secret";
        let user_id = Uuid::new_v4();
        let role = Role::Admin;

        let token = encode_jwt(user_id, role, secret).unwrap();
        let claims = decode_jwt(&token, secret).unwrap();

        assert_eq!(claims.sub, user_id);
        assert_eq!(claims.role, Role::Admin);
    }

    #[test]
    fn test_jwt_invalid_secret() {
        let user_id = Uuid::new_v4();
        let token = encode_jwt(user_id, Role::Driver, "correct-secret").unwrap();
        assert!(decode_jwt(&token, "wrong-secret").is_err());
    }
}
