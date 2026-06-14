use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use crate::domain::JwtClaims;
use crate::error::AuthServiceError;
use chrono::Utc;

const TOKEN_EXPIRY_HOURS: i64 = 24;

pub fn create_jwt(
    user_id: String,
    email: String,
    roles: Vec<String>,
    secret: &str,
) -> Result<String, AuthServiceError> {
    let now = Utc::now().timestamp();
    let exp = now + (TOKEN_EXPIRY_HOURS * 3600);

    let claims = JwtClaims {
        sub: user_id,
        email,
        roles,
        iat: now,
        exp,
        iss: "https://auth.bornemap.tn".to_string(),
    };

    let key = EncodingKey::from_secret(secret.as_ref());
    encode(&Header::default(), &claims, &key)
        .map_err(|_| AuthServiceError::JwtError)
}

pub fn verify_jwt(token: &str, secret: &str) -> Result<JwtClaims, AuthServiceError> {
    let key = DecodingKey::from_secret(secret.as_ref());
    let token_data = decode::<JwtClaims>(
        token,
        &key,
        &Validation::default(),
    )
    .map_err(|_| AuthServiceError::InvalidToken)?;

    Ok(token_data.claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_roundtrip() {
        let secret = "test-secret-key";
        let token = create_jwt(
            "user-123".to_string(),
            "test@example.com".to_string(),
            vec!["driver".to_string()],
            secret,
        ).unwrap();

        let claims = verify_jwt(&token, secret).unwrap();
        assert_eq!(claims.sub, "user-123");
        assert_eq!(claims.email, "test@example.com");
    }

    #[test]
    fn test_jwt_expiry() {
        let secret = "test-secret-key";
        let token = create_jwt(
            "user-123".to_string(),
            "test@example.com".to_string(),
            vec!["driver".to_string()],
            secret,
        ).unwrap();

        let claims = verify_jwt(&token, secret).unwrap();
        let now = Utc::now().timestamp();
        assert!(claims.exp > now);
    }
}
