use std::env;
use std::time::{Duration, SystemTime};
use uuid::Uuid;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

/// Admin JWT token generation
pub fn generate_admin_token() -> Result<String, jsonwebtoken::errors::Error> {
    let secret = env::var("ADMIN_JWT_SECRET")
        .unwrap_or_else(|_| "dev-admin-secret".to_string());

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let claims = AdminClaims {
        sub: Uuid::new_v4().to_string(),
        role: "admin".to_string(),
        exp: now + Duration::from_secs(3600).as_secs(), // 1 hour expiration
        iat: now,
        iss: "bornemap-admin".to_string(),
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_ref()),
    )
}

/// Admin JWT token verification
pub fn verify_admin_token(token: &str) -> Result<AdminClaims, jsonwebtoken::errors::Error> {
    let secret = env::var("ADMIN_JWT_SECRET")
        .unwrap_or_else(|_| "dev-admin-secret".to_string());

    let decoding_key = DecodingKey::from_secret(secret.as_ref());
    let token_data = decode::<AdminClaims>(
        token,
        &decoding_key,
        &Validation::new(Algorithm::HS256),
    )?;

    Ok(token_data.claims)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AdminClaims {
    pub sub: String, // Subject (user ID)
    pub role: String, // Role (admin)
    pub exp: usize, // Expiration timestamp
    pub iat: usize, // Issued at timestamp
    pub iss: String, // Issuer
}

/// Check if JWT token is for admin role
pub fn is_admin_token(token: &str) -> Result<bool, jsonwebtoken::errors::Error> {
    let claims = verify_admin_token(token)?;
    Ok(claims.role == "admin")
}
