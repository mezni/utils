use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    pub sub: String,
    pub partner_id: Option<String>,
    pub role: String,
    pub exp: usize,
    pub iat: usize,
}

pub fn validate_claims(claims: &JwtClaims) -> Result<(), crate::AuthError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as usize;

    if now > claims.exp {
        return Err(crate::AuthError::ExpiredToken);
    }
    Ok(())
}
