use bornemap_core::AppError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenClaims {
    pub sub: String,
    pub role: String,
}

pub struct JwtValidator;

/// SPRINT 01 STUB ONLY — no real validation yet
impl JwtValidator {
    pub fn validate(_token: &str) -> Result<TokenClaims, AppError> {
        Err(AppError::Unauthorized)
    }
}
