use actix_web::HttpRequest;
use crate::domain::JwtClaims;
use crate::error::AuthServiceError;

/// Extract JWT token from Authorization header
pub fn extract_token_from_header(req: &HttpRequest) -> Result<String, AuthServiceError> {
    let auth_header = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .ok_or(AuthServiceError::InvalidToken)?;

    if !auth_header.starts_with("Bearer ") {
        return Err(AuthServiceError::InvalidToken);
    }

    Ok(auth_header.trim_start_matches("Bearer ").to_string())
}

/// Validate JWT claims for required role
pub fn validate_role(claims: &JwtClaims, required_role: &str) -> Result<(), AuthServiceError> {
    if !claims.roles.contains(&required_role.to_string()) {
        Err(AuthServiceError::InvalidToken)
    } else {
        Ok(())
    }
}
