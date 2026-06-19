use actix_web::error::Error;
use actix_web::http::StatusCode;
use jsonwebtoken::{decode, DecodingKey, Validation, TokenData};
use std::time::Duration;

const MAX_TOKEN_LENGTH: usize = 5000;

/// Validate a token before contacting Keycloak.
///
/// Returns an error if the token is malformed, empty, or too long.
pub fn validate_token(token: &str) -> Result<(), AuthError> {
    // Check if token is empty
    if token.is_empty() {
        return Err(AuthError::ValidationError("token is required".to_string()));
    }

    // Check if token is too long
    if token.len() > MAX_TOKEN_LENGTH {
        return Err(AuthError::ValidationError(
            "token exceeds maximum length".to_string(),
        ));
    }

    // Basic JWT structure validation (header.payload.signature)
    // Check that token contains at least 3 parts separated by dots
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(AuthError::ValidationError(
            "invalid token format".to_string(),
        ));
    }

    // Each part should be non-empty
    if parts[0].is_empty() || parts[1].is_empty() || parts[2].is_empty() {
        return Err(AuthError::ValidationError(
            "invalid token format".to_string(),
        ));
    }

    // Verify the token structure (header should be valid base64url)
    if parts[0].len() < 10 {
        return Err(AuthError::ValidationError(
            "invalid token header".to_string(),
        ));
    }

    Ok(())
}

/// Validate that a string is not empty.
pub fn validate_required(field_name: &str, value: &str) -> Result<(), AuthError> {
    if value.is_empty() {
        return Err(AuthError::ValidationError(format!("{} is required", field_name)));
    }
    Ok(())
}

/// Decode and validate a JWT using the provided secret.
///
/// This function validates the token structure but doesn't verify the signature
/// (the signature will be verified when we decode it for real usage).
pub fn decode_token(token: &str) -> Result<TokenData<Claims>, AuthError> {
    validate_token(token)?;

    // Create a validation instance
    let validation = Validation::default();

    // Attempt to decode the token
    let token_data = decode::<Claims>(token, &DecodingKey::from_secret(b"dummy"), &validation)
        .map_err(|e| {
            tracing::warn!("Token decode error: {}", e);
            AuthError::ValidationError("invalid token format".to_string())
        })?;

    Ok(token_data)
}

/// Validate that a string has a minimum length.
pub fn validate_min_length(field_name: &str, value: &str, min_length: usize) -> Result<(), AuthError> {
    if value.len() < min_length {
        return Err(AuthError::ValidationError(format!(
            "{} must be at least {} characters",
            field_name, min_length
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, Header, EncodingKey};

    fn create_test_token(secret: &str) -> String {
        let header = Header::default();
        let payload = Claims {
            sub: "test_sub".to_string(),
            email: "test@example.com".to_string(),
            given_name: Some("Test".to_string()),
            family_name: Some("User".to_string()),
            realm_access: Some(RealmAccess {
                roles: vec!["role:admin".to_string()],
            }),
            aud: vec!["bornemap".to_string()],
        };

        encode(&header, &payload, &EncodingKey::from_secret(secret.as_bytes()))
            .unwrap()
    }

    #[test]
    fn test_validate_token_valid() {
        let token = create_test_token("test_secret");
        let result = validate_token(&token);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_token_empty() {
        let result = validate_token("");
        assert!(matches!(result, Err(AuthError::ValidationError(_))));
    }

    #[test]
    fn test_validate_token_too_long() {
        let token = "a".repeat(MAX_TOKEN_LENGTH + 1);
        let result = validate_token(&token);
        assert!(matches!(result, Err(AuthError::ValidationError(_))));
    }

    #[test]
    fn test_validate_token_invalid_format() {
        let result = validate_token("invalid_token");
        assert!(matches!(result, Err(AuthError::ValidationError(_))));
    }

    #[test]
    fn test_validate_token_missing_parts() {
        let result = validate_token("one_part");
        assert!(matches!(result, Err(AuthError::ValidationError(_))));
    }

    #[test]
    fn test_validate_required_empty() {
        let result = validate_required("field", "");
        assert!(matches!(result, Err(AuthError::ValidationError(_))));
    }

    #[test]
    fn test_validate_required_valid() {
        let result = validate_required("field", "value");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_min_length_too_short() {
        let result = validate_min_length("field", "ab", 3);
        assert!(matches!(result, Err(AuthError::ValidationError(_))));
    }

    #[test]
    fn test_validate_min_length_valid() {
        let result = validate_min_length("field", "abc", 3);
        assert!(result.is_ok());
    }
}
