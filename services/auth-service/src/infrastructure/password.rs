use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng},
};
use bornemap_core::AuthError;

pub struct PasswordService;

impl PasswordService {
    pub fn hash(password: &str) -> Result<String, AuthError> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| {
                tracing::error!("Password hashing error: {:?}", e);
                AuthError::InternalError
            })?;

        Ok(hash.to_string())
    }

    pub fn verify(password: &str, hash: &str) -> Result<bool, AuthError> {
        let parsed_hash = PasswordHash::new(hash).map_err(|e| {
            tracing::error!("Password hash parse error: {:?}", e);
            AuthError::InternalError
        })?;

        let argon2 = Argon2::default();
        match argon2.verify_password(password.as_bytes(), &parsed_hash) {
            Ok(_) => Ok(true), // Password matches
            Err(_) => Ok(false), // Password mismatch or other error
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_roundtrip() {
        let password = "secure_password_123!";
        let hash = PasswordService::hash(password).expect("hashing failed");
        assert!(PasswordService::verify(password, &hash).expect("verify failed"));
    }

    #[test]
    fn verify_wrong_password_rejected() {
        let hash = PasswordService::hash("correct_password").expect("hashing failed");
        // Test with a wrong password
        let verify_result = PasswordService::verify("wrong_password", &hash);
        assert!(verify_result.is_ok()); // Should return Ok(false) for wrong password
        assert!(!verify_result.unwrap()); // Check that it's false
    }

    #[test]
    fn verify_invalid_hash_returns_internal_error() {
        let result = PasswordService::verify("any_password", "not_a_valid_hash");
        assert!(matches!(result, Err(AuthError::InternalError)));
    }
}
