use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;

use crate::domain::error::DomainError;

pub struct PasswordService;

impl PasswordService {
    pub fn hash(password: &str) -> Result<String, DomainError> {
        if password.len() < 8 {
            return Err(DomainError::WeakPassword);
        }
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|_| DomainError::InvalidCredentials)?
            .to_string();
        Ok(hash)
    }

    pub fn verify(password: &str, hash: &str) -> bool {
        let parsed_hash = match PasswordHash::new(hash) {
            Ok(h) => h,
            Err(_) => return false,
        };
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_hashing() {
        let password = "secure_password_123";
        let hash = PasswordService::hash(password).unwrap();
        assert!(PasswordService::verify(password, &hash));
    }

    #[test]
    fn test_password_verification_fails_for_wrong_password() {
        let hash = PasswordService::hash("correct_password").unwrap();
        assert!(!PasswordService::verify("wrong_password", &hash));
    }

    #[test]
    fn test_weak_password_rejected() {
        let result = PasswordService::hash("short");
        assert!(result.is_err());
    }

    #[test]
    fn test_different_hashes_for_same_password() {
        let pw = "same_password_here";
        let h1 = PasswordService::hash(pw).unwrap();
        let h2 = PasswordService::hash(pw).unwrap();
        assert_ne!(h1, h2);
    }
}
