use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2, ParamsBuilder,
};
use shared_contracts::PasswordHash as ContractPasswordHash;
use tracing::{error, info};

const ARGON2_ALGORITHM: &str = "argon2id";
const ARGON2_COST: u32 = 12;
const ARGON2_MEMORY: u64 = 64 * 1024; // 64 MB
const ARGON2_PARALLELISM: u32 = 4;
const ARGON2_TIME_COST: u32 = 3;

pub struct PasswordService;

impl PasswordService {
    /// Hash a password using Argon2id
    pub fn hash(password: &str, pepper: &str) -> Result<ContractPasswordHash, argon2::password_hash::Error> {
        info!("Hashing password with Argon2id");

        let pepper = pepper.as_bytes();

        let params = ParamsBuilder::default()
            .m(ARGON2_MEMORY)
            .t(ARGON2_TIME_COST)
            .p(ARGON2_PARALLELISM)
            .build()
            .expect("Invalid Argon2 parameters");

        let salt = SaltString::from_os_rng(&mut OsRng);
        let argon2 = Argon2::default();

        let password_hash = argon2
            .hash_password(pepper.chain(password.as_bytes()).as_ref(), &salt, &params)
            .expect("Argon2 hashing failed")?
            .to_string();

        let algorithm = ARGON2_ALGORITHM.to_string();
        let cost = ARGON2_COST;

        info!("Password hashed successfully");
        Ok(ContractPasswordHash {
            value: password_hash,
            algorithm,
            cost,
        })
    }

    /// Verify a password against a hash
    pub fn verify(password: &str, hash: &ContractPasswordHash, pepper: &str) -> Result<(), argon2::password_hash::Error> {
        info!("Verifying password");

        let pepper = pepper.as_bytes();
        let full_password = pepper.chain(password.as_bytes()).as_ref();

        let params = ParamsBuilder::default()
            .m(ARGON2_MEMORY)
            .t(ARGON2_TIME_COST)
            .p(ARGON2_PARALLELISM)
            .build()
            .expect("Invalid Argon2 parameters");

        let salt = SaltString::from_b64(&hash.value.split('$').nth(3).unwrap_or(""))?;
        let argon2 = Argon2::default();
        let stored_hash = PasswordHash::new(&hash.value)?;

        argon2.verify_password(full_password, &salt, &stored_hash)?;

        info!("Password verified successfully");
        Ok(())
    }

    /// Validate password strength
    pub fn validate_password_strength(password: &str) -> Result<(), String> {
        if password.len() < 12 {
            return Err("Password must be at least 12 characters long".to_string());
        }

        if password.len() > 128 {
            return Err("Password must be at most 128 characters long".to_string());
        }

        if !password.contains(char::is_uppercase) {
            return Err("Password must contain at least one uppercase letter".to_string());
        }

        if !password.contains(char::is_lowercase) {
            return Err("Password must contain at least one lowercase letter".to_string());
        }

        if !password.chars().any(|c| c.is_numeric()) {
            return Err("Password must contain at least one number".to_string());
        }

        if !password.chars().any(|c| c.is_punctuation()) {
            return Err("Password must contain at least one special character".to_string());
        }

        Ok(())
    }

    /// Generate a random refresh token string
    pub fn generate_refresh_token() -> String {
        use rand::Rng;
        let mut rng = rand::rngs::OsRng;
        let mut bytes = [0u8; 64];
        rng.fill_bytes(&mut bytes);
        format!("{:?}", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_hashing() {
        let password = "SecurePassword123!";
        let pepper = "dev_pepper";

        let hash = PasswordService::hash(password, pepper).unwrap();

        assert!(!hash.value.is_empty());
        assert_eq!(hash.algorithm, ARGON2_ALGORITHM);
        assert_eq!(hash.cost, ARGON2_COST);
    }

    #[test]
    fn test_password_verification() {
        let password = "SecurePassword123!";
        let pepper = "dev_pepper";

        let hash = PasswordService::hash(password, pepper).unwrap();
        let result = PasswordService::verify(password, &hash, pepper);

        assert!(result.is_ok());
    }

    #[test]
    fn test_password_verification_failure() {
        let password = "SecurePassword123!";
        let wrong_password = "WrongPassword456!";
        let pepper = "dev_pepper";

        let hash = PasswordService::hash(password, pepper).unwrap();
        let result = PasswordService::verify(wrong_password, &hash, pepper);

        assert!(result.is_err());
    }

    #[test]
    fn test_password_strength_validation() {
        assert!(PasswordService::validate_password_strength("SecurePassword123!").is_ok());
        assert!(PasswordService::validate_password_strength("short").is_err());
        assert!(PasswordService::validate_password_strength("nocaps123!").is_err());
        assert!(PasswordService::validate_password_strength("NoNumber!").is_err());
        assert!(PasswordService::validate_password_strength("NoSpecial!").is_err());
    }

    #[test]
    fn test_refresh_token_generation() {
        let token = PasswordService::generate_refresh_token();
        assert!(!token.is_empty());
        assert!(token.len() > 50);
    }
}