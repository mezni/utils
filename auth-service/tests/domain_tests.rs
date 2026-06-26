mod database_tests;
mod integration_tests;

use crate::domain::entities::RefreshToken;
use crate::domain::services::PasswordService;
use crate::domain::{Email, PasswordHash};
use uuid::Uuid;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_email_validation_valid() {
        let email = Email::new("test@example.com".to_string()).unwrap();
        assert_eq!(email.as_str(), "test@example.com");
    }

    #[test]
    fn test_email_validation_invalid() {
        assert!(Email::new("invalid-email".to_string()).is_err());
        assert!(Email::new("test@example".to_string()).is_err());
        assert!(Email::new("".to_string()).is_err());
    }

    #[test]
    fn test_password_hashing() {
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
    }

    #[test]
    fn test_refresh_token_creation() {
        let user_id = Uuid::new_v4();
        let jti = Uuid::new_v4();
        let expires_at = chrono::Utc::now() + chrono::Duration::minutes(30);

        let token = RefreshToken::new(user_id, jti, expires_at);

        assert_eq!(token.user_id, user_id);
        assert_eq!(token.jti, jti);
        assert!(!token.is_expired());
    }

    #[test]
    fn test_refresh_token_expiry() {
        let user_id = Uuid::new_v4();
        let jti = Uuid::new_v4();
        let expires_at = chrono::Utc::now() - chrono::Duration::minutes(30);

        let token = RefreshToken::new(user_id, jti, expires_at);

        assert!(token.is_expired());
    }
}
