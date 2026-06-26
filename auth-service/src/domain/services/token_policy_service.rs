use tracing::{info, warn};
use uuid::Uuid;

pub struct TokenPolicyService;

impl TokenPolicyService {
    /// Generate a unique token ID (JTI)
    pub fn generate_jti() -> Uuid {
        let jti = Uuid::new_v4();
        info!("Generated new JTI: {}", jti);
        jti
    }

    /// Validate token expiration
    pub fn validate_expiry(expires_at: chrono::DateTime<chrono::Utc>) -> Result<(), String> {
        let now = chrono::Utc::now();

        if expires_at < now {
            return Err(format!(
                "Token expired at {}, current time: {}",
                expires_at, now
            ));
        }

        Ok(())
    }

    /// Calculate remaining token lifetime in seconds
    pub fn remaining_lifetime(expires_at: chrono::DateTime<chrono::Utc>) -> i64 {
        let now = chrono::Utc::now();
        let duration = expires_at - now;
        duration.num_seconds()
    }

    /// Generate access token expiration time (5 minutes from now)
    pub fn generate_access_token_expiration() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now() + chrono::Duration::minutes(5)
    }

    /// Generate refresh token expiration time (30 days from now)
    pub fn generate_refresh_token_expiration() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now() + chrono::Duration::days(30)
    }

    /// Validate refresh token expiration
    pub fn validate_refresh_token_expiry(expires_at: chrono::DateTime<chrono::Utc>) -> Result<(), String> {
        let remaining = self.remaining_lifetime(expires_at);

        if remaining < 0 {
            return Err(format!(
                "Refresh token expired at {}, remaining: {} seconds",
                expires_at, remaining
            ));
        }

        if remaining < 3600 {
            warn!(
                "Refresh token has less than 1 hour remaining: {} seconds",
                remaining
            );
        }

        Ok(())
    }

    /// Check if token should be revoked due to reuse
    pub fn check_token_reuse(old_jti: Uuid, new_jti: Uuid) -> bool {
        old_jti == new_jti
    }

    /// Validate access token TTL
    pub fn validate_access_token_ttl(minutes: i64) -> Result<(), String> {
        if minutes <= 0 {
            return Err("Access token TTL must be greater than 0 minutes".to_string());
        }

        if minutes > 60 {
            return Err("Access token TTL cannot exceed 60 minutes".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jti_generation() {
        let jti1 = TokenPolicyService::generate_jti();
        let jti2 = TokenPolicyService::generate_jti();

        assert_ne!(jti1, jti2);
    }

    #[test]
    fn test_valid_expiry() {
        let future = chrono::Utc::now() + chrono::Duration::minutes(10);
        assert!(TokenPolicyService::validate_expiry(future).is_ok());
    }

    #[test]
    fn test_invalid_expiry() {
        let past = chrono::Utc::now() - chrono::Duration::minutes(10);
        assert!(TokenPolicyService::validate_expiry(past).is_err());
    }

    #[test]
    fn test_remaining_lifetime() {
        let future = chrono::Utc::now() + chrono::Duration::minutes(30);
        let lifetime = TokenPolicyService::remaining_lifetime(future);
        assert!(lifetime > 29 * 60 && lifetime < 31 * 60);
    }

    #[test]
    fn test_access_token_expiration() {
        let expires_at = TokenPolicyService::generate_access_token_expiration();
        assert!(TokenPolicyService::validate_expiry(expires_at).is_ok());
        assert_eq!(
            TokenPolicyService::remaining_lifetime(expires_at),
            300
        );
    }

    #[test]
    fn test_refresh_token_expiration() {
        let expires_at = TokenPolicyService::generate_refresh_token_expiration();
        assert!(TokenPolicyService::validate_expiry(expires_at).is_ok());
        assert!(TokenPolicyService::remaining_lifetime(expires_at) > 30 * 24 * 3600);
    }

    #[test]
    fn test_token_reuse_check() {
        let jti = Uuid::new_v4();
        assert!(TokenPolicyService::check_token_reuse(jti, jti));

        let jti1 = Uuid::new_v4();
        let jti2 = Uuid::new_v4();
        assert!(!TokenPolicyService::check_token_reuse(jti1, jti2));
    }

    #[test]
    fn test_access_token_ttl_validation() {
        assert!(TokenPolicyService::validate_access_token_ttl(5).is_ok());
        assert!(TokenPolicyService::validate_access_token_ttl(60).is_ok());
        assert!(TokenPolicyService::validate_access_token_ttl(0).is_err());
        assert!(TokenPolicyService::validate_access_token_ttl(-5).is_err());
        assert!(TokenPolicyService::validate_access_token_ttl(61).is_err());
    }
}