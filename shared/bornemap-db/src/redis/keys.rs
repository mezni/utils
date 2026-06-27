use std::time::Duration;

pub struct RedisKeys;

impl RedisKeys {
    /// OAuth state keys for anti-CSRF protection
    pub fn oauth_state(state: &str) -> String {
        format!("oauth_state:{}", state)
    }

    /// Rate limiting keys for IP-based rate limiting
    pub fn rate_limit(ip: &str) -> String {
        format!("rate_limit:{}", ip)
    }

    /// Session keys for temporary authentication data
    pub fn session(session_id: &str) -> String {
        format!("session:{}", session_id)
    }

    /// Email verification keys
    pub fn email_verification(email: &str) -> String {
        format!("email_verification:{}", email)
    }

    /// Password reset keys
    pub fn password_reset(email: &str) -> String {
        format!("password_reset:{}", email)
    }

    /// MFA challenge keys
    pub fn mfa_challenge(user_id: &str) -> String {
        format!("mfa_challenge:{}", user_id)
    }

    /// Login attempt tracking keys
    pub fn login_attempts(ip: &str) -> String {
        format!("login_attempts:{}", ip)
    }

    /// Temporary authentication token keys
    pub fn temp_token(token: &str) -> String {
        format!("temp_token:{}", token)
    }

    /// Generic key with prefix and suffix
    pub fn generic_key(prefix: &str, suffix: &str) -> String {
        format!("{}:{}", prefix, suffix)
    }

    /// Key expiration time for different use cases
    pub fn get_ttl(duration: Duration) -> u64 {
        duration.as_secs()
    }

    /// Default TTLs for different use cases
    pub const fn oauth_state_ttl() -> Duration {
        Duration::from_secs(300) // 5 minutes
    }

    pub const fn rate_limit_window() -> Duration {
        Duration::from_secs(60) // 1 minute
    }

    pub const fn email_verification_ttl() -> Duration {
        Duration::from_secs(3600) // 1 hour
    }

    pub const fn password_reset_ttl() -> Duration {
        Duration::from_secs(1800) // 30 minutes
    }

    pub const fn mfa_challenge_ttl() -> Duration {
        Duration::from_secs(300) // 5 minutes
    }

    pub const fn login_attempts_ttl() -> Duration {
        Duration::from_secs(900) // 15 minutes
    }

    pub const fn temp_token_ttl() -> Duration {
        Duration::from_secs(600) // 10 minutes
    }
}