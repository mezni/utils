use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Email {
    value: String,
}

impl Email {
    /// Create a new Email with validation
    pub fn new(value: String) -> Result<Self, String> {
        Self::validate(&value)?;
        Ok(Email { value })
    }

    /// Validate email format
    fn validate(value: &str) -> Result<(), String> {
        if value.is_empty() {
            return Err("Email cannot be empty".to_string());
        }

        let email_regex = regex::Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap();

        if !email_regex.is_match(value) {
            return Err(format!("Invalid email format: {}", value));
        }

        if value.len() > 254 {
            return Err("Email exceeds maximum length of 254 characters".to_string());
        }

        Ok(())
    }

    /// Get the email value
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for Email {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PasswordHash {
    value: String,
    algorithm: String,
    cost: u32,
}

impl PasswordHash {
    /// Create a new password hash using Argon2id
    pub fn new(hash: String, algorithm: String, cost: u32) -> Self {
        PasswordHash {
            value: hash,
            algorithm,
            cost,
        }
    }

    /// Verify a password against this hash
    pub fn verify(&self, password: &str) -> Result<(), argon2::password_hash::Error> {
        let params = argon2::Params::new(self.cost, None, None, None)?;
        let salt = argon2::password_hash::SaltString::from_b64(&self.value.split('$').nth(3).unwrap_or(""))?;

        argon2::PasswordHash::verify_password(password.as_bytes(), &salt, &self.value)?;

        Ok(())
    }

    /// Get the hash value
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for PasswordHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_email_creation() {
        let email = Email::new("test@example.com".to_string()).unwrap();
        assert_eq!(email.as_str(), "test@example.com");
    }

    #[test]
    fn test_invalid_email_creation() {
        assert!(Email::new("invalid-email".to_string()).is_err());
        assert!(Email::new("test@example".to_string()).is_err());
        assert!(Email::new("".to_string()).is_err());
    }

    #[test]
    fn test_email_to_string() {
        let email = Email::new("user@example.com".to_string()).unwrap();
        assert_eq!(email.to_string(), "user@example.com");
    }
}