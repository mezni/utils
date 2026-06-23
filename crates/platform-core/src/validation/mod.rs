//! Validation utilities
//! Provides common validation functions for application data

/// Validate email address format
pub fn validate_email(email: &str) -> bool {
    email.contains('@') && email.contains('.')
}

/// Validate phone number format (basic)
pub fn validate_phone(phone: &str) -> bool {
    phone.len() >= 10 && phone.len() <= 15
}

/// Validate string is not empty after trimming
pub fn validate_not_empty(s: &str) -> bool {
    !s.trim().is_empty()
}

/// Validate string length
pub fn validate_length(s: &str, min: usize, max: usize) -> bool {
    let len = s.len();
    len >= min && len <= max
}

/// Validate alphanumeric string
pub fn validate_alphanumeric(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_email_validation() {
        assert!(validate_email("test@example.com"));
        assert!(!validate_email("invalid"));
    }

    #[test]
    fn test_length_validation() {
        assert!(validate_length("test", 4, 10));
        assert!(!validate_length("tes", 4, 10));
    }
}
