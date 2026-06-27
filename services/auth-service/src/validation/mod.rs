use std::collections::HashMap;
use std::fmt;
use serde::{Deserialize, Serialize};
use regex::Regex;

#[cfg(test)]
mod tests;

pub mod login;
pub mod register;

#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    InvalidEmail,
    PasswordTooShort,
    PasswordTooLong,
    PasswordTooCommon,
    PasswordMissingUppercase,
    PasswordMissingLowercase,
    PasswordMissingDigit,
    PasswordMissingSpecial,
    PasswordMissingCharacterTypes,
    Required(String),
    InvalidField(String),
    JsonError(String),
    MalformedRequest,
    RateLimitExceeded,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::InvalidEmail => write!(f, "Email format is invalid"),
            ValidationError::PasswordTooShort => write!(f, "Password must be at least 12 characters long"),
            ValidationError::PasswordTooLong => write!(f, "Password must be at most 128 characters long"),
            ValidationError::PasswordTooCommon => write!(f, "Password is too common and easily guessable"),
            ValidationError::PasswordMissingUppercase => write!(f, "Password must contain at least one uppercase letter"),
            ValidationError::PasswordMissingLowercase => write!(f, "Password must contain at least one lowercase letter"),
            ValidationError::PasswordMissingDigit => write!(f, "Password must contain at least one digit"),
            ValidationError::PasswordMissingSpecial => write!(f, "Password must contain at least one special character"),
            ValidationError::PasswordMissingCharacterTypes => write!(f, "Password must contain character diversity"),
            ValidationError::Required(field) => write!(f, "Field '{}' is required", field),
            ValidationError::InvalidField(field) => write!(f, "Field '{}' is invalid", field),
            ValidationError::JsonError(msg) => write!(f, "JSON parsing error: {}", msg),
            ValidationError::MalformedRequest => write!(f, "Malformed request body"),
            ValidationError::RateLimitExceeded => write!(f, "Rate limit exceeded. Please try again later"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationErrors {
    pub errors: HashMap<String, Vec<String>>,
}

impl ValidationErrors {
    pub fn new() -> Self {
        Self {
            errors: HashMap::new(),
        }
    }
    
    pub fn add_error(&mut self, field: &str, error: String) {
        self.errors
            .entry(field.to_string())
            .or_default()
            .push(error);
    }
    
    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }
    
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

impl Default for ValidationErrors {
    fn default() -> Self {
        Self::new()
    }
}

pub trait Validator {
    fn validate(&self) -> Result<(), ValidationError>;
}

// Cache the email regex for performance
static EMAIL_REGEX: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$").unwrap()
});

pub fn validate_email(email: &str) -> Result<(), ValidationError> {
    if email.is_empty() {
        return Err(ValidationError::Required("email".to_string()));
    }
    
    // Check email length limits
    if email.len() > 254 {
        return Err(ValidationError::InvalidEmail);
    }
    
    // Use comprehensive email validation
    if !EMAIL_REGEX.is_match(email) {
        return Err(ValidationError::InvalidEmail);
    }
    
    // Check for email injection attempts
    let dangerous_patterns = ["<script>", "javascript:", "vbscript:", "onload=", "onerror="];
    for pattern in dangerous_patterns {
        if email.to_lowercase().contains(pattern) {
            return Err(ValidationError::InvalidEmail);
        }
    }
    
    Ok(())
}

// Common passwords that should be rejected
static COMMON_PASSWORDS: once_cell::sync::Lazy<Vec<&str>> = once_cell::sync::Lazy::new(|| {
    vec![
        "password", "123456", "12345678", "123456789", "1234567890",
        "qwerty", "abc123", "password123", "letmein", "welcome",
        "admin", "user", "root", "test", "guest", "default",
        "password1", "123123", "111111", "222222", "333333",
        "444444", "555555", "666666", "777777", "888888",
        "999999", "000000", "123321", "654321", "987654",
        "qwe123", "qazwsx", "1q2w3e4r", "1qaz2wsx", "1qazxsw2",
        "admin123", "user123", "root123", "test123", "guest123",
        "welcome123", "letmein123", "password123", "qwerty123", "abc12345",
        "iloveyou", "monkey", "sunshine", "football", "password1",
        "123456a", "a123456", "123456!", "password!", "qwerty!",
        "admin!", "user!", "root!", "test!", "guest!"
    ]
});

pub fn validate_password(password: &str) -> Result<(), ValidationError> {
    if password.is_empty() {
        return Err(ValidationError::Required("password".to_string()));
    }
    
    // Increased minimum password length
    if password.len() < 12 {
        return Err(ValidationError::PasswordTooShort);
    }
    
    if password.len() > 128 {
        return Err(ValidationError::PasswordTooLong);
    }
    
    // Check for common passwords
    let normalized_password = password.to_lowercase();
    if COMMON_PASSWORDS.contains(&normalized_password.as_str()) {
        return Err(ValidationError::PasswordTooCommon);
    }
    
    // Check for sequential characters (123, abc, etc.)
    if has_sequential_chars(password, 3) {
        return Err(ValidationError::PasswordTooCommon);
    }
    
    // Check for repeated characters (aaaa, 1111, etc.)
    if has_repeated_chars(password, 4) {
        return Err(ValidationError::PasswordTooCommon);
    }
    
    let has_uppercase = password.chars().any(|c| c.is_uppercase());
    let has_lowercase = password.chars().any(|c| c.is_lowercase());
    let has_digit = password.chars().any(|c| c.is_ascii_digit());
    let has_special = password.chars().any(|c| !c.is_alphanumeric());
    
    if !has_uppercase {
        return Err(ValidationError::PasswordMissingUppercase);
    }
    
    if !has_lowercase {
        return Err(ValidationError::PasswordMissingLowercase);
    }
    
    if !has_digit {
        return Err(ValidationError::PasswordMissingDigit);
    }
    
    if !has_special {
        return Err(ValidationError::PasswordMissingSpecial);
    }
    
    // Check for character diversity
    let character_types = [
        has_uppercase,
        has_lowercase,
        has_digit,
        has_special,
    ];
    let diversity_score = character_types.iter().filter(|&&x| x).count();
    
    if diversity_score < 3 {
        return Err(ValidationError::PasswordMissingCharacterTypes);
    }
    
    Ok(())
}

// Helper function to check for sequential characters
fn has_sequential_chars(s: &str, length: usize) -> bool {
    let chars: Vec<char> = s.chars().collect();
    
    for i in 0..chars.len().saturating_sub(length - 1) {
        let mut is_sequential = true;
        
        for j in 1..length {
            if chars[i + j] as u32 != chars[i] as u32 + j as u32 {
                is_sequential = false;
                break;
            }
        }
        
        if is_sequential {
            return true;
        }
    }
    
    false
}

// Helper function to check for repeated characters
fn has_repeated_chars(s: &str, length: usize) -> bool {
    let chars: Vec<char> = s.chars().collect();
    
    for i in 0..chars.len().saturating_sub(length - 1) {
        let mut is_repeated = true;
        
        for j in 1..length {
            if chars[i + j] != chars[i] {
                is_repeated = false;
                break;
            }
        }
        
        if is_repeated {
            return true;
        }
    }
    
    false
}