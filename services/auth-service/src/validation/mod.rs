use std::collections::HashMap;
use std::fmt;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

pub mod login;
pub mod register;

#[derive(Debug, Clone)]
pub enum ValidationError {
    InvalidEmail,
    PasswordTooShort,
    PasswordTooLong,
    PasswordMissingUppercase,
    PasswordMissingLowercase,
    PasswordMissingDigit,
    PasswordMissingSpecial,
    Required(String),
    InvalidField(String),
    JsonError(String),
    MalformedRequest,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::InvalidEmail => write!(f, "Email format is invalid"),
            ValidationError::PasswordTooShort => write!(f, "Password must be at least 8 characters long"),
            ValidationError::PasswordTooLong => write!(f, "Password must be at most 128 characters long"),
            ValidationError::PasswordMissingUppercase => write!(f, "Password must contain at least one uppercase letter"),
            ValidationError::PasswordMissingLowercase => write!(f, "Password must contain at least one lowercase letter"),
            ValidationError::PasswordMissingDigit => write!(f, "Password must contain at least one digit"),
            ValidationError::PasswordMissingSpecial => write!(f, "Password must contain at least one special character"),
            ValidationError::Required(field) => write!(f, "Field '{}' is required", field),
            ValidationError::InvalidField(field) => write!(f, "Field '{}' is invalid", field),
            ValidationError::JsonError(msg) => write!(f, "JSON parsing error: {}", msg),
            ValidationError::MalformedRequest => write!(f, "Malformed request body"),
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

pub fn validate_email(email: &str) -> Result<(), ValidationError> {
    if email.is_empty() {
        return Err(ValidationError::Required("email".to_string()));
    }
    
    // Basic email format validation
    if !email.contains('@') || !email.contains('.') {
        return Err(ValidationError::InvalidEmail);
    }
    
    // Check for common invalid patterns
    if email.starts_with('@') || email.ends_with('@') || email.starts_with('.') || email.ends_with('.') {
        return Err(ValidationError::InvalidEmail);
    }
    
    // Check for multiple @ symbols
    if email.matches('@').count() > 1 {
        return Err(ValidationError::InvalidEmail);
    }
    
    Ok(())
}

pub fn validate_password(password: &str) -> Result<(), ValidationError> {
    if password.is_empty() {
        return Err(ValidationError::Required("password".to_string()));
    }
    
    if password.len() < 8 {
        return Err(ValidationError::PasswordTooShort);
    }
    
    if password.len() > 128 {
        return Err(ValidationError::PasswordTooLong);
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
    
    Ok(())
}