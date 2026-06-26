use super::ValidationError;
use super::Validator;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

impl Validator for LoginRequest {
    fn validate(&self) -> Result<(), ValidationError> {
        // Validate email presence
        if self.email.is_empty() {
            return Err(ValidationError::Required("email".to_string()));
        }

        // Validate password presence
        if self.password.is_empty() {
            return Err(ValidationError::Required("password".to_string()));
        }

        Ok(())
    }
}

impl LoginRequest {
    pub fn new(email: String, password: String) -> Self {
        Self { email, password }
    }

    pub fn email(&self) -> &str {
        &self.email
    }

    pub fn password(&self) -> &str {
        &self.password
    }
}
