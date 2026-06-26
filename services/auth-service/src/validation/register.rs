use super::ValidationError;
use super::{Validator, validate_email, validate_password};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
}

impl Validator for RegisterRequest {
    fn validate(&self) -> Result<(), ValidationError> {
        // Validate email
        validate_email(&self.email)?;

        // Validate password
        validate_password(&self.password)?;

        Ok(())
    }
}

impl RegisterRequest {
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
