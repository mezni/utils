use validator::Validate;

#[derive(Debug, Clone, Validate)]
pub struct PartnerValidation {
    pub name: String,
    pub network_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_phone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_email: Option<String>,
}

impl PartnerValidation {
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.name.is_empty() {
            errors.push("name is required".to_string());
        } else if self.name.len() > 255 {
            errors.push("name must be less than 255 characters".to_string());
        }

        if !self.network_type.is_empty() {
            let valid_network_types = ["individual", "individuals", "company"];
            if !valid_network_types.contains(&self.network_type.to_lowercase().as_str()) {
                errors.push(format!("invalid network_type value. Valid values: INDIVIDUAL, COMPANY"));
            }
        }

        if let Some(phone) = &self.support_phone {
            if phone.len() > 50 {
                errors.push("support_phone must be less than 50 characters".to_string());
            }
        }

        if let Some(email) = &self.support_email {
            if !email.is_email() {
                errors.push("support_email must be a valid email address".to_string());
            } else if email.len() > 255 {
                errors.push("support_email must be less than 255 characters".to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

pub fn validate_partner(request: &crate::CreatePartnerRequest) -> Result<(), Vec<String>> {
    let validation = PartnerValidation {
        name: request.name.clone(),
        network_type: request.network_type.to_string(),
        support_phone: request.support_phone.clone(),
        support_email: request.support_email.clone(),
    };

    validation.validate()
}
