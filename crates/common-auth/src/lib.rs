use common_types::Role;

pub fn validate_token(_token: &str) -> Result<String, String> {
    Err("not implemented".to_string())
}

pub fn extract_role(_token: &str) -> Result<Role, String> {
    Err("not implemented".to_string())
}
