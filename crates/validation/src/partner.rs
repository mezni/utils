pub fn validate_email(email: &str) -> Result<(), String> {
    if email.is_empty() {
        return Err("email cannot be empty".into());
    }
    if !email.contains('@') {
        return Err("invalid email format".into());
    }
    Ok(())
}
