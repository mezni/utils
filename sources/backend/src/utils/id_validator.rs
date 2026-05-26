#![allow(dead_code)]

pub fn validate_semantic_id(id: &str) -> Result<(), String> {
    let parts: Vec<&str> = id.splitn(2, '-').collect();
    if parts.len() != 2 {
        return Err(format!(
            "Invalid ID format '{}': expected [PREFIX]-[12 lowercase alphanumeric chars]",
            id
        ));
    }
    let prefix = parts[0];
    let suffix = parts[1];

    let valid_prefixes = ["USR", "PRT", "STN", "CHG", "CNT"];
    if !valid_prefixes.contains(&prefix) {
        return Err(format!(
            "Invalid ID prefix '{}': expected one of USR, PRT, STN, CHG, CNT",
            prefix
        ));
    }

    if suffix.len() != 12 {
        return Err(format!(
            "Invalid ID suffix length for '{}': expected 12 characters, got {}",
            id,
            suffix.len()
        ));
    }

    if !suffix.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()) {
        return Err(format!(
            "Invalid ID suffix '{}': must be lowercase alphanumeric",
            id
        ));
    }

    Ok(())
}

pub fn validate_id_prefix(id: &str, expected_prefix: &str) -> Result<(), String> {
    validate_semantic_id(id)?;
    let actual_prefix = id.split('-').next().unwrap_or("");
    if actual_prefix != expected_prefix {
        return Err(format!(
            "Expected ID with prefix '{}', got '{}'",
            expected_prefix, actual_prefix
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_ids() {
        assert!(validate_semantic_id("USR-m1k9p2v4x7q3").is_ok());
        assert!(validate_semantic_id("STN-k4m2n9p1q5v8").is_ok());
        assert!(validate_semantic_id("CHG-a1b2c3d4e5f6").is_ok());
        assert!(validate_semantic_id("CNT-0123456789ab").is_ok());
        assert!(validate_semantic_id("PRT-xy1z2a3b4c5d").is_ok());
    }

    #[test]
    fn test_invalid_ids() {
        assert!(validate_semantic_id("").is_err());
        assert!(validate_semantic_id("USR-abc").is_err());
        assert!(validate_semantic_id("USR-ABCDEF123456").is_err());
        assert!(validate_semantic_id("XYZ-m1k9p2v4x7q3").is_err());
        assert!(validate_semantic_id("USR-m1k9p2v4x7q3!").is_err());
        assert!(validate_semantic_id("user-123456789012").is_err());
    }

    #[test]
    fn test_id_prefix() {
        assert!(validate_id_prefix("USR-m1k9p2v4x7q3", "USR").is_ok());
        assert!(validate_id_prefix("STN-k4m2n9p1q5v8", "STN").is_ok());
        assert!(validate_id_prefix("CHG-a1b2c3d4e5f6", "STN").is_err());
    }
}
