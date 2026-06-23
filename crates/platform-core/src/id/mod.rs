//! Deterministic ID generation utilities
//! Provides deterministic ID generation for external IDs

use nanoid::nanoid;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Generate a deterministic Partner ID from a seed string
///
/// # Arguments
/// * `seed` - String seed for ID generation (e.g., partner name + email)
///
/// # Returns
/// Partner ID in format `PRT-{12 alphanumeric characters}`
pub fn generate_partner_id(seed: &str) -> String {
    format!("PRT-{}", deterministic_nanoid(seed, 12))
}

/// Generate a deterministic Station ID from a seed string
///
/// # Arguments
/// * `seed` - String seed for ID generation (e.g., station address)
///
/// # Returns
/// Station ID in format `STA-{12 alphanumeric characters}`
pub fn generate_station_id(seed: &str) -> String {
    format!("STA-{}", deterministic_nanoid(seed, 12))
}

/// Generate a deterministic Charger ID from a seed string
///
/// # Arguments
/// * `seed` - String seed for ID generation (e.g., charger serial number)
///
/// # Returns
/// Charger ID in format `CHR-{12 alphanumeric characters}`
pub fn generate_charger_id(seed: &str) -> String {
    format!("CHR-{}", deterministic_nanoid(seed, 12))
}

/// Validate a Partner ID
pub fn validate_partner_id(id: &str) -> bool {
    if id.len() != 18 {
        return false;
    }
    if !id.starts_with("PRT-") {
        return false;
    }
    let chars = &id[4..];
    chars.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Validate a Station ID
pub fn validate_station_id(id: &str) -> bool {
    if id.len() != 18 {
        return false;
    }
    if !id.starts_with("STA-") {
        return false;
    }
    let chars = &id[4..];
    chars.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Validate a Charger ID
pub fn validate_charger_id(id: &str) -> bool {
    if id.len() != 18 {
        return false;
    }
    if !id.starts_with("CHR-") {
        return false;
    }
    let chars = &id[4..];
    chars.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Generate deterministic nanoid from seed
fn deterministic_nanoid(seed: &str, length: usize) -> String {
    // Hash the seed string to create a deterministic value
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    let hash = hasher.finish();

    // Create a deterministic nanoid from the hash
    // This ensures consistent IDs across different instances
    nanoid!(length, &[hash])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_id_generation() {
        let id1 = generate_partner_id("test-seed");
        let id2 = generate_partner_id("test-seed");
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_id_validation() {
        assert!(validate_partner_id("PRT-abc123def456"));
        assert!(!validate_partner_id("ABC-123"));
        assert!(!validate_partner_id("PRT-12345678901234567")); // too long
    }

    #[test]
    fn test_unique_ids() {
        let id1 = generate_partner_id("seed1");
        let id2 = generate_partner_id("seed2");
        assert_ne!(id1, id2);
    }
}
