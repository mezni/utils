use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const BASE62: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

fn deterministic_id(seed: &str, prefix: &str, length: usize) -> String {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    let mut h = hasher.finish();

    let mut out = String::with_capacity(prefix.len() + length);
    out.push_str(prefix);
    for _ in 0..length {
        out.push(BASE62[(h as usize) % 62] as char);
        h = h.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    }
    out
}

pub fn generate_partner_id(seed: &str) -> String {
    deterministic_id(seed, "PRT-", 12)
}

pub fn generate_station_id(seed: &str) -> String {
    deterministic_id(seed, "STA-", 12)
}

pub fn generate_charger_id(seed: &str) -> String {
    deterministic_id(seed, "CHR-", 12)
}

pub fn validate_partner_id(id: &str) -> bool {
    id.len() == 18 && id.starts_with("PRT-") && id[4..].chars().all(|c| c.is_ascii_alphanumeric())
}

pub fn validate_station_id(id: &str) -> bool {
    id.len() == 18 && id.starts_with("STA-") && id[4..].chars().all(|c| c.is_ascii_alphanumeric())
}

pub fn validate_charger_id(id: &str) -> bool {
    id.len() == 18 && id.starts_with("CHR-") && id[4..].chars().all(|c| c.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_id_generation() {
        let id1 = generate_partner_id("test-seed");
        let id2 = generate_partner_id("test-seed");
        assert_eq!(id1, id2);
        assert!(id1.starts_with("PRT-"));
        assert_eq!(id1.len(), 18);
    }

    #[test]
    fn test_id_validation() {
        assert!(validate_partner_id("PRT-abc123def456"));
        assert!(!validate_partner_id("ABC-123"));
        assert!(!validate_partner_id("PRT-12345678901234567"));
    }

    #[test]
    fn test_unique_ids() {
        let id1 = generate_partner_id("seed1");
        let id2 = generate_partner_id("seed2");
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_base62_charset() {
        let id = generate_partner_id("base62-test");
        let suffix = &id[4..];
        assert!(suffix.chars().all(|c| c.is_ascii_alphanumeric()));
    }
}
