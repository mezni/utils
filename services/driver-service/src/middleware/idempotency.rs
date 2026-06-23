//! Idempotency middleware for telemetry events

use uuid::Uuid;

/// Generate a UUID v4 idempotency key for duplicate detection
///
/// # Returns
/// A new UUID v4
pub fn generate_idempotency_key() -> Uuid {
    Uuid::new_v4()
}

/// Check if a UUID idempotency key is valid
///
/// # Returns
/// true if the UUID is valid, false otherwise
pub fn is_valid_idempotency_key(id: &str) -> bool {
    Uuid::parse_str(id).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_idempotency_key() {
        let key = generate_idempotency_key();
        assert_eq!(key.get_version(), Some(uuid::Version::Random));
        assert!(is_valid_idempotency_key(&key.to_string()));
    }

    #[test]
    fn test_is_valid_idempotency_key() {
        assert!(is_valid_idempotency_key(&Uuid::new_v4().to_string()));
        assert!(!is_valid_idempotency_key("invalid-uuid"));
    }

    #[test]
    fn test_idempotency_key_uniqueness() {
        let key1 = generate_idempotency_key();
        let key2 = generate_idempotency_key();
        assert_ne!(key1, key2);
    }
}
