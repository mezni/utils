//! Idempotency middleware for telemetry events

use uuid::Uuid;

/// Generate a UUID v7 idempotency key for duplicate detection
///
/// UUID v7 provides time-ordered, globally unique identifiers which are
/// ideal for analytics events (better for sorting and querying).
///
/// # Returns
/// A new UUID v7
pub fn generate_idempotency_key() -> Uuid {
    Uuid::new_v7()
}

/// Check if a UUID v7 idempotency key is valid
///
/// # Returns
/// true if the UUID is valid, false otherwise
pub fn is_valid_idempotency_key(id: &str) -> bool {
    match Uuid::parse_str(id) {
        Ok(uuid) => uuid.is_v7(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_idempotency_key() {
        let key = generate_idempotency_key();
        assert_eq!(key.version(), 7);
        assert!(is_valid_idempotency_key(&key.to_string()));
    }

    #[test]
    fn test_is_valid_idempotency_key() {
        assert!(is_valid_idempotency_key(&Uuid::new_v7().to_string()));
        assert!(!is_valid_idempotency_key("invalid-uuid"));
        assert!(!is_valid_idempotency_key("00000000-0000-0000-0000-000000000000")); // v4, not v7
    }

    #[test]
    fn test_idempotency_key_uniqueness() {
        let key1 = generate_idempotency_key();
        let key2 = generate_idempotency_key();
        assert_ne!(key1, key2);
    }
}
