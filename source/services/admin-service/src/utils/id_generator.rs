use chrono::{Utc, Timelike};
use std::collections::HashMap;
use std::ops::Add;
use std::time::SystemTime;
use rand::Rng;

// Simple NanoID-like generator with type prefix
// Format: TYPE-TIMESTAMP-RANDOM
// Example: OPR-20260619-153022-a1b2c3d4e5f6

pub fn generate_id(prefix: &str) -> String {
    let timestamp = Utc::now().format("%Y%m%d%H%M%S");
    let random = generate_random_string(10);

    format!("{}-{}-{}", prefix, timestamp, random)
}

fn generate_random_string(length: usize) -> String {
    const ALPHANUMERIC: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut rng = rand::thread_rng();
    (0..length)
        .map(|_| {
            ALPHANUMERIC
                .choose(&mut rng)
                .unwrap()
                .to_ascii_lowercase()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_partner_id() {
        let id = generate_id("OPR");
        assert!(id.starts_with("OPR-"));
        assert!(id.len() > 20);
    }

    #[test]
    fn test_generate_station_id() {
        let id = generate_id("STA");
        assert!(id.starts_with("STA-"));
    }

    #[test]
    fn test_generate_charger_id() {
        let id = generate_id("CHG");
        assert!(id.starts_with("CHG-"));
    }
}
