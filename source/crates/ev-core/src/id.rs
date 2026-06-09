const DEFAULT_ALPHABET: &[char] = &[
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9',
];

/// Generates a NanoID with the given alphanumeric prefix and random character length
/// using the default URL-safe alphabet (A-Z, a-z, 0-9).
///
/// # Panics
///
/// Panics if `length` is 0.
///
/// # Example
///
/// ```
/// use ev_core::generate_id;
/// let id = generate_id("PRT", 8);
/// assert!(id.starts_with("PRT"));
/// assert_eq!(id.len(), 11); // "PRT" + 8 random chars
/// ```
pub fn generate_id(prefix: &str, length: usize) -> String {
    assert!(length > 0, "length must be greater than 0");
    let random_part = nanoid::nanoid!(length, &DEFAULT_ALPHABET);
    format!("{}{}", prefix, random_part)
}

/// Generates a NanoID with a custom alphabet.
///
/// # Panics
///
/// Panics if `length` is 0 or `alphabet` has fewer than 2 characters.
///
/// # Example
///
/// ```
/// use ev_core::generate_id_with_alphabet;
/// let id = generate_id_with_alphabet("STN", 6, &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']);
/// assert!(id.starts_with("STN"));
/// assert_eq!(id.len(), 9); // "STN" + 6 digits
/// ```
pub fn generate_id_with_alphabet(prefix: &str, length: usize, alphabet: &[char]) -> String {
    assert!(length > 0, "length must be greater than 0");
    assert!(alphabet.len() >= 2, "alphabet must have at least 2 characters");
    let random_part = nanoid::nanoid!(length, alphabet);
    format!("{}{}", prefix, random_part)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn generate_id_has_correct_prefix() {
        let id = generate_id("PRT", 8);
        assert!(id.starts_with("PRT"));
    }

    #[test]
    fn generate_id_has_correct_length() {
        let id = generate_id("PRT", 8);
        assert_eq!(id.len(), 11); // "PRT" + 8 chars
    }

    #[test]
    fn generate_id_1000_unique_ids() {
        let mut ids = HashSet::new();
        for _ in 0..1000 {
            let id = generate_id("PRT", 8);
            assert!(ids.insert(id), "collision detected");
        }
    }

    #[test]
    fn generate_id_empty_prefix() {
        let id = generate_id("", 8);
        assert_eq!(id.len(), 8);
    }

    #[test]
    fn generate_id_custom_alphabet() {
        let digits: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];
        let id = generate_id_with_alphabet("STN", 6, digits);
        assert!(id.starts_with("STN"));
        assert_eq!(id.len(), 9);
        // All chars after prefix should be digits
        for c in id.chars().skip(3) {
            assert!(c.is_ascii_digit(), "non-digit found: {}", c);
        }
    }

    #[test]
    #[should_panic(expected = "length must be greater than 0")]
    fn generate_id_zero_length_panics() {
        generate_id("PRT", 0);
    }

    #[test]
    #[should_panic(expected = "alphabet must have at least 2 characters")]
    fn generate_id_single_char_alphabet_panics() {
        generate_id_with_alphabet("PRT", 8, &['A']);
    }
}
