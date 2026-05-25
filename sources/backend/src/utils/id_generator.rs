/// Generates URL-safe, human-readable identifiers with a typed prefix.
///
/// Format: `[PREFIX]-[12-char-lowercase-alphanumeric-nanoid]`
///
/// # Example
/// ```
/// let id = generate_id("STN");
/// assert_eq!(id.len(), 16); // "STN-" (4) + 12 chars
/// assert!(id.starts_with("STN-"));
/// ```
#[allow(dead_code)]
pub fn generate_id(prefix: &str) -> String {
    let alphabet: [char; 36] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
        'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];
    let id: String = (0..12)
        .map(|_| alphabet[fastrand::usize(..alphabet.len())])
        .collect();
    format!("{}-{}", prefix, id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_id_format() {
        let id = generate_id("USR");
        assert_eq!(id.len(), 16);
        assert!(id.starts_with("USR-"));
    }

    #[test]
    fn test_generate_id_alphabet() {
        let id = generate_id("STN");
        let suffix = &id[4..];
        assert!(suffix
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    }

    #[test]
    fn test_generate_id_unique() {
        let a = generate_id("CHG");
        let b = generate_id("CHG");
        assert_ne!(a, b);
    }
}
