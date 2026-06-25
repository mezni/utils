const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
const ID_LENGTH: usize = 12;

pub fn generate_nanoid() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..ID_LENGTH)
        .map(|_| {
            let idx = rng.gen_range(0..ALPHABET.len());
            ALPHABET[idx] as char
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nanoid_length() {
        let id = generate_nanoid();
        assert_eq!(id.len(), 12);
    }

    #[test]
    fn test_nanoid_alphabet() {
        let id = generate_nanoid();
        assert!(id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    }

    #[test]
    fn test_nanoid_unique() {
        let a = generate_nanoid();
        let b = generate_nanoid();
        assert_ne!(a, b);
    }
}
