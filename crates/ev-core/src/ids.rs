use nanoid::nanoid;

const ALPHABET: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

fn generate_id(prefix: &str, size: usize) -> String {
    format!("{}_{}", prefix, nanoid!(size, ALPHABET))
}

pub fn new_usr() -> String {
    generate_id("USR", 16)
}

pub fn new_prt() -> String {
    generate_id("PRT", 16)
}

pub fn new_stn() -> String {
    generate_id("STN", 16)
}

pub fn new_chg() -> String {
    generate_id("CHG", 16)
}

pub fn new_rev() -> String {
    generate_id("REV", 16)
}

pub fn new_evt() -> String {
    generate_id("EVT", 16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_usr_prefix() {
        let id = new_usr();
        assert!(id.starts_with("USR_"));
    }

    #[test]
    fn test_new_prt_prefix() {
        let id = new_prt();
        assert!(id.starts_with("PRT_"));
    }

    #[test]
    fn test_new_stn_prefix() {
        let id = new_stn();
        assert!(id.starts_with("STN_"));
    }

    #[test]
    fn test_new_chg_prefix() {
        let id = new_chg();
        assert!(id.starts_with("CHG_"));
    }

    #[test]
    fn test_new_rev_prefix() {
        let id = new_rev();
        assert!(id.starts_with("REV_"));
    }

    #[test]
    fn test_new_evt_prefix() {
        let id = new_evt();
        assert!(id.starts_with("EVT_"));
    }

    #[test]
    fn test_ids_are_unique() {
        let ids: std::collections::HashSet<String> = (0..100).map(|_| new_usr()).collect();
        assert_eq!(ids.len(), 100);
    }

    #[test]
    fn test_ids_have_correct_length() {
        let id = new_usr();
        let suffix = id.strip_prefix("USR_").unwrap();
        assert_eq!(suffix.len(), 16);
    }

    #[test]
    fn test_alphabet_does_not_contain_confusable() {
        let id = new_usr();
        let suffix = id.strip_prefix("USR_").unwrap();
        assert!(!suffix.contains('0'), "contains '0'");
        assert!(!suffix.contains('O'), "contains 'O'");
        assert!(!suffix.contains('o'), "contains 'o'");
        assert!(!suffix.contains('l'), "contains 'l'");
        assert!(!suffix.contains('I'), "contains 'I'");
    }
}
