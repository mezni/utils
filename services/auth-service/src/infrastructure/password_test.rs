#[cfg(test)]
mod tests {
    use crate::infrastructure::password;

    #[test]
    fn test_password_verify_in_isolation() {
        let password = "password123";
        let wrong_password = "wrong_password";

        let hash = password::hash_password(password).expect("Failed to hash password");

        let valid_correct = password::verify_password(password, &hash).expect("Failed to verify correct password");
        assert!(valid_correct, "Correct password should be valid");

        let valid_wrong = password::verify_password(wrong_password, &hash).expect("Failed to verify wrong password");
        assert!(!valid_wrong, "Wrong password should be invalid");
    }
}
