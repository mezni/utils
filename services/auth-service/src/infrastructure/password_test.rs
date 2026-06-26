#[cfg(test)]
mod tests {
    use bornemap_auth::infrastructure::password::PasswordService;

    #[test]
    fn test_password_verify_in_isolation() {
        let password = "password123";
        let wrong_password = "wrong_password";
        
        // Hash the password
        let hash = PasswordService::hash(password).expect("Failed to hash password");
        println!("Hash: {}", hash);
        
        // Test correct password
        let valid_correct = PasswordService::verify(password, &hash).expect("Failed to verify correct password");
        assert!(valid_correct, "Correct password should be valid");
        
        // Test wrong password
        let valid_wrong = PasswordService::verify(wrong_password, &hash).expect("Failed to verify wrong password");
        assert!(!valid_wrong, "Wrong password should be invalid");
    }
}