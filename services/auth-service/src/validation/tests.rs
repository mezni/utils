#[cfg(test)]
mod validation_tests {
    
    use crate::validation::{ValidationError, validate_email, validate_password, Validator};

    #[test]
    fn test_valid_email() {
        let valid_emails = vec![
            "user@example.com",
            "test.email@domain.co.uk",
            "user+tag@domain.com",
            "user123@sub.domain.com",
            "a@b.co",
        ];
        
        for email in valid_emails {
            assert!(validate_email(email).is_ok(), "Email {} should be valid", email);
        }
    }

    #[test]
    fn test_invalid_email() {
        let invalid_emails = vec![
            "",
            "invalid-email",
            "invalid.email@",
            "@domain.com",
            "user@.com",
            "user@domain",
            "user@domain.",
            "user@domain..com",
            "user@domain.com.",
            "user@domain..com",
            "user@domain,com",
            "user domain@domain.com",
            "user@domain com",
        ];
        
        for email in invalid_emails {
            assert!(validate_email(email).is_err(), "Email {} should be invalid", email);
        }
    }

    #[test]
    fn test_valid_password() {
        let valid_passwords = vec![
            "ValidPassword123!",
            "AnotherPass456$",
            "Short1!",
            "ThisIsALongPassword123@",
            "Password123!",
        ];
        
        for password in valid_passwords {
            assert!(validate_password(password).is_ok(), "Password {} should be valid", password);
        }
    }

    #[test]
    fn test_invalid_password() {
        let test_cases = vec![
            ("", ValidationError::Required("password".to_string())),
            ("short", ValidationError::PasswordTooShort),
            ("Toolongpasswordthatexceedslimit123!", ValidationError::PasswordTooLong),
            ("nocaps123!", ValidationError::PasswordMissingUppercase),
            ("NOCAPS123!", ValidationError::PasswordMissingLowercase),
            ("NoNumbers!", ValidationError::PasswordMissingDigit),
            ("nosppecial123", ValidationError::PasswordMissingSpecial),
        ];
        
        for (password, _expected_error) in test_cases {
            assert!(validate_password(password).is_err(), "Password {} should be invalid", password);
        }
    }

    #[test]
    fn test_register_request_validation() {
        use crate::validation::register::RegisterRequest;
        
        // Test valid request
        let valid_request = RegisterRequest {
            email: "user@example.com".to_string(),
            password: "ValidPassword123!".to_string(),
        };
        assert!(valid_request.validate().is_ok());
        
        // Test invalid email
        let invalid_email = RegisterRequest {
            email: "invalid-email".to_string(),
            password: "ValidPassword123!".to_string(),
        };
        assert!(invalid_email.validate().is_err());
        
        // Test invalid password
        let invalid_password = RegisterRequest {
            email: "user@example.com".to_string(),
            password: "short".to_string(),
        };
        assert!(invalid_password.validate().is_err());
    }

    #[test]
    fn test_login_request_validation() {
        use crate::validation::login::LoginRequest;
        
        // Test valid request
        let valid_request = LoginRequest {
            email: "user@example.com".to_string(),
            password: "ValidPassword123!".to_string(),
        };
        assert!(valid_request.validate().is_ok());
        
        // Test empty email
        let empty_email = LoginRequest {
            email: "".to_string(),
            password: "ValidPassword123!".to_string(),
        };
        assert!(empty_email.validate().is_err());
        
        // Test empty password
        let empty_password = LoginRequest {
            email: "user@example.com".to_string(),
            password: "".to_string(),
        };
        assert!(empty_password.validate().is_err());
    }

    #[test]
    fn test_validation_errors() {
        use crate::validation::ValidationErrors;
        
        let mut errors = ValidationErrors::new();
        assert!(errors.is_empty());
        assert!(!errors.has_errors());
        
        errors.add_error("email", "Invalid email format".to_string());
        errors.add_error("password", "Password too short".to_string());
        
        assert!(!errors.is_empty());
        assert!(errors.has_errors());
        assert!(errors.errors.contains_key("email"));
        assert!(errors.errors.contains_key("password"));
    }
}