#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::register::{RegisterUseCase, RegisterRequest, AuthResponse};
    use crate::infrastructure::jwt::JwtService;
    use crate::infrastructure::password::PasswordService;
    use bornemap_core::{User, UserRepository, AuthError, UserRole, UserStatus};
    use chrono::Utc;
    use uuid::Uuid;
    use std::collections::HashMap;
    use async_trait::async_trait;

    // Mock user repository for testing
    struct MockUserRepository {
        users: HashMap<String, User>,
        should_fail: bool,
    }

    impl MockUserRepository {
        fn new() -> Self {
            Self {
                users: HashMap::new(),
                should_fail: false,
            }
        }

        fn set_fail(&mut self, fail: bool) {
            self.should_fail = fail;
        }

        fn add_user(&mut self, email: &str, password_hash: &str) {
            let user = User {
                id: Uuid::new_v4(),
                email: email.to_string(),
                password_hash: password_hash.to_string(),
                role: UserRole::RegisteredDriver,
                status: UserStatus::Active,
                created_at: Utc::now(),
            };
            self.users.insert(email.to_lowercase(), user);
        }
    }

    #[async_trait]
    impl UserRepository for MockUserRepository {
        async fn create(&self, user: &User) -> Result<(), AuthError> {
            if self.should_fail {
                return Err(AuthError::InternalError);
            }
            Ok(())
        }

        async fn find_by_email(&self, email: &str) -> Result<Option<User>, AuthError> {
            Ok(self.users.get(email).cloned())
        }

        async fn find_by_id(&self, id: UserId) -> Result<Option<User>, AuthError> {
            // Find user by ID (simplified for testing)
            for user in self.users.values() {
                if user.id == id {
                    return Ok(Some(user.clone()));
                }
            }
            Ok(None)
        }

        async fn email_exists(&self, email: &str) -> Result<bool, AuthError> {
            Ok(self.users.contains_key(&email.to_lowercase()))
        }
    }

    #[tokio::test]
    async fn test_successful_registration() {
        let mut repo = MockUserRepository::new();
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let use_case = RegisterUseCase::new(repo, jwt_service);

        let request = RegisterRequest {
            email: "test@example.com".to_string(),
            password: "ValidPassword123!".to_string(),
        };

        let result = use_case.execute(request).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(!response.access_token.is_empty());
        assert_eq!(response.token_type, "Bearer");
        assert_eq!(response.expires_in, 86400);
    }

    #[tokio::test]
    async fn test_duplicate_email_registration() {
        let mut repo = MockUserRepository::new();
        // Add a user with the same email
        repo.add_user("test@example.com", "existing_hash");
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let use_case = RegisterUseCase::new(repo, jwt_service);

        let request = RegisterRequest {
            email: "test@example.com".to_string(),
            password: "ValidPassword123!".to_string(),
        };

        let result = use_case.execute(request).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::EmailAlreadyExists)));
    }

    #[tokio::test]
    async fn test_repository_failure() {
        let mut repo = MockUserRepository::new();
        repo.set_fail(true); // Simulate repository failure
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let use_case = RegisterUseCase::new(repo, jwt_service);

        let request = RegisterRequest {
            email: "test@example.com".to_string(),
            password: "ValidPassword123!".to_string(),
        };

        let result = use_case.execute(request).await;

        assert!(result.is_err());
        // The exact error might depend on how the repository failure is handled
        // In our implementation, it would be AuthError::InternalError
    }

    #[tokio::test]
    async fn test_email_case_insensitive() {
        let mut repo = MockUserRepository::new();
        repo.add_user("test@example.com", "existing_hash");
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let use_case = RegisterUseCase::new(repo, jwt_service);

        let request = RegisterRequest {
            email: "TEST@example.com".to_string(), // Different case
            password: "ValidPassword123!".to_string(),
        };

        let result = use_case.execute(request).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::EmailAlreadyExists)));
    }

    #[test]
    fn test_password_hashing() {
        let password = "TestPassword123!";
        let hash = PasswordService::hash(password).expect("Failed to hash password");
        assert!(!hash.is_empty());
        assert!(hash.starts_with("$argon2id$"));
        
        // Verify the hash
        let is_valid = PasswordService::verify(password, &hash).expect("Failed to verify password");
        assert!(is_valid);
        
        // Verify with wrong password
        let is_valid_wrong = PasswordService::verify("wrongpassword", &hash).expect("Failed to verify password");
        assert!(!is_valid_wrong);
    }

    #[test]
    fn test_jwt_token_generation() {
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let token = jwt_service.generate_token("user-id-123", "REGISTERED_DRIVER")
            .expect("Failed to generate token");
        
        assert!(!token.is_empty());
        assert!(token.contains('.')); // JWT tokens have dots
    }

    #[test]
    fn test_jwt_token_verification() {
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        
        // Generate a token
        let token = jwt_service.generate_token("user-id-123", "REGISTERED_DRIVER")
            .expect("Failed to generate token");
        
        // Verify the token
        let claims = jwt_service.verify_token(&token)
            .expect("Failed to verify token");
        
        assert_eq!(claims.sub, "user-id-123");
        assert_eq!(claims.role, "REGISTERED_DRIVER");
    }

    #[test]
    fn test_invalid_jwt_token() {
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        
        // Test with invalid token
        let result = jwt_service.verify_token("invalid.token.here");
        assert!(result.is_err());
    }

    #[test]
    fn test_different_signature_jwt_tokens() {
        let jwt_service1 = JwtService::new("secret1".to_string(), 3600, "test-issuer", "test-audience");
        let jwt_service2 = JwtService::new("secret2".to_string(), 3600, "test-issuer", "test-audience");
        
        // Generate token with first secret
        let token1 = jwt_service1.generate_token("user-id-123", "REGISTERED_DRIVER")
            .expect("Failed to generate token");
        
        // Try to verify with second secret (should fail)
        let result = jwt_service2.verify_token(&token1);
        assert!(result.is_err());
    }
}