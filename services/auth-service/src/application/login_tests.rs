#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::login::{LoginUseCase, LoginRequest};
    use crate::infrastructure::jwt::JwtService;
    use crate::infrastructure::pg_session_repo::MockSessionRepository;
    use crate::infrastructure::pg_user_repo::MockUserRepository;
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

        fn add_user_with_status(&mut self, email: &str, password_hash: &str, status: UserStatus) {
            let user = User {
                id: Uuid::new_v4(),
                email: email.to_string(),
                password_hash: password_hash.to_string(),
                role: UserRole::RegisteredDriver,
                status,
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
            Ok(self.users.get(&email.to_lowercase()).cloned())
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
    async fn test_successful_login() {
        let mut user_repo = MockUserRepository::new();
        let session_repo = MockSessionRepository::new();
        
        // Add a test user
        let password_hash = PasswordService::hash("correct_password").expect("Failed to hash password");
        user_repo.add_user("test@example.com", &password_hash);
        
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let use_case = LoginUseCase::new(
            user_repo,
            session_repo,
            jwt_service,
            86400, // 1 day refresh TTL
        );

        let request = LoginRequest {
            email: "test@example.com".to_string(),
            password: "correct_password".to_string(),
        };

        let result = use_case.execute(request).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(!response.access_token.is_empty());
        assert!(!response.refresh_token.is_empty());
        assert_eq!(response.token_type, "Bearer");
        assert_eq!(response.expires_in, 86400);
    }

    #[tokio::test]
    async fn test_wrong_password() {
        let mut user_repo = MockUserRepository::new();
        let session_repo = MockSessionRepository::new();
        
        // Add a test user
        let password_hash = PasswordService::hash("correct_password").expect("Failed to hash password");
        user_repo.add_user("test@example.com", &password_hash);
        
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let use_case = LoginUseCase::new(
            user_repo,
            session_repo,
            jwt_service,
            86400,
        );

        let request = LoginRequest {
            email: "test@example.com".to_string(),
            password: "wrong_password".to_string(),
        };

        let result = use_case.execute(request).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[tokio::test]
    async fn test_unknown_user() {
        let user_repo = MockUserRepository::new();
        let session_repo = MockSessionRepository::new();
        
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let use_case = LoginUseCase::new(
            user_repo,
            session_repo,
            jwt_service,
            86400,
        );

        let request = LoginRequest {
            email: "unknown@example.com".to_string(),
            password: "any_password".to_string(),
        };

        let result = use_case.execute(request).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[tokio::test]
    async fn test_suspended_user() {
        let mut user_repo = MockUserRepository::new();
        let session_repo = MockSessionRepository::new();
        
        // Add a suspended user
        let password_hash = PasswordService::hash("correct_password").expect("Failed to hash password");
        user_repo.add_user_with_status("test@example.com", &password_hash, UserStatus::Suspended);
        
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let use_case = LoginUseCase::new(
            user_repo,
            session_repo,
            jwt_service,
            86400,
        );

        let request = LoginRequest {
            email: "test@example.com".to_string(),
            password: "correct_password".to_string(),
        };

        let result = use_case.execute(request).await;

        assert!(result.is_err());
        // Suspended users should be treated as invalid credentials
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[tokio::test]
    async fn test_email_case_insensitive_login() {
        let mut user_repo = MockUserRepository::new();
        let session_repo = MockSessionRepository::new();
        
        // Add a test user
        let password_hash = PasswordService::hash("correct_password").expect("Failed to hash password");
        user_repo.add_user("test@example.com", &password_hash);
        
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let use_case = LoginUseCase::new(
            user_repo,
            session_repo,
            jwt_service,
            86400,
        );

        let request = LoginRequest {
            email: "TEST@example.com".to_string(), // Different case
            password: "correct_password".to_string(),
        };

        let result = use_case.execute(request).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(!response.access_token.is_empty());
        assert!(!response.refresh_token.is_empty());
    }

    #[tokio::test]
    async fn test_repository_failure() {
        let mut user_repo = MockUserRepository::new();
        let session_repo = MockSessionRepository::new();
        
        // Add a test user
        let password_hash = PasswordService::hash("correct_password").expect("Failed to hash password");
        user_repo.add_user("test@example.com", &password_hash);
        
        // Make user repository fail
        user_repo.set_fail(true);
        
        let jwt_service = JwtService::new("test-secret".to_string(), 3600, "test-issuer", "test-audience");
        let use_case = LoginUseCase::new(
            user_repo,
            session_repo,
            jwt_service,
            86400,
        );

        let request = LoginRequest {
            email: "test@example.com".to_string(),
            password: "correct_password".to_string(),
        };

        let result = use_case.execute(request).await;

        assert!(result.is_err());
        // The exact error might depend on how the repository failure is handled
        // In our implementation, it would be AuthError::InternalError
    }

    #[test]
    fn test_password_verification_edge_cases() {
        let test_cases = vec![
            ("CorrectPassword123!", true),
            ("wrongpassword", false),
            ("", false), // Empty password
            ("short", false), // Too short
            ("Toolongpasswordthatexceedslimit123!", false), // Too long
            ("nocaps123!", false), // No uppercase
            ("NOCAPS123!", false), // No lowercase
            ("NoNumbers!", false), // No numbers
            ("nosppecial123", false), // No special character
        ];

        for (password, should_be_valid) in test_cases {
            let hash = PasswordService::hash("CorrectPassword123!").expect("Failed to hash password");
            let result = PasswordService::verify(password, &hash).expect("Failed to verify password");
            assert_eq!(result, should_be_valid, "Password: {}", password);
        }
    }
}