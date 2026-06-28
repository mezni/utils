use uuid::Uuid;

use common_auth::jwt::{self, JwtError};
use common_auth::roles::Role;

pub struct JwtService {
    secret: String,
}

impl JwtService {
    pub fn new(secret: String) -> Self {
        Self { secret }
    }

    pub fn generate_token(&self, user_id: Uuid, role: Role) -> Result<String, JwtError> {
        jwt::encode_jwt(user_id, role, &self.secret)
    }

    pub fn validate_token(&self, token: &str) -> Result<common_auth::jwt::Claims, JwtError> {
        jwt::decode_jwt(token, &self.secret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_generate_and_validate() {
        let service = JwtService::new("test-secret".to_string());
        let user_id = Uuid::new_v4();
        let token = service.generate_token(user_id, Role::Driver).unwrap();
        let claims = service.validate_token(&token).unwrap();
        assert_eq!(claims.sub, user_id);
        assert_eq!(claims.role, Role::Driver);
    }

    #[test]
    fn test_jwt_invalid_token() {
        let service = JwtService::new("test-secret".to_string());
        let result = service.validate_token("invalid-token");
        assert!(result.is_err());
    }

    #[test]
    fn test_jwt_wrong_secret() {
        let s1 = JwtService::new("secret1".to_string());
        let s2 = JwtService::new("secret2".to_string());
        let user_id = Uuid::new_v4();
        let token = s1.generate_token(user_id, Role::Admin).unwrap();
        let result = s2.validate_token(&token);
        assert!(result.is_err());
    }
}
