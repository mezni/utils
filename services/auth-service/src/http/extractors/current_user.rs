use crate::infrastructure::jwt::JwtService;
use actix_web::{dev::Payload, error, Error, FromRequest, HttpRequest, web};
use bornemap_core::{AppError, UserId};
use bornemap_auth::jwt_validator::ValidatedClaims;
use bornemap_auth::rbac::Role;
use futures::future::{ready, Ready};

#[derive(Debug, Clone)]
pub struct CurrentUser {
    pub user_id: UserId,
    pub role: Role,
    pub claims: ValidatedClaims,
}

impl CurrentUser {
    pub fn new(claims: ValidatedClaims) -> Result<Self, AppError> {
        let user_id = claims.user_id()?;
        let role = Role::try_from_str(&claims.role)
            .ok_or_else(|| AppError::InvalidConfiguration(format!("Invalid role in token: {}", claims.role)))?;

        Ok(Self {
            user_id,
            role,
            claims,
        })
    }

    pub fn is_admin(&self) -> bool {
        self.role == Role::Admin
    }

    pub fn is_partner(&self) -> bool {
        self.role == Role::Partner
    }

    pub fn is_driver(&self) -> bool {
        self.role == Role::RegisteredDriver
    }

    pub fn is_system(&self) -> bool {
        false // System role no longer exists
    }
}

impl FromRequest for CurrentUser {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let jwt_service_data = req.app_data::<web::Data<crate::infrastructure::jwt_service::AuthJwtService>>();

        let authorization_header = req.headers()
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| error::ErrorUnauthorized("Missing Authorization header"))
            .map_err(Error::from);

        let token = match authorization_header {
            Ok(header) => header
                .strip_prefix("Bearer ")
                .ok_or_else(|| error::ErrorUnauthorized("Invalid Authorization header format")),
            Err(e) => Err(e),
        };

        let token_str = match token {
            Ok(t) => t,
            Err(e) => return ready(Err(e)),
        };

        let validator = match jwt_service_data
            .as_ref()
            .ok_or_else(|| error::ErrorInternalServerError("JWT service not configured"))
        {
            Ok(v) => v.validator(),
            Err(e) => return ready(Err(e)),
        };

        let claims = match validator.validate(token_str) {
            Ok(claims) => claims,
            Err(e) => {
                let error = match e {
                    AppError::TokenExpired => error::ErrorUnauthorized("Token expired"),
                    AppError::InvalidToken => error::ErrorUnauthorized("Invalid token"),
                    AppError::InvalidSignature => error::ErrorUnauthorized("Invalid signature"),
                    AppError::InvalidIssuer => error::ErrorUnauthorized("Invalid token issuer"),
                    AppError::InvalidAudience => error::ErrorUnauthorized("Invalid token audience"),
                    _ => error::ErrorUnauthorized("Invalid token"),
                };
                return ready(Err(error));
            }
        };

        let current_user = match CurrentUser::new(claims) {
            Ok(user) => user,
            Err(e) => {
                let error = match e {
                    AppError::InvalidConfiguration(_) => error::ErrorInternalServerError("Invalid token configuration"),
                    _ => error::ErrorUnauthorized("Invalid token"),
                };
                return ready(Err(error));
            }
        };

        ready(Ok(current_user))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;
    use bornemap_auth::jwt_validator::JwtConfig;
    use bornemap_auth::rbac::Role;
    use uuid::Uuid;

    fn create_test_jwt_service() -> JwtService {
        JwtService::new(
            "test-secret-key".to_string(),
            3600,
            "bornemap".to_string(),
            "bornemap-app".to_string(),
        )
    }

    #[test]
    fn current_user_creation_with_valid_claims() {
        let claims = ValidatedClaims {
            sub: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            role: "ADMIN".to_string(),
            iat: chrono::Utc::now(),
            exp: chrono::Utc::now() + chrono::Duration::hours(1),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let result = CurrentUser::new(claims);
        assert!(result.is_ok());
        let user = result.unwrap();
        assert_eq!(user.user_id, Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());
        assert_eq!(user.role, Role::Admin);
    }

    #[test]
    fn current_user_creation_with_invalid_role() {
        let claims = ValidatedClaims {
            sub: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            role: "INVALID_ROLE".to_string(),
            iat: chrono::Utc::now(),
            exp: chrono::Utc::now() + chrono::Duration::hours(1),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let result = CurrentUser::new(claims);
        assert!(matches!(result, Err(AppError::InvalidConfiguration(_))));
    }

    #[test]
    fn current_user_creation_with_invalid_user_id() {
        let claims = ValidatedClaims {
            sub: "invalid-uuid".to_string(),
            role: "ADMIN".to_string(),
            iat: chrono::Utc::now(),
            exp: chrono::Utc::now() + chrono::Duration::hours(1),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let result = CurrentUser::new(claims);
        assert!(matches!(result, Err(AppError::InvalidToken)));
    }

    #[test]
    fn current_user_role_checkers() {
        let claims = ValidatedClaims {
            sub: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            role: "ADMIN".to_string(),
            iat: chrono::Utc::now(),
            exp: chrono::Utc::now() + chrono::Duration::hours(1),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let user = CurrentUser::new(claims).unwrap();
        assert!(user.is_admin());
        assert!(!user.is_partner());
        assert!(!user.is_driver());
        assert!(!user.is_system());
    }

    #[test]
    fn current_user_partner_role_checkers() {
        let claims = ValidatedClaims {
            sub: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            role: "PARTNER".to_string(),
            iat: chrono::Utc::now(),
            exp: chrono::Utc::now() + chrono::Duration::hours(1),
            iss: "bornemap".to_string(),
            aud: "bornemap-app".to_string(),
            jti: "jti-123".to_string(),
        };

        let user = CurrentUser::new(claims).unwrap();
        assert!(!user.is_admin());
        assert!(user.is_partner());
        assert!(!user.is_driver());
        assert!(!user.is_system());
    }
}
