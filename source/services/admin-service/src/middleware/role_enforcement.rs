use actix_web::{dev::RequestHead, Error, FromRequest, HttpRequest};
use crate::middleware::auth::AuthMiddleware;
use crate::error::AuthError;

pub struct RoleEnforcement {
    pub required_roles: Vec<String>,
}

impl RoleEnforcement {
    pub fn new(roles: &[&str]) -> Self {
        Self {
            required_roles: roles.iter().map(|r| r.to_string()).collect(),
        }
    }
}

impl FromRequest for RoleEnforcement {
    type Error = Error;
    type Future = std::future::Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, head: &mut RequestHead) -> Self::Future {
        let auth_context = match extract_user_context(req) {
            Ok(context) => context,
            Err(e) => return std::future::ready(Err(e.into())),
        };

        // Extract auth middleware from request extensions
        let auth_middleware = match req.app_data::<AuthMiddleware>() {
            Some(ctx) => ctx.clone(),
            None => {
                // If no auth middleware, create with empty roles
                let user_context = crate::middleware::auth::UserContext {
                    user_id: auth_context.user_id,
                    roles: vec!["role:guest".to_string()],
                };
                AuthMiddleware::new(user_context)
            }
        };

        // Validate roles
        if let Err(e) = auth_middleware.validate_access(&auth_context) {
            return std::future::ready(Err(e.into()));
        }

        std::future::ready(Ok(RoleEnforcement {
            required_roles: auth_context.roles,
        }))
    }
}

#[cfg(test)]
mod tests {
    use actix_web::test::{call_service, TestRequest};
    use super::*;

    #[actix_web::test]
    async fn test_role_enforcement_without_roles() {
        let req = TestRequest::get()
            .insert_header((header::CONTENT_TYPE, "application/json"))
            .insert_header((header::HeaderName::from_static("X-User-Id"), "USR-12345"))
            .insert_header((header::HeaderName::from_static("X-User-Roles"), "role:partner"))
            .to_http_request();

        let result = call_service(&req, RoleEnforcement::from_request(&req, &req.head()).await);
        assert!(result.is_err());
    }

    #[actix_web::test]
    async fn test_role_enforcement_with_admin_role() {
        let req = TestRequest::get()
            .insert_header((header::CONTENT_TYPE, "application/json"))
            .insert_header((header::HeaderName::from_static("X-User-Id"), "USR-12345"))
            .insert_header((header::HeaderName::from_static("X-User-Roles"), "role:admin"))
            .to_http_request();

        let result = call_service(&req, RoleEnforcement::from_request(&req, &req.head()).await);
        assert!(result.is_ok());
    }
}
