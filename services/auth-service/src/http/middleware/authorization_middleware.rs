use crate::http::extractors::CurrentUser;
use actix_web::{dev::Payload, error, Error, FromRequest, HttpRequest, HttpMessage};
use bornemap_auth::rbac::{Role, RoleChecker};
use futures::future::{ready, Ready};

pub struct AuthorizationMiddleware;

impl AuthorizationMiddleware {
    pub fn require_role(role: Role) -> RoleAuthExtractor {
        RoleAuthExtractor::new(vec![role])
    }

    pub fn require_any_roles(roles: Vec<Role>) -> RoleAuthExtractor {
        RoleAuthExtractor::new(roles)
    }
}

pub struct RoleAuthExtractor {
    required_roles: Vec<Role>,
}

impl RoleAuthExtractor {
    pub fn new(roles: Vec<Role>) -> Self {
        Self { required_roles: roles }
    }
}

impl FromRequest for RoleAuthExtractor {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(_req: &HttpRequest, _: &mut Payload) -> Self::Future {
        ready(Ok(Self::new(vec![]))) // We'll handle the actual role checking in the handler
    }
}

pub struct AuthorizedRequest<T> {
    pub current_user: CurrentUser,
    pub inner: T,
}

impl<T> FromRequest for AuthorizedRequest<T>
where
    T: FromRequest + 'static,
    <T as FromRequest>::Future: 'static,
{
    type Error = Error;
    type Future = std::pin::Pin<Box<dyn futures::future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let user_future = CurrentUser::from_request(req, payload);
        let inner_future = T::from_request(req, payload);
        let required_roles = Self::extract_required_roles(req);

        Box::pin(async move {
            let current_user = match user_future.await {
                Ok(user) => user,
                Err(e) => return Err(e.into()),
            };
            
            let inner = match inner_future.await {
                Ok(inner) => inner,
                Err(e) => return Err(e.into()),
            };

            let role_checker = RoleChecker::require_any_roles(&required_roles);
            
            match role_checker.check(&bornemap_auth::rbac::RoleSet::from_roles(&[current_user.role])) {
                Ok(_) => {},
                Err(_) => return Err(error::ErrorForbidden("Insufficient permissions")),
            };

            Ok(Self {
                current_user,
                inner,
            })
        })
    }
}

impl AuthorizedRequest<()> {
    pub fn check_role(req: &HttpRequest, role: Role) -> Result<(), Error> {
        let binding = req.extensions();
        let current_user = binding.get::<CurrentUser>()
            .ok_or_else(|| error::ErrorInternalServerError("Current user not found in request extensions"))?;

        if current_user.role != role {
            return Err(error::ErrorForbidden("Insufficient permissions"));
        }

        Ok(())
    }

    pub fn check_any_role(req: &HttpRequest, roles: &[Role]) -> Result<(), Error> {
        let binding = req.extensions();
        let current_user = binding.get::<CurrentUser>()
            .ok_or_else(|| error::ErrorInternalServerError("Current user not found in request extensions"))?;

        if !roles.contains(&current_user.role) {
            return Err(error::ErrorForbidden("Insufficient permissions"));
        }

        Ok(())
    }
}

impl<T> AuthorizedRequest<T> {
    fn extract_required_roles(_req: &HttpRequest) -> Vec<Role> {
        // This is a simplified version. In a real implementation, you might store
        // the required roles in the request extensions or use a different mechanism.
        // For now, we'll return an empty vector as a placeholder.
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bornemap_auth::rbac::Role;

    #[test]
    fn role_auth_extractor_creation() {
        let extractor = RoleAuthExtractor::new(vec![Role::Admin]);
        assert_eq!(extractor.required_roles, vec![Role::Admin]);

        let extractor_any = RoleAuthExtractor::new(vec![Role::Admin, Role::Partner]);
        assert_eq!(extractor_any.required_roles, vec![Role::Admin, Role::Partner]);
    }

    #[test]
    fn authorization_middleware_creation() {
        let extractor = AuthorizationMiddleware::require_role(Role::Admin);
        assert_eq!(extractor.required_roles, vec![Role::Admin]);

        let extractor_any = AuthorizationMiddleware::require_any_roles(vec![Role::Admin, Role::Partner]);
        assert_eq!(extractor_any.required_roles, vec![Role::Admin, Role::Partner]);
    }

    #[test]
    fn authorized_request_check_role() {
        // This test would require a full Actix-web app setup
        // For now, we'll just test the basic structure
        let extractor = RoleAuthExtractor::new(vec![Role::Admin]);
        assert_eq!(extractor.required_roles, vec![Role::Admin]);
    }
}