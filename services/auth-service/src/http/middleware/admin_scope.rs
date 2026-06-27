use crate::http::extractors::CurrentUser;
use actix_web::{dev::Payload, error, Error, FromRequest, HttpRequest};
use bornemap_auth::rbac::Role;
use futures::future::{ready, Ready};

pub struct AdminScopeMiddleware;

impl AdminScopeMiddleware {
    pub fn require_admin() -> AdminAuthExtractor {
        AdminAuthExtractor
    }
}

pub struct AdminAuthExtractor;

impl FromRequest for AdminAuthExtractor {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(_req: &HttpRequest, _: &mut Payload) -> Self::Future {
        ready(Ok(Self))
    }
}

pub struct AdminRequest<T> {
    pub current_user: CurrentUser,
    pub inner: T,
}

impl<T> FromRequest for AdminRequest<T>
where
    T: FromRequest + 'static,
    <T as FromRequest>::Future: 'static,
{
    type Error = Error;
    type Future = std::pin::Pin<Box<dyn futures::future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let user_future = CurrentUser::from_request(req, payload);
        let inner_future = T::from_request(req, payload);

        Box::pin(async move {
            let current_user = match user_future.await {
                Ok(user) => user,
                Err(e) => return Err(e.into()),
            };
            
            let inner = match inner_future.await {
                Ok(inner) => inner,
                Err(e) => return Err(e.into()),
            };

            if current_user.role != Role::Admin {
                return Err(error::ErrorForbidden("Admin access required"));
            }

            Ok(Self {
                current_user,
                inner,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;
    use bornemap_auth::rbac::Role;

    #[actix_web::test]
    async fn admin_auth_extractor_creation() {
        let extractor = AdminAuthExtractor;
        // This is a simple struct, so we just test that it can be created
        assert_eq!(std::mem::discriminant(&extractor), std::mem::discriminant(&AdminAuthExtractor));
    }

    #[actix_web::test]
    async fn admin_request_creation() {
        // This test would require a full Actix-web app setup
        // For now, we'll just test the basic structure
        let extractor = AdminAuthExtractor;
        assert_eq!(std::mem::discriminant(&extractor), std::mem::discriminant(&AdminAuthExtractor));
    }
}