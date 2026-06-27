use crate::http::extractors::CurrentUser;
use actix_web::{dev::Payload, Error, FromRequest, HttpRequest};
use futures::future::{ready, Ready};

pub struct AuthenticationMiddleware;

impl AuthenticationMiddleware {
    pub fn require_auth() -> AuthExtractor {
        AuthExtractor::new()
    }
}

pub struct AuthExtractor {
    require_auth: bool,
}

impl AuthExtractor {
    pub fn new() -> Self {
        Self { require_auth: true }
    }

    pub fn optional() -> Self {
        Self { require_auth: false }
    }
}

impl FromRequest for AuthExtractor {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(_req: &HttpRequest, _: &mut Payload) -> Self::Future {
        ready(Ok(Self::new()))
    }
}

pub struct AuthenticatedRequest<T> {
    pub current_user: CurrentUser,
    pub inner: T,
}

impl<T> FromRequest for AuthenticatedRequest<T>
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
    use bornemap_auth::jwt_validator::JwtConfig;

    #[actix_web::test]
    async fn auth_extractor_creation() {
        let extractor = AuthExtractor::new();
        assert!(extractor.require_auth);

        let optional = AuthExtractor::optional();
        assert!(!optional.require_auth);
    }

    #[actix_web::test]
    async fn authenticated_request_creation() {
        // This test would require a full Actix-web app setup
        // For now, we'll just test the struct creation
        let extractor = AuthExtractor::new();
        assert!(extractor.require_auth);
    }
}