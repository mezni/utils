use crate::auth::jwt::{validate_token, Claims, jwt_secret};
use actix_web::{dev::Payload, Error, FromRequest, HttpRequest, HttpResponse};
use futures_util::future::{Ready, ready};

pub struct AuthUser(pub Claims);

impl FromRequest for AuthUser {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let auth_header = req
            .headers()
            .get("Authorization")
            .and_then(|v| v.to_str().ok());

        match auth_header {
            Some(header) if header.starts_with("Bearer ") => {
                let token = &header[7..];
                match validate_token(token, &jwt_secret()) {
                    Ok(claims) => ready(Ok(AuthUser(claims))),
                    Err(e) => ready(Err(actix_web::error::ErrorUnauthorized(
                        serde_json::json!({
                            "type": "unauthorized",
                            "title": "Invalid or expired token",
                            "status": 401,
                            "detail": e
                        }),
                    ))),
                }
            }
            _ => ready(Err(actix_web::error::ErrorUnauthorized(
                serde_json::json!({
                    "type": "unauthorized",
                    "title": "Authentication required",
                    "status": 401,
                    "detail": "A valid Bearer token is required to access this resource."
                }),
            ))),
        }
    }
}
