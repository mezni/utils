use crate::auth::middleware::AuthUser;
use crate::domain::partners::repository;
use actix_web::{dev::Payload, web, Error, FromRequest, HttpRequest, HttpResponse};
use futures_util::future::{Ready, ready};
use std::future::Future;
use std::pin::Pin;

pub struct PartnerUser {
    pub user_id: String,
    pub partner_profile_id: String,
}

fn forbidden(detail: impl Into<String>) -> Error {
    actix_web::error::InternalError::from_response(
        "",
        HttpResponse::Forbidden().json(serde_json::json!({
            "type": "forbidden",
            "title": "Partner access required",
            "status": 403,
            "detail": detail.into(),
        })),
    ).into()
}

fn internal_error(detail: impl Into<String>) -> Error {
    actix_web::error::InternalError::from_response(
        "",
        HttpResponse::InternalServerError().json(serde_json::json!({
            "type": "internal_error",
            "title": "Internal server error",
            "status": 500,
            "detail": detail.into(),
        })),
    ).into()
}

impl FromRequest for PartnerUser {
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let auth_user = match AuthUser::from_request(req, payload).into_inner() {
            Ok(user) => user,
            Err(e) => return Box::pin(ready(Err(e))),
        };

        if auth_user.0.role != "partner" {
            return Box::pin(ready(Err(forbidden("Only partner accounts can access this resource"))));
        }

        let pool = match req.app_data::<web::Data<sqlx::PgPool>>() {
            Some(pool) => pool.get_ref().clone(),
            None => return Box::pin(ready(Err(internal_error("Database connection not available")))),
        };

        let user_id = auth_user.0.sub.clone();

        Box::pin(async move {
            match repository::get_by_user_id(&pool, &user_id).await {
                Ok(Some(profile)) => Ok(PartnerUser {
                    user_id,
                    partner_profile_id: profile.id,
                }),
                Ok(None) => Err(forbidden("No partner profile found for this user")),
                Err(e) => Err(internal_error(format!("Failed to lookup partner profile: {}", e))),
            }
        })
    }
}
