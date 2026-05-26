use actix_web::HttpResponse;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ProblemResponse {
    #[serde(rename = "type")]
    pub problem_type: &'static str,
    pub title: &'static str,
    pub status: u16,
    pub detail: String,
}

impl ProblemResponse {
    pub fn validation(detail: impl Into<String>) -> HttpResponse {
        HttpResponse::UnprocessableEntity().json(Self {
            problem_type: "validation_error",
            title: "Validation error",
            status: 422,
            detail: detail.into(),
        })
    }

    pub fn not_found(detail: impl Into<String>) -> HttpResponse {
        HttpResponse::NotFound().json(Self {
            problem_type: "not_found",
            title: "Resource not found",
            status: 404,
            detail: detail.into(),
        })
    }

    pub fn conflict(detail: impl Into<String>) -> HttpResponse {
        HttpResponse::Conflict().json(Self {
            problem_type: "conflict",
            title: "Conflict",
            status: 409,
            detail: detail.into(),
        })
    }

    pub fn unauthorized(detail: impl Into<String>) -> HttpResponse {
        HttpResponse::Unauthorized().json(Self {
            problem_type: "unauthorized",
            title: "Authentication required",
            status: 401,
            detail: detail.into(),
        })
    }

    pub fn forbidden(detail: impl Into<String>) -> HttpResponse {
        HttpResponse::Forbidden().json(Self {
            problem_type: "forbidden",
            title: "Access denied",
            status: 403,
            detail: detail.into(),
        })
    }

    pub fn internal_error() -> HttpResponse {
        HttpResponse::InternalServerError().json(Self {
            problem_type: "internal_error",
            title: "Internal server error",
            status: 500,
            detail: "An unexpected error occurred.".into(),
        })
    }

    pub fn internal_error_with(detail: impl Into<String>) -> HttpResponse {
        HttpResponse::InternalServerError().json(Self {
            problem_type: "internal_error",
            title: "Internal server error",
            status: 500,
            detail: detail.into(),
        })
    }
}
