use actix_web::{web, HttpResponse};
use sqlx::PgPool;
use crate::domain::{CreatePartnerRequest, CreateResponse};
use crate::error::AdminServiceError;
use services_shared::domain::PartnerDto;

/// Create a new charging network partner
#[utoipa::path(
    post,
    path = "/admin/partners",
    request_body = CreatePartnerRequest,
    responses(
        (status = 201, description = "Partner created successfully", body = CreateResponse<PartnerDto>),
        (status = 400, description = "Invalid request"),
        (status = 500, description = "Internal server error")
    )
)]
#[actix_web::post("/admin/partners")]
pub async fn create_partner(
    req: web::Json<CreatePartnerRequest>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AdminServiceError> {
    let partner = crate::usecase::create_partner(
        pool.get_ref(),
        req.name.clone(),
        req.partner_type.clone(),
        req.email.clone(),
        req.phone.clone(),
    )
    .await?;

    tracing::info!("Partner created: {}", partner.id);

    Ok(HttpResponse::Created().json(CreateResponse {
        data: partner,
        message: "Partner created successfully".to_string(),
    }))
}

/// Get partner by ID
#[utoipa::path(
    get,
    path = "/admin/partners/{partner_id}",
    params(
        ("partner_id" = String, Path, description = "Partner ID")
    ),
    responses(
        (status = 200, description = "Partner retrieved successfully", body = PartnerDto),
        (status = 404, description = "Partner not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[actix_web::get("/admin/partners/{partner_id}")]
pub async fn get_partner(
    partner_id: web::Path<String>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AdminServiceError> {
    let partner = crate::usecase::get_partner(pool.get_ref(), &partner_id).await?;

    Ok(HttpResponse::Ok().json(partner))
}
