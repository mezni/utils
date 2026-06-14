use actix_web::{web, HttpResponse};
use sqlx::PgPool;
use crate::domain::{CreateChargerRequest, CreateResponse};
use crate::error::AdminServiceError;
use services_shared::domain::ChargerDetailDto;

/// Create a new charger at a station
#[utoipa::path(
    post,
    path = "/admin/chargers",
    request_body = CreateChargerRequest,
    responses(
        (status = 201, description = "Charger created successfully", body = CreateResponse<ChargerDetailDto>),
        (status = 400, description = "Invalid request"),
        (status = 404, description = "Station or plug type not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[actix_web::post("/admin/chargers")]
pub async fn create_charger(
    req: web::Json<CreateChargerRequest>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AdminServiceError> {
    let charger = crate::usecase::create_charger(
        pool.get_ref(),
        req.station_id.clone(),
        req.identifier_code.clone(),
        req.plug_type_code.clone(),
        req.max_power_kw,
    )
    .await?;

    tracing::info!("Charger created: {} at station {}", charger.id, req.station_id);

    Ok(HttpResponse::Created().json(CreateResponse {
        data: charger,
        message: "Charger created successfully".to_string(),
    }))
}
