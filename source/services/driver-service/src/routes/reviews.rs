use actix_web::{get, web, HttpResponse};
use crate::error::AppError;
use crate::models::ReviewsStubResponse;
use crate::AppState;

#[get("/api/stations/{id}/reviews")]
pub async fn reviews(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let station_id = path.into_inner();

    let exists = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*) FROM "ev-platform".station s
        JOIN "ev-platform".partner p ON s.partner_id = p.id
        WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
          AND s.id = $1
        "#,
    )
    .bind(&station_id)
    .fetch_one(&state.pool)
    .await?;

    if exists == 0 {
        return Err(AppError::NotFound(format!("Station {} not found", station_id)));
    }

    Ok(HttpResponse::Ok().json(ReviewsStubResponse {
        station_id,
        message: "Reviews are coming soon".to_string(),
    }))
}
