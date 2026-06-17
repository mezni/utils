use actix_web::{web, HttpResponse};
use serde::Deserialize;
use sqlx::PgPool;

use crate::models::error::{ErrorResponse, ErrorDetail, Result as AppResult, ResponseMeta};

#[derive(Deserialize)]
pub struct ImportRequest {
    pub region: String,
    pub bbox: BoundingBox,
}

#[derive(Deserialize)]
pub struct BoundingBox {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

pub async fn import_handler(
    pool: web::Data<PgPool>,
    import_request: web::Json<ImportRequest>,
) -> AppResult<HttpResponse> {
    if let Some(error) = validate_bounding_box(&import_request.bbox) {
        return Ok(HttpResponse::BadRequest().json(error));
    }

    let import_id = format!("imp_{}", uuid::Uuid::new_v4());

    let response = serde_json::json!({
        "data": {
            "import_id": import_id,
            "region": import_request.region,
            "stations_imported": 0,
            "stations_updated": 0,
            "stations_failed": 0,
            "status": "pending",
        },
        "meta": ResponseMeta {
            request_id: uuid::Uuid::new_v4().to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
        },
    });

    Ok(HttpResponse::Accepted().json(response))
}

fn validate_bounding_box(bbox: &BoundingBox) -> Option<ErrorResponse> {
    if bbox.min_lat < -90.0 || bbox.max_lat > 90.0 {
        return Some(ErrorResponse {
            error: ErrorDetail {
                code: "GEO_001".to_string(),
                message: "Bounding box latitude must be within valid range (-90 to 90)".to_string(),
                field: Some("bbox.min_lat".to_string()),
            },
            meta: ResponseMeta {
                request_id: uuid::Uuid::new_v4().to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        });
    }

    if bbox.min_lon < -180.0 || bbox.max_lon > 180.0 {
        return Some(ErrorResponse {
            error: ErrorDetail {
                code: "GEO_001".to_string(),
                message: "Bounding box longitude must be within valid range (-180 to 180)".to_string(),
                field: Some("bbox.min_lon".to_string()),
            },
            meta: ResponseMeta {
                request_id: uuid::Uuid::new_v4().to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        });
    }

    if bbox.min_lat >= bbox.max_lat {
        return Some(ErrorResponse {
            error: ErrorDetail {
                code: "GEO_001".to_string(),
                message: "Bounding box min_lat must be less than max_lat".to_string(),
                field: Some("bbox".to_string()),
            },
            meta: ResponseMeta {
                request_id: uuid::Uuid::new_v4().to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        });
    }

    if bbox.min_lon >= bbox.max_lon {
        return Some(ErrorResponse {
            error: ErrorDetail {
                code: "GEO_001".to_string(),
                message: "Bounding box min_lon must be less than max_lon".to_string(),
                field: Some("bbox".to_string()),
            },
            meta: ResponseMeta {
                request_id: uuid::Uuid::new_v4().to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        });
    }

    None
}
