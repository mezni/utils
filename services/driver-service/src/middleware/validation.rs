use crate::models::error::{ErrorResponse, ErrorDetail};
use sqlx::types::BigDecimal;

pub fn validate_coordinates(lat: BigDecimal, lon: BigDecimal) -> Option<ErrorResponse> {
    let lat: f64 = lat.try_into().ok()?;
    let lon: f64 = lon.try_into().ok()?;

    if lat < -90.0 || lat > 90.0 {
        return Some(ErrorResponse {
            error: ErrorDetail {
                code: "GEO_001".to_string(),
                message: "Coordinates must be within valid geographic ranges (lat: -90 to 90, lon: -180 to 180)".to_string(),
                field: Some("coordinates".to_string()),
            },
            meta: crate::models::error::ResponseMeta {
                request_id: uuid::Uuid::new_v4().to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        });
    }

    if lon < -180.0 || lon > 180.0 {
        return Some(ErrorResponse {
            error: ErrorDetail {
                code: "GEO_001".to_string(),
                message: "Coordinates must be within valid geographic ranges (lat: -90 to 90, lon: -180 to 180)".to_string(),
                field: Some("coordinates".to_string()),
            },
            meta: crate::models::error::ResponseMeta {
                request_id: uuid::Uuid::new_v4().to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        });
    }

    None
}

pub fn validate_radius_m(radius_m: Option<i32>) -> Option<ErrorResponse> {
    let radius_m = radius_m.unwrap_or(5000);

    if radius_m < 1 || radius_m > 50000 {
        return Some(ErrorResponse {
            error: ErrorDetail {
                code: "GEO_002".to_string(),
                message: "Radius must be between 1 and 50000 meters".to_string(),
                field: Some("radius_m".to_string()),
            },
            meta: crate::models::error::ResponseMeta {
                request_id: uuid::Uuid::new_v4().to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        });
    }

    None
}

pub fn validate_max_results(max_results: Option<i32>) -> Option<ErrorResponse> {
    let max_results = max_results.unwrap_or(50);

    if max_results < 1 || max_results > 100 {
        return Some(ErrorResponse {
            error: ErrorDetail {
                code: "GEO_003".to_string(),
                message: "Max results must be between 1 and 100".to_string(),
                field: Some("max_results".to_string()),
            },
            meta: crate::models::error::ResponseMeta {
                request_id: uuid::Uuid::new_v4().to_string(),
                timestamp: chrono::Utc::now().to_rfc3339(),
            },
        });
    }

    None
}
