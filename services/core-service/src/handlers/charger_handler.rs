use actix_web::{get, post, put, delete, web, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use validator::Validate;
use crate::services::{ChargerService, ChargerServiceError};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateChargerRequest {
    pub station_id: String,
    pub name: String,
    pub description: Option<String>,
    pub charger_type: String,
    pub power_output_kw: f64,
    pub voltage: Option<f64>,
    pub current: Option<f64>,
    pub connector_types: Vec<String>,
    pub is_public: Option<bool>,
    pub pricing_info: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateChargerRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub charger_type: Option<String>,
    pub power_output_kw: Option<f64>,
    pub voltage: Option<f64>,
    pub current: Option<f64>,
    pub connector_types: Option<Vec<String>>,
    pub status: Option<String>,
    pub is_public: Option<bool>,
    pub pricing_info: Option<serde_json::Value>,
    pub is_active: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateChargerStatusRequest {
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChargerSearchRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChargerDateRangeRequest {
    pub start: chrono::DateTime<chrono::Utc>,
    pub end: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct ChargerResponse {
    pub id: String,
    pub station_id: String,
    pub name: String,
    pub description: Option<String>,
    pub charger_type: String,
    pub power_output_kw: f64,
    pub voltage: Option<f64>,
    pub current: Option<f64>,
    pub connector_types: Vec<String>,
    pub status: String,
    pub last_status_update: Option<chrono::DateTime<chrono::Utc>>,
    pub is_public: bool,
    pub pricing_info: Option<serde_json::Value>,
    pub is_active: bool,
    pub version: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<crate::models::Charger> for ChargerResponse {
    fn from(charger: crate::models::Charger) -> Self {
        Self {
            id: charger.base.id,
            station_id: charger.station_id,
            name: charger.name,
            description: charger.description,
            charger_type: format!("{:?}", charger.charger_type),
            power_output_kw: charger.power_output_kw,
            voltage: charger.voltage,
            current: charger.current,
            connector_types: charger.connector_types.iter().map(|ct| format!("{:?}", ct)).collect(),
            status: format!("{:?}", charger.status),
            last_status_update: charger.last_status_update,
            is_public: charger.is_public,
            pricing_info: charger.pricing_info,
            is_active: charger.is_active,
            version: charger.version,
            created_at: charger.base.created_at,
            updated_at: charger.base.updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

impl From<ChargerServiceError> for HttpResponse {
    fn from(err: ChargerServiceError) -> Self {
        match err {
            ChargerServiceError::NotFound(id) => HttpResponse::NotFound().json(ErrorResponse {
                error: "NOT_FOUND".to_string(),
                message: format!("Charger not found: {}", id),
            }),
            ChargerServiceError::Validation(msg) => HttpResponse::BadRequest().json(ErrorResponse {
                error: "VALIDATION_ERROR".to_string(),
                message: msg,
            }),
            ChargerServiceError::OptimisticLock(msg) => HttpResponse::Conflict().json(ErrorResponse {
                error: "OPTIMISTIC_LOCK_ERROR".to_string(),
                message: msg,
            }),
            ChargerServiceError::Database(msg) => HttpResponse::InternalServerError().json(ErrorResponse {
                error: "DATABASE_ERROR".to_string(),
                message: msg,
            }),
            ChargerServiceError::StationNotFound(id) => HttpResponse::BadRequest().json(ErrorResponse {
                error: "STATION_NOT_FOUND".to_string(),
                message: format!("Station not found: {}", id),
            }),
            ChargerServiceError::StationSoftDeleted(id) => HttpResponse::BadRequest().json(ErrorResponse {
                error: "STATION_SOFT_DELETED".to_string(),
                message: format!("Station is soft-deleted: {}", id),
            }),
            ChargerServiceError::NameAlreadyExists(name) => HttpResponse::Conflict().json(ErrorResponse {
                error: "NAME_ALREADY_EXISTS".to_string(),
                message: format!("Charger already exists with name: {}", name),
            }),
            ChargerServiceError::SoftDeleted(id) => HttpResponse::Gone().json(ErrorResponse {
                error: "SOFT_DELETED".to_string(),
                message: format!("Charger is soft-deleted: {}", id),
            }),
            ChargerServiceError::InvalidStatusTransition(from, to) => HttpResponse::BadRequest().json(ErrorResponse {
                error: "INVALID_STATUS_TRANSITION".to_string(),
                message: format!("Invalid status transition: {} -> {}", from, to),
            }),
        }
    }
}

/// Create a new charger
#[post("/chargers")]
pub async fn create_charger(
    service: web::Data<Arc<ChargerService>>,
    request: web::Json<CreateChargerRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    // Parse charger type
    let charger_type = match request.charger_type.to_uppercase().as_str() {
        "AC" => crate::models::ChargerType::AC,
        "DC" => crate::models::ChargerType::DC,
        "DCFC" => crate::models::ChargerType::DCFC,
        _ => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Invalid charger type. Must be: AC, DC, or DCFC".to_string(),
        })),
    };

    // Parse connector types
    let connector_types: Result<Vec<_>, _> = request.connector_types.iter().map(|ct| {
        match ct.to_uppercase().as_str() {
            "TYPE1" => Ok(crate::models::ConnectorType::Type1),
            "TYPE2" => Ok(crate::models::ConnectorType::Type2),
            "CCS" => Ok(crate::models::ConnectorType::CCS),
            "CHADEMO" => Ok(crate::models::ConnectorType::CHAdeMO),
            "TESLA" => Ok(crate::models::ConnectorType::Tesla),
            "OTHER" => Ok(crate::models::ConnectorType::Other),
            _ => Err("Invalid connector type".to_string()),
        }
    }).collect();

    let connector_types = match connector_types {
        Ok(ct) => ct,
        Err(e) => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Invalid connector type: {}", e),
        })),
    };

    match service.create_charger(
        request.station_id.clone(),
        request.name.clone(),
        request.description.clone(),
        charger_type,
        request.power_output_kw,
        request.voltage,
        request.current,
        connector_types,
        request.is_public,
        request.pricing_info.clone(),
    ).await {
        Ok(charger) => Ok(HttpResponse::Created().json(ChargerResponse::from(charger))),
        Err(err) => Ok(err.into()),
    }
}

/// Get a charger by ID
#[get("/chargers/{id}")]
pub async fn get_charger(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.get_charger(&id).await {
        Ok(charger) => Ok(HttpResponse::Ok().json(ChargerResponse::from(charger))),
        Err(err) => Ok(err.into()),
    }
}

/// Get all chargers for a station
#[get("/stations/{station_id}/chargers")]
pub async fn get_chargers_by_station(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let station_id = path.into_inner();
    
    match service.get_chargers_by_station(&station_id).await {
        Ok(chargers) => {
            let responses: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Get all chargers
#[get("/chargers")]
pub async fn get_all_chargers(
    service: web::Data<Arc<ChargerService>>,
) -> Result<HttpResponse> {
    match service.get_all_chargers().await {
        Ok(chargers) => {
            let responses: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Update a charger
#[put("/chargers/{id}")]
pub async fn update_charger(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
    request: web::Json<UpdateChargerRequest>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    // Parse charger type
    let charger_type = match request.charger_type.as_ref() {
        Some(ct) => match ct.to_uppercase().as_str() {
            "AC" => Some(crate::models::ChargerType::AC),
            "DC" => Some(crate::models::ChargerType::DC),
            "DCFC" => Some(crate::models::ChargerType::DCFC),
            _ => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
                error: "VALIDATION_ERROR".to_string(),
                message: "Invalid charger type. Must be: AC, DC, or DCFC".to_string(),
            })),
        },
        None => None,
    };

    // Parse connector types
    let connector_types = match request.connector_types.as_ref() {
        Some(cts) => {
            let parsed: Result<Vec<_>, _> = cts.iter().map(|ct| {
                match ct.to_uppercase().as_str() {
                    "TYPE1" => Ok(crate::models::ConnectorType::Type1),
                    "TYPE2" => Ok(crate::models::ConnectorType::Type2),
                    "CCS" => Ok(crate::models::ConnectorType::CCS),
                    "CHADEMO" => Ok(crate::models::ConnectorType::CHAdeMO),
                    "TESLA" => Ok(crate::models::ConnectorType::Tesla),
                    "OTHER" => Ok(crate::models::ConnectorType::Other),
                    _ => Err("Invalid connector type".to_string()),
                }
            }).collect();

            match parsed {
                Ok(ct) => Some(ct),
                Err(e) => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
                    error: "VALIDATION_ERROR".to_string(),
                    message: format!("Invalid connector type: {}", e),
                })),
            }
        },
        None => None,
    };

    // Parse status
    let status = match request.status.as_ref() {
        Some(s) => match s.to_uppercase().as_str() {
            "AVAILABLE" => Some(crate::models::ChargerStatus::Available),
            "OCCUPIED" => Some(crate::models::ChargerStatus::Occupied),
            "OFFLINE" => Some(crate::models::ChargerStatus::Offline),
            "MAINTENANCE" => Some(crate::models::ChargerStatus::Maintenance),
            "RESERVED" => Some(crate::models::ChargerStatus::Reserved),
            "FAULTED" => Some(crate::models::ChargerStatus::Faulted),
            _ => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
                error: "VALIDATION_ERROR".to_string(),
                message: "Invalid status. Must be: AVAILABLE, OCCUPIED, OFFLINE, MAINTENANCE, RESERVED, or FAULTED".to_string(),
            })),
        },
        None => None,
    };

    match service.update_charger(
        &id,
        request.name.clone(),
        request.description.clone(),
        charger_type,
        request.power_output_kw,
        request.voltage,
        request.current,
        connector_types,
        status,
        request.is_public,
        request.pricing_info.clone(),
        request.is_active,
    ).await {
        Ok(charger) => Ok(HttpResponse::Ok().json(ChargerResponse::from(charger))),
        Err(err) => Ok(err.into()),
    }
}

/// Update charger status only
#[put("/chargers/{id}/status")]
pub async fn update_charger_status(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
    request: web::Json<UpdateChargerStatusRequest>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    // Parse status
    let status = match request.status.to_uppercase().as_str() {
        "AVAILABLE" => crate::models::ChargerStatus::Available,
        "OCCUPIED" => crate::models::ChargerStatus::Occupied,
        "OFFLINE" => crate::models::ChargerStatus::Offline,
        "MAINTENANCE" => crate::models::ChargerStatus::Maintenance,
        "RESERVED" => crate::models::ChargerStatus::Reserved,
        "FAULTED" => crate::models::ChargerStatus::Faulted,
        _ => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Invalid status. Must be: AVAILABLE, OCCUPIED, OFFLINE, MAINTENANCE, RESERVED, or FAULTED".to_string(),
        })),
    };

    match service.update_charger_status(&id, status).await {
        Ok(_) => Ok(HttpResponse::Ok().json("Charger status updated successfully")),
        Err(err) => Ok(err.into()),
    }
}

/// Soft delete a charger
#[delete("/chargers/{id}")]
pub async fn delete_charger(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.delete_charger(&id).await {
        Ok(_) => Ok(HttpResponse::NoContent().finish()),
        Err(err) => Ok(err.into()),
    }
}

/// Restore a soft-deleted charger
#[post("/chargers/{id}/restore")]
pub async fn restore_charger(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.restore_charger(&id).await {
        Ok(_) => Ok(HttpResponse::Ok().json("Charger restored successfully")),
        Err(err) => Ok(err.into()),
    }
}

/// Search chargers by name
#[post("/chargers/search")]
pub async fn search_chargers(
    service: web::Data<Arc<ChargerService>>,
    request: web::Json<ChargerSearchRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    match service.search_chargers_by_name(&request.name).await {
        Ok(chargers) => {
            let responses: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find chargers by status
#[get("/chargers/status/{status}")]
pub async fn find_chargers_by_status(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let status_str = path.into_inner();
    
    let status = match status_str.to_uppercase().as_str() {
        "AVAILABLE" => crate::models::ChargerStatus::Available,
        "OCCUPIED" => crate::models::ChargerStatus::Occupied,
        "OFFLINE" => crate::models::ChargerStatus::Offline,
        "MAINTENANCE" => crate::models::ChargerStatus::Maintenance,
        "RESERVED" => crate::models::ChargerStatus::Reserved,
        "FAULTED" => crate::models::ChargerStatus::Faulted,
        _ => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Invalid status. Must be: AVAILABLE, OCCUPIED, OFFLINE, MAINTENANCE, RESERVED, or FAULTED".to_string(),
        })),
    };

    match service.find_chargers_by_status(status).await {
        Ok(chargers) => {
            let responses: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find available chargers
#[get("/chargers/available")]
pub async fn find_available_chargers(
    service: web::Data<Arc<ChargerService>>,
) -> Result<HttpResponse> {
    match service.find_available_chargers().await {
        Ok(chargers) => {
            let responses: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find chargers by charger type
#[get("/chargers/type/{charger_type}")]
pub async fn find_chargers_by_type(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let charger_type_str = path.into_inner();
    
    let charger_type = match charger_type_str.to_uppercase().as_str() {
        "AC" => crate::models::ChargerType::AC,
        "DC" => crate::models::ChargerType::DC,
        "DCFC" => crate::models::ChargerType::DCFC,
        _ => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Invalid charger type. Must be: AC, DC, or DCFC".to_string(),
        })),
    };

    match service.find_chargers_by_type(charger_type).await {
        Ok(chargers) => {
            let responses: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find chargers by connector type
#[get("/chargers/connector/{connector_type}")]
pub async fn find_chargers_by_connector_type(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let connector_type_str = path.into_inner();
    
    let connector_type = match connector_type_str.to_uppercase().as_str() {
        "TYPE1" => crate::models::ConnectorType::Type1,
        "TYPE2" => crate::models::ConnectorType::Type2,
        "CCS" => crate::models::ConnectorType::CCS,
        "CHADEMO" => crate::models::ConnectorType::CHAdeMO,
        "TESLA" => crate::models::ConnectorType::Tesla,
        "OTHER" => crate::models::ConnectorType::Other,
        _ => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Invalid connector type. Must be: TYPE1, TYPE2, CCS, CHADEMO, TESLA, or OTHER".to_string(),
        })),
    };

    match service.find_chargers_by_connector_type(connector_type).await {
        Ok(chargers) => {
            let responses: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find public chargers
#[get("/chargers/public")]
pub async fn find_public_chargers(
    service: web::Data<Arc<ChargerService>>,
) -> Result<HttpResponse> {
    match service.find_public_chargers().await {
        Ok(chargers) => {
            let responses: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find chargers created within a date range
#[post("/chargers/search/created")]
pub async fn find_chargers_created_between(
    service: web::Data<Arc<ChargerService>>,
    request: web::Json<ChargerDateRangeRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if request.start > request.end {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Start date must be before end date".to_string(),
        }));
    }

    match service.find_chargers_created_between(request.start, request.end).await {
        Ok(chargers) => {
            let responses: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find chargers updated within a date range
#[post("/chargers/search/updated")]
pub async fn find_chargers_updated_between(
    service: web::Data<Arc<ChargerService>>,
    request: web::Json<ChargerDateRangeRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if request.start > request.end {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Start date must be before end date".to_string(),
        }));
    }

    match service.find_chargers_updated_between(request.start, request.end).await {
        Ok(chargers) => {
            let responses: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Check if a charger exists
#[get("/chargers/{id}/exists")]
pub async fn charger_exists(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.charger_exists(&id).await {
        Ok(exists) => Ok(HttpResponse::Ok().json(exists)),
        Err(err) => Ok(err.into()),
    }
}

/// Get charger count for a station
#[get("/stations/{station_id}/chargers/count")]
pub async fn get_charger_count_by_station(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let station_id = path.into_inner();
    
    match service.get_charger_count_by_station(&station_id).await {
        Ok(count) => Ok(HttpResponse::Ok().json(count)),
        Err(err) => Ok(err.into()),
    }
}

/// Get total charger count
#[get("/chargers/count")]
pub async fn get_charger_count(
    service: web::Data<Arc<ChargerService>>,
) -> Result<HttpResponse> {
    match service.get_charger_count().await {
        Ok(count) => Ok(HttpResponse::Ok().json(count)),
        Err(err) => Ok(err.into()),
    }
}

/// Get available charger count
#[get("/chargers/available/count")]
pub async fn get_available_charger_count(
    service: web::Data<Arc<ChargerService>>,
) -> Result<HttpResponse> {
    match service.get_available_charger_count().await {
        Ok(count) => Ok(HttpResponse::Ok().json(count)),
        Err(err) => Ok(err.into()),
    }
}

/// Get charger version
#[get("/chargers/{id}/version")]
pub async fn get_charger_version(
    service: web::Data<Arc<ChargerService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.get_charger_version(&id).await {
        Ok(version) => {
            match version {
                Some(v) => Ok(HttpResponse::Ok().json(v)),
                None => Ok(HttpResponse::NotFound().json(ErrorResponse {
                    error: "NOT_FOUND".to_string(),
                    message: format!("Charger not found: {}", id),
                })),
            }
        },
        Err(err) => Ok(err.into()),
    }
}

/// Configure charger routes
pub fn configure_charger_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .service(create_charger)
            .service(get_charger)
            .service(get_chargers_by_station)
            .service(get_all_chargers)
            .service(update_charger)
            .service(update_charger_status)
            .service(delete_charger)
            .service(restore_charger)
            .service(search_chargers)
            .service(find_chargers_by_status)
            .service(find_available_chargers)
            .service(find_chargers_by_type)
            .service(find_chargers_by_connector_type)
            .service(find_public_chargers)
            .service(find_chargers_created_between)
            .service(find_chargers_updated_between)
            .service(charger_exists)
            .service(get_charger_count_by_station)
            .service(get_charger_count)
            .service(get_available_charger_count)
            .service(get_charger_version)
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    
    #[actix_rt::test]
    async fn test_create_charger_endpoint() {
        let app = test::init_service(
            App::new().configure(configure_charger_routes)
        ).await;
        
        let req = test::TestRequest::post()
            .uri("/api/v1/chargers")
            .set_json(&CreateChargerRequest {
                station_id: "STA-123456789012".to_string(),
                name: "Test Charger".to_string(),
                description: Some("Test Description".to_string()),
                charger_type: "AC".to_string(),
                power_output_kw: 7.4,
                voltage: None,
                current: None,
                connector_types: vec!["TYPE2".to_string()],
                is_public: None,
                pricing_info: None,
            })
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        // This would return 400 since the station doesn't exist
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
    
    #[actix_rt::test]
    async fn test_get_charger_endpoint() {
        let app = test::init_service(
            App::new().configure(configure_charger_routes)
        ).await;
        
        let req = test::TestRequest::get()
            .uri("/api/v1/chargers/CHR-123456789012")
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        // This would return 404 since the charger doesn't exist
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
    }
    
    #[actix_rt::test]
    async fn test_update_charger_status_endpoint() {
        let app = test::init_service(
            App::new().configure(configure_charger_routes)
        ).await;
        
        let req = test::TestRequest::put()
            .uri("/api/v1/chargers/CHR-123456789012/status")
            .set_json(&UpdateChargerStatusRequest {
                status: "OCCUPIED".to_string(),
            })
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        // This would return 404 since the charger doesn't exist
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
    }
    
    #[actix_rt::test]
    async fn test_find_available_chargers_endpoint() {
        let app = test::init_service(
            App::new().configure(configure_charger_routes)
        ).await;
        
        let req = test::TestRequest::get()
            .uri("/api/v1/chargers/available")
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
    }
    
    #[actix_rt::test]
    async fn test_validation_error() {
        let app = test::init_service(
            App::new().configure(configure_charger_routes)
        ).await;
        
        let req = test::TestRequest::post()
            .uri("/api/v1/chargers")
            .set_json(&CreateChargerRequest {
                station_id: "STA-123456789012".to_string(),
                name: "".to_string(), // Empty name should fail validation
                description: None,
                charger_type: "AC".to_string(),
                power_output_kw: 7.4,
                voltage: None,
                current: None,
                connector_types: vec!["TYPE2".to_string()],
                is_public: None,
                pricing_info: None,
            })
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
}