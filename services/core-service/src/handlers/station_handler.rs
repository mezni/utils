use actix_web::{get, post, put, delete, web, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use validator::Validate;
use crate::services::{StationService, StationServiceError};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateStationRequest {
    pub company_id: String,
    pub name: String,
    pub description: Option<String>,
    pub address: String,
    pub latitude: f64,
    pub longitude: f64,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub website: Option<String>,
    pub access_type: Option<String>,
    pub operating_hours: Option<serde_json::Value>,
    pub amenities: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateStationRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub address: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub website: Option<String>,
    pub access_type: Option<String>,
    pub operating_hours: Option<serde_json::Value>,
    pub amenities: Option<Vec<String>>,
    pub is_active: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StationSearchRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StationRadiusSearchRequest {
    pub center_lat: f64,
    pub center_lon: f64,
    pub radius_km: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StationDateRangeRequest {
    pub start: chrono::DateTime<chrono::Utc>,
    pub end: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct StationResponse {
    pub id: String,
    pub company_id: String,
    pub name: String,
    pub description: Option<String>,
    pub address: String,
    pub latitude: f64,
    pub longitude: f64,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub website: Option<String>,
    pub access_type: String,
    pub operating_hours: Option<serde_json::Value>,
    pub amenities: Option<Vec<String>>,
    pub is_active: bool,
    pub version: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<crate::models::Station> for StationResponse {
    fn from(station: crate::models::Station) -> Self {
        Self {
            id: station.base.id,
            company_id: station.company_id,
            name: station.name,
            description: station.description,
            address: station.address,
            latitude: station.latitude,
            longitude: station.longitude,
            phone: station.phone,
            email: station.email,
            website: station.website,
            access_type: format!("{:?}", station.access_type),
            operating_hours: station.operating_hours,
            amenities: station.amenities,
            is_active: station.is_active,
            version: station.version,
            created_at: station.base.created_at,
            updated_at: station.base.updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

impl From<StationServiceError> for HttpResponse {
    fn from(err: StationServiceError) -> Self {
        match err {
            StationServiceError::NotFound(id) => HttpResponse::NotFound().json(ErrorResponse {
                error: "NOT_FOUND".to_string(),
                message: format!("Station not found: {}", id),
            }),
            StationServiceError::Validation(msg) => HttpResponse::BadRequest().json(ErrorResponse {
                error: "VALIDATION_ERROR".to_string(),
                message: msg,
            }),
            StationServiceError::OptimisticLock(msg) => HttpResponse::Conflict().json(ErrorResponse {
                error: "OPTIMISTIC_LOCK_ERROR".to_string(),
                message: msg,
            }),
            StationServiceError::Database(msg) => HttpResponse::InternalServerError().json(ErrorResponse {
                error: "DATABASE_ERROR".to_string(),
                message: msg,
            }),
            StationServiceError::CompanyNotFound(id) => HttpResponse::BadRequest().json(ErrorResponse {
                error: "COMPANY_NOT_FOUND".to_string(),
                message: format!("Company not found: {}", id),
            }),
            StationServiceError::CompanySoftDeleted(id) => HttpResponse::BadRequest().json(ErrorResponse {
                error: "COMPANY_SOFT_DELETED".to_string(),
                message: format!("Company is soft-deleted: {}", id),
            }),
            StationServiceError::NameAlreadyExists(name) => HttpResponse::Conflict().json(ErrorResponse {
                error: "NAME_ALREADY_EXISTS".to_string(),
                message: format!("Station already exists with name: {}", name),
            }),
            StationServiceError::SoftDeleted(id) => HttpResponse::Gone().json(ErrorResponse {
                error: "SOFT_DELETED".to_string(),
                message: format!("Station is soft-deleted: {}", id),
            }),
        }
    }
}

/// Create a new station
#[post("/stations")]
pub async fn create_station(
    service: web::Data<Arc<StationService>>,
    request: web::Json<CreateStationRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    // Parse access type
    let access_type = match request.access_type.as_ref() {
        Some(at) => match at.to_uppercase().as_str() {
            "PUBLIC" => Some(crate::models::AccessType::Public),
            "PRIVATE" => Some(crate::models::AccessType::Private),
            "RESTRICTED" => Some(crate::models::AccessType::Restricted),
            _ => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
                error: "VALIDATION_ERROR".to_string(),
                message: "Invalid access type. Must be: PUBLIC, PRIVATE, or RESTRICTED".to_string(),
            })),
        },
        None => None,
    };

    match service.create_station(
        request.company_id.clone(),
        request.name.clone(),
        request.description.clone(),
        request.address.clone(),
        request.latitude,
        request.longitude,
        request.phone.clone(),
        request.email.clone(),
        request.website.clone(),
        access_type,
        request.operating_hours.clone(),
        request.amenities.clone(),
    ).await {
        Ok(station) => Ok(HttpResponse::Created().json(StationResponse::from(station))),
        Err(err) => Ok(err.into()),
    }
}

/// Get a station by ID
#[get("/stations/{id}")]
pub async fn get_station(
    service: web::Data<Arc<StationService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.get_station(&id).await {
        Ok(station) => Ok(HttpResponse::Ok().json(StationResponse::from(station))),
        Err(err) => Ok(err.into()),
    }
}

/// Get all stations for a company
#[get("/companies/{company_id}/stations")]
pub async fn get_stations_by_company(
    service: web::Data<Arc<StationService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let company_id = path.into_inner();
    
    match service.get_stations_by_company(&company_id).await {
        Ok(stations) => {
            let responses: Vec<StationResponse> = stations.into_iter().map(StationResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Get all stations
#[get("/stations")]
pub async fn get_all_stations(
    service: web::Data<Arc<StationService>>,
) -> Result<HttpResponse> {
    match service.get_all_stations().await {
        Ok(stations) => {
            let responses: Vec<StationResponse> = stations.into_iter().map(StationResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Update a station
#[put("/stations/{id}")]
pub async fn update_station(
    service: web::Data<Arc<StationService>>,
    path: web::Path<String>,
    request: web::Json<UpdateStationRequest>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    // Parse access type
    let access_type = match request.access_type.as_ref() {
        Some(at) => match at.to_uppercase().as_str() {
            "PUBLIC" => Some(crate::models::AccessType::Public),
            "PRIVATE" => Some(crate::models::AccessType::Private),
            "RESTRICTED" => Some(crate::models::AccessType::Restricted),
            _ => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
                error: "VALIDATION_ERROR".to_string(),
                message: "Invalid access type. Must be: PUBLIC, PRIVATE, or RESTRICTED".to_string(),
            })),
        },
        None => None,
    };

    match service.update_station(
        &id,
        request.name.clone(),
        request.description.clone(),
        request.address.clone(),
        request.latitude,
        request.longitude,
        request.phone.clone(),
        request.email.clone(),
        request.website.clone(),
        access_type,
        request.operating_hours.clone(),
        request.amenities.clone(),
        request.is_active,
    ).await {
        Ok(station) => Ok(HttpResponse::Ok().json(StationResponse::from(station))),
        Err(err) => Ok(err.into()),
    }
}

/// Soft delete a station
#[delete("/stations/{id}")]
pub async fn delete_station(
    service: web::Data<Arc<StationService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.delete_station(&id).await {
        Ok(_) => Ok(HttpResponse::NoContent().finish()),
        Err(err) => Ok(err.into()),
    }
}

/// Restore a soft-deleted station
#[post("/stations/{id}/restore")]
pub async fn restore_station(
    service: web::Data<Arc<StationService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.restore_station(&id).await {
        Ok(_) => Ok(HttpResponse::Ok().json("Station restored successfully")),
        Err(err) => Ok(err.into()),
    }
}

/// Search stations by name
#[post("/stations/search")]
pub async fn search_stations(
    service: web::Data<Arc<StationService>>,
    request: web::Json<StationSearchRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    match service.search_stations_by_name(&request.name).await {
        Ok(stations) => {
            let responses: Vec<StationResponse> = stations.into_iter().map(StationResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find stations within a geographic radius
#[post("/stations/search/radius")]
pub async fn find_stations_by_radius(
    service: web::Data<Arc<StationService>>,
    request: web::Json<StationRadiusSearchRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    match service.find_stations_by_radius(
        request.center_lat,
        request.center_lon,
        request.radius_km,
    ).await {
        Ok(stations) => {
            let responses: Vec<StationResponse> = stations.into_iter().map(StationResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find stations by access type
#[get("/stations/access-type/{access_type}")]
pub async fn find_stations_by_access_type(
    service: web::Data<Arc<StationService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let access_type_str = path.into_inner();
    
    let access_type = match access_type_str.to_uppercase().as_str() {
        "PUBLIC" => crate::models::AccessType::Public,
        "PRIVATE" => crate::models::AccessType::Private,
        "RESTRICTED" => crate::models::AccessType::Restricted,
        _ => return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Invalid access type. Must be: PUBLIC, PRIVATE, or RESTRICTED".to_string(),
        })),
    };

    match service.find_stations_by_access_type(access_type).await {
        Ok(stations) => {
            let responses: Vec<StationResponse> = stations.into_iter().map(StationResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find stations created within a date range
#[post("/stations/search/created")]
pub async fn find_stations_created_between(
    service: web::Data<Arc<StationService>>,
    request: web::Json<StationDateRangeRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if request.start > request.end {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Start date must be before end date".to_string(),
        }));
    }

    match service.find_stations_created_between(request.start, request.end).await {
        Ok(stations) => {
            let responses: Vec<StationResponse> = stations.into_iter().map(StationResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find stations updated within a date range
#[post("/stations/search/updated")]
pub async fn find_stations_updated_between(
    service: web::Data<Arc<StationService>>,
    request: web::Json<StationDateRangeRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if request.start > request.end {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Start date must be before end date".to_string(),
        }));
    }

    match service.find_stations_updated_between(request.start, request.end).await {
        Ok(stations) => {
            let responses: Vec<StationResponse> = stations.into_iter().map(StationResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Check if a station exists
#[get("/stations/{id}/exists")]
pub async fn station_exists(
    service: web::Data<Arc<StationService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.station_exists(&id).await {
        Ok(exists) => Ok(HttpResponse::Ok().json(exists)),
        Err(err) => Ok(err.into()),
    }
}

/// Get station count for a company
#[get("/companies/{company_id}/stations/count")]
pub async fn get_station_count_by_company(
    service: web::Data<Arc<StationService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let company_id = path.into_inner();
    
    match service.get_station_count_by_company(&company_id).await {
        Ok(count) => Ok(HttpResponse::Ok().json(count)),
        Err(err) => Ok(err.into()),
    }
}

/// Get total station count
#[get("/stations/count")]
pub async fn get_station_count(
    service: web::Data<Arc<StationService>>,
) -> Result<HttpResponse> {
    match service.get_station_count().await {
        Ok(count) => Ok(HttpResponse::Ok().json(count)),
        Err(err) => Ok(err.into()),
    }
}

/// Get station version
#[get("/stations/{id}/version")]
pub async fn get_station_version(
    service: web::Data<Arc<StationService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.get_station_version(&id).await {
        Ok(version) => {
            match version {
                Some(v) => Ok(HttpResponse::Ok().json(v)),
                None => Ok(HttpResponse::NotFound().json(ErrorResponse {
                    error: "NOT_FOUND".to_string(),
                    message: format!("Station not found: {}", id),
                })),
            }
        },
        Err(err) => Ok(err.into()),
    }
}

/// Configure station routes
pub fn configure_station_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .service(create_station)
            .service(get_station)
            .service(get_stations_by_company)
            .service(get_all_stations)
            .service(update_station)
            .service(delete_station)
            .service(restore_station)
            .service(search_stations)
            .service(find_stations_by_radius)
            .service(find_stations_by_access_type)
            .service(find_stations_created_between)
            .service(find_stations_updated_between)
            .service(station_exists)
            .service(get_station_count_by_company)
            .service(get_station_count)
            .service(get_station_version)
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    
    #[actix_rt::test]
    async fn test_create_station_endpoint() {
        let app = test::init_service(
            App::new().configure(configure_station_routes)
        ).await;
        
        let req = test::TestRequest::post()
            .uri("/api/v1/stations")
            .set_json(&CreateStationRequest {
                company_id: "CMP-123456789012".to_string(),
                name: "Test Station".to_string(),
                description: Some("Test Description".to_string()),
                address: "Test Address".to_string(),
                latitude: 36.8065,
                longitude: 10.1815,
                phone: None,
                email: None,
                website: None,
                access_type: Some("PUBLIC".to_string()),
                operating_hours: None,
                amenities: None,
            })
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        // This would return 400 since the company doesn't exist
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
    
    #[actix_rt::test]
    async fn test_get_station_endpoint() {
        let app = test::init_service(
            App::new().configure(configure_station_routes)
        ).await;
        
        let req = test::TestRequest::get()
            .uri("/api/v1/stations/STA-123456789012")
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        // This would return 404 since the station doesn't exist
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
    }
    
    #[actix_rt::test]
    async fn test_radius_search_endpoint() {
        let app = test::init_service(
            App::new().configure(configure_station_routes)
        ).await;
        
        let req = test::TestRequest::post()
            .uri("/api/v1/stations/search/radius")
            .set_json(&StationRadiusSearchRequest {
                center_lat: 36.8065,
                center_lon: 10.1815,
                radius_km: 10.0,
            })
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
    }
    
    #[actix_rt::test]
    async fn test_validation_error() {
        let app = test::init_service(
            App::new().configure(configure_station_routes)
        ).await;
        
        let req = test::TestRequest::post()
            .uri("/api/v1/stations")
            .set_json(&CreateStationRequest {
                company_id: "CMP-123456789012".to_string(),
                name: "".to_string(), // Empty name should fail validation
                description: None,
                address: "Test Address".to_string(),
                latitude: 36.8065,
                longitude: 10.1815,
                phone: None,
                email: None,
                website: None,
                access_type: None,
                operating_hours: None,
                amenities: None,
            })
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
}