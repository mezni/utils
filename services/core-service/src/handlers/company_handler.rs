use actix_web::{get, post, put, delete, web, HttpResponse, Result};
use crate::services::{CompanyService, CompanyServiceError};
use crate::dto::{CreateCompanyRequest, UpdateCompanyRequest, CompanyResponse, CompanyListResponse};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub struct CompanySearchRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompanyDateRangeRequest {
    pub start: chrono::DateTime<chrono::Utc>,
    pub end: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

impl From<CompanyServiceError> for HttpResponse {
    fn from(err: CompanyServiceError) -> Self {
        match err {
            CompanyServiceError::NotFound(id) => HttpResponse::NotFound().json(ErrorResponse {
                error: "NOT_FOUND".to_string(),
                message: format!("Company not found: {}", id),
            }),
            CompanyServiceError::Validation(msg) => HttpResponse::BadRequest().json(ErrorResponse {
                error: "VALIDATION_ERROR".to_string(),
                message: msg,
            }),
            CompanyServiceError::OptimisticLock(msg) => HttpResponse::Conflict().json(ErrorResponse {
                error: "OPTIMISTIC_LOCK_ERROR".to_string(),
                message: msg,
            }),
            CompanyServiceError::Database(msg) => HttpResponse::InternalServerError().json(ErrorResponse {
                error: "DATABASE_ERROR".to_string(),
                message: msg,
            }),
            CompanyServiceError::EmailAlreadyExists(email) => HttpResponse::Conflict().json(ErrorResponse {
                error: "EMAIL_ALREADY_EXISTS".to_string(),
                message: format!("Company already exists with email: {}", email),
            }),
            CompanyServiceError::SoftDeleted(id) => HttpResponse::Gone().json(ErrorResponse {
                error: "SOFT_DELETED".to_string(),
                message: format!("Company is soft-deleted: {}", id),
            }),
        }
    }
}

/// Create a new company
#[post("/companies")]
pub async fn create_company(
    service: web::Data<Arc<CompanyService>>,
    request: web::Json<CreateCompanyRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    match service.create_company(
        request.name.clone(),
        request.description.clone(),
        request.email.clone(),
        request.phone.clone(),
        request.website.clone(),
        request.address.clone(),
        request.logo_url.clone(),
    ).await {
        Ok(company) => Ok(HttpResponse::Created().json(CompanyResponse::from(company))),
        Err(err) => Ok(err.into()),
    }
}

/// Get a company by ID
#[get("/companies/{id}")]
pub async fn get_company(
    service: web::Data<Arc<CompanyService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.get_company(&id).await {
        Ok(company) => Ok(HttpResponse::Ok().json(CompanyResponse::from(company))),
        Err(err) => Ok(err.into()),
    }
}

/// Get all companies
#[get("/companies")]
pub async fn get_all_companies(
    service: web::Data<Arc<CompanyService>>,
) -> Result<HttpResponse> {
    match service.get_all_companies().await {
        Ok(companies) => {
            let responses: Vec<CompanyResponse> = companies.into_iter().map(CompanyResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Update a company
#[put("/companies/{id}")]
pub async fn update_company(
    service: web::Data<Arc<CompanyService>>,
    path: web::Path<String>,
    request: web::Json<UpdateCompanyRequest>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    match service.update_company(
        &id,
        request.name.clone(),
        request.description.clone(),
        request.email.clone(),
        request.phone.clone(),
        request.website.clone(),
        request.address.clone(),
        request.logo_url.clone(),
        request.is_active,
    ).await {
        Ok(company) => Ok(HttpResponse::Ok().json(CompanyResponse::from(company))),
        Err(err) => Ok(err.into()),
    }
}

/// Soft delete a company
#[delete("/companies/{id}")]
pub async fn delete_company(
    service: web::Data<Arc<CompanyService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.delete_company(&id).await {
        Ok(_) => Ok(HttpResponse::NoContent().finish()),
        Err(err) => Ok(err.into()),
    }
}

/// Restore a soft-deleted company
#[post("/companies/{id}/restore")]
pub async fn restore_company(
    service: web::Data<Arc<CompanyService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.restore_company(&id).await {
        Ok(_) => Ok(HttpResponse::Ok().json("Company restored successfully")),
        Err(err) => Ok(err.into()),
    }
}

/// Search companies by name
#[post("/companies/search")]
pub async fn search_companies(
    service: web::Data<Arc<CompanyService>>,
    request: web::Json<CompanySearchRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if let Err(errors) = request.validate() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: format!("Validation failed: {}", errors),
        }));
    }

    match service.search_companies_by_name(&request.name).await {
        Ok(companies) => {
            let responses: Vec<CompanyResponse> = companies.into_iter().map(CompanyResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find companies created within a date range
#[post("/companies/search/created")]
pub async fn find_companies_created_between(
    service: web::Data<Arc<CompanyService>>,
    request: web::Json<CompanyDateRangeRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if request.start > request.end {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Start date must be before end date".to_string(),
        }));
    }

    match service.find_companies_created_between(request.start, request.end).await {
        Ok(companies) => {
            let responses: Vec<CompanyResponse> = companies.into_iter().map(CompanyResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Find companies updated within a date range
#[post("/companies/search/updated")]
pub async fn find_companies_updated_between(
    service: web::Data<Arc<CompanyService>>,
    request: web::Json<CompanyDateRangeRequest>,
) -> Result<HttpResponse> {
    // Validate request
    if request.start > request.end {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "VALIDATION_ERROR".to_string(),
            message: "Start date must be before end date".to_string(),
        }));
    }

    match service.find_companies_updated_between(request.start, request.end).await {
        Ok(companies) => {
            let responses: Vec<CompanyResponse> = companies.into_iter().map(CompanyResponse::from).collect();
            Ok(HttpResponse::Ok().json(responses))
        },
        Err(err) => Ok(err.into()),
    }
}

/// Check if a company exists
#[get("/companies/{id}/exists")]
pub async fn company_exists(
    service: web::Data<Arc<CompanyService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.company_exists(&id).await {
        Ok(exists) => Ok(HttpResponse::Ok().json(exists)),
        Err(err) => Ok(err.into()),
    }
}

/// Get company count
#[get("/companies/count")]
pub async fn get_company_count(
    service: web::Data<Arc<CompanyService>>,
) -> Result<HttpResponse> {
    match service.get_company_count().await {
        Ok(count) => Ok(HttpResponse::Ok().json(count)),
        Err(err) => Ok(err.into()),
    }
}

/// Get company version
#[get("/companies/{id}/version")]
pub async fn get_company_version(
    service: web::Data<Arc<CompanyService>>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    
    match service.get_company_version(&id).await {
        Ok(version) => {
            match version {
                Some(v) => Ok(HttpResponse::Ok().json(v)),
                None => Ok(HttpResponse::NotFound().json(ErrorResponse {
                    error: "NOT_FOUND".to_string(),
                    message: format!("Company not found: {}", id),
                })),
            }
        },
        Err(err) => Ok(err.into()),
    }
}

/// Configure company routes
pub fn configure_company_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .service(create_company)
            .service(get_company)
            .service(get_all_companies)
            .service(update_company)
            .service(delete_company)
            .service(restore_company)
            .service(search_companies)
            .service(find_companies_created_between)
            .service(find_companies_updated_between)
            .service(company_exists)
            .service(get_company_count)
            .service(get_company_version)
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    
    #[actix_rt::test]
    async fn test_create_company_endpoint() {
        let app = test::init_service(
            App::new().configure(configure_company_routes)
        ).await;
        
        let req = test::TestRequest::post()
            .uri("/api/v1/companies")
            .set_json(&CreateCompanyRequest {
                name: "Test Company".to_string(),
                description: Some("Test Description".to_string()),
                email: Some("test@example.com".to_string()),
                phone: None,
                website: None,
                address: None,
                logo_url: None,
            })
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::CREATED);
    }
    
    #[actix_rt::test]
    async fn test_get_company_endpoint() {
        let app = test::init_service(
            App::new().configure(configure_company_routes)
        ).await;
        
        let req = test::TestRequest::get()
            .uri("/api/v1/companies/CMP-123456789012")
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        // This would return 404 since the company doesn't exist
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
    }
    
    #[actix_rt::test]
    async fn test_validation_error() {
        let app = test::init_service(
            App::new().configure(configure_company_routes)
        ).await;
        
        let req = test::TestRequest::post()
            .uri("/api/v1/companies")
            .set_json(&CreateCompanyRequest {
                name: "".to_string(), // Empty name should fail validation
                description: None,
                email: None,
                phone: None,
                website: None,
                address: None,
                logo_url: None,
            })
            .to_request();
        
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
}