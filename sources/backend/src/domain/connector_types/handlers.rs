use crate::domain::connector_types::models::{ConnectorType, CreateConnectorTypeRequest, UpdateConnectorTypeRequest};
use crate::domain::connector_types::repository;
use crate::utils::error::ProblemResponse;
use crate::utils::id_validator;
use crate::utils::pagination::Cursor;
use crate::utils::pagination::ListQuery;
use actix_web::{web, HttpResponse};
use serde::Serialize;
use sqlx::PgPool;

#[derive(Serialize)]
pub struct ConnectorTypeResponse {
    pub id: String,
    pub name: String,
    pub description: String,
    pub is_test: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<ConnectorType> for ConnectorTypeResponse {
    fn from(c: ConnectorType) -> Self {
        Self {
            id: c.id,
            name: c.name,
            description: c.description,
            is_test: c.is_test,
            created_at: c.created_at,
            updated_at: c.updated_at,
        }
    }
}

pub async fn create_connector_type(
    pool: web::Data<PgPool>,
    _auth: crate::auth::middleware::AuthUser,
    body: web::Json<CreateConnectorTypeRequest>,
) -> HttpResponse {
    let req = body.into_inner();

    if req.name.len() < 2 || req.name.len() > 100 {
        return ProblemResponse::validation("Name must be between 2 and 100 characters");
    }

    match repository::exists_by_name(&pool, &req.name).await {
        Ok(true) => return ProblemResponse::conflict("Connector type with this name already exists"),
        Ok(false) => {}
        Err(_) => return ProblemResponse::internal_error(),
    }

    let id = crate::utils::id_generator::generate_id("CNT");

    match repository::create(&pool, &id, &req, false).await {
        Ok(ct) => HttpResponse::Created().json(ConnectorTypeResponse::from(ct)),
        Err(e) => {
            if let sqlx::Error::Database(ref db_err) = e {
                if db_err.constraint() == Some("connector_types_name_key") {
                    return ProblemResponse::conflict("Connector type with this name already exists");
                }
            }
            tracing::error!("Failed to create connector type: {:?}", e);
            ProblemResponse::internal_error()
        }
    }
}

pub async fn list_connector_types(
    pool: web::Data<PgPool>,
    query: web::Query<ListQuery>,
) -> HttpResponse {
    let q = query.into_inner();
    let limit = q.limit();

    let cursor = match q.cursor.as_ref() {
        Some(c) => match Cursor::decode(c) {
            Ok(c) => Some(c),
            Err(_) => return ProblemResponse::validation("Invalid cursor format"),
        },
        None => None,
    };

    match repository::list(&pool, cursor, limit, q.include_test()).await {
        Ok((types, next_cursor, has_more)) => {
            let data: Vec<ConnectorTypeResponse> = types.into_iter().map(ConnectorTypeResponse::from).collect();
            HttpResponse::Ok().json(serde_json::json!({
                "data": data,
                "pagination": {
                    "next_cursor": next_cursor,
                    "has_more": has_more
                }
            }))
        }
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn get_connector_type(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> HttpResponse {
    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "CNT") {
        return ProblemResponse::not_found(e);
    }

    match repository::get_by_id(&pool, &id).await {
        Ok(Some(ct)) => HttpResponse::Ok().json(ConnectorTypeResponse::from(ct)),
        Ok(None) => ProblemResponse::not_found(format!("Connector type '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn update_connector_type(
    pool: web::Data<PgPool>,
    _auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
    body: web::Json<UpdateConnectorTypeRequest>,
) -> HttpResponse {
    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "CNT") {
        return ProblemResponse::not_found(e);
    }

    match repository::update(&pool, &id, &body).await {
        Ok(Some(ct)) => HttpResponse::Ok().json(ConnectorTypeResponse::from(ct)),
        Ok(None) => {
            let exists = repository::get_by_id(&pool, &id).await.unwrap_or(None);
            if exists.is_some() {
                ProblemResponse::conflict("Concurrent modification detected — re-read and retry")
            } else {
                ProblemResponse::not_found(format!("Connector type '{}' not found", &id))
            }
        }
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn delete_connector_type(
    pool: web::Data<PgPool>,
    _auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
) -> HttpResponse {
    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "CNT") {
        return ProblemResponse::not_found(e);
    }

    match repository::is_referenced_by_charger(&pool, &id).await {
        Ok(true) => {
            return ProblemResponse::conflict(
                "Cannot delete connector type — it is referenced by one or more chargers"
            );
        }
        Ok(false) => {}
        Err(_) => return ProblemResponse::internal_error(),
    }

    match repository::soft_delete(&pool, &id).await {
        Ok(Some(_)) => HttpResponse::NoContent().finish(),
        Ok(None) => ProblemResponse::not_found(format!("Connector type '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}
