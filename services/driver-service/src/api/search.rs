use actix_web::{web, HttpMessage, HttpRequest, HttpResponse, Responder};
use domain_types::jwt::JwtClaims;
use domain_types::search::{SearchMetadata, SearchResponse, SearchResult};
use crate::db::pool::PlatformDb;
use serde::Deserialize;
use sqlx::PgPool;
use std::time::Instant;
use tracing::{error, info};

#[derive(Debug, Deserialize)]
pub struct SearchQueryParams {
    q: String,
    lat: Option<f64>,
    lng: Option<f64>,
    limit: Option<u32>,
}

fn extract_user_id(req: &HttpRequest) -> Result<String, HttpResponse> {
    req.extensions()
        .get::<JwtClaims>()
        .map(|claims| claims.sub.to_string())
        .ok_or_else(|| {
            HttpResponse::Unauthorized().json(serde_json::json!({
                "error": "unauthorized",
                "message": "User identity not found in request",
            }))
        })
}

pub async fn search_stations(
    req: HttpRequest,
    pool: web::Data<PlatformDb>,
    query: web::Query<SearchQueryParams>,
) -> impl Responder {
    let _user_id = match extract_user_id(&req) {
        Ok(uid) => uid,
        Err(e) => return e,
    };

    let search_text = query.q.trim();
    if search_text.len() < 2 {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "query_too_short",
            "message": "Search query must be at least 2 characters",
        }));
    }

    let limit = query.limit.unwrap_or(20).min(50);
    let start = Instant::now();
    let db = &pool.0;

    let _lat = query.lat;
    let _lng = query.lng;
    match execute_trigram_search(db, search_text, query.lat, query.lng, limit).await {
        Ok(results) => {
            let latency_ms = start.elapsed().as_millis() as u64;
            let total = results.len();

            info!(
                query = %search_text,
                results = %total,
                latency_ms = %latency_ms,
                "Search executed"
            );

            HttpResponse::Ok().json(SearchResponse {
                data: results,
                metadata: SearchMetadata {
                    query: search_text.to_string(),
                    total,
                    latency_ms,
                },
            })
        }
        Err(e) => {
            error!("Search failed: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "search_failed",
                "message": e.to_string(),
            }))
        }
    }
}

async fn execute_trigram_search(
    pool: &PgPool,
    query: &str,
    lat: Option<f64>,
    lng: Option<f64>,
    limit: u32,
) -> Result<Vec<SearchResult>, sqlx::Error> {
    let query_lower = query.to_lowercase();

    let rows = sqlx::query_as::<_, (String, String, String, Option<f64>, f64, Vec<String>, bool, f64, f64)>(
        r#"
        SELECT
            id,
            station_name,
            COALESCE(address->>'street', '') || ', ' || COALESCE(address->>'city', '') AS address,
            GREATEST(similarity(LOWER(station_name), $1), similarity(LOWER(COALESCE(address->>'street', '') || ' ' || COALESCE(address->>'city', '')), $1)) AS relevance,
            connector_types,
            is_available,
            latitude,
            longitude
        FROM gis.osm_charging_stations
        WHERE
            LOWER(station_name) % $1
            OR LOWER(COALESCE(address->>'street', '') || ' ' || COALESCE(address->>'city', '')) % $1
        ORDER BY relevance DESC
        LIMIT $2
        "#,
    )
    .bind(&query_lower)
    .bind(limit as i32)
    .fetch_all(pool)
    .await?;

    let results = rows
        .into_iter()
        .map(|(id, name, address, distance, relevance, connector_types, available, lat_val, lng_val)| {
            SearchResult {
                station_id: id,
                name,
                address,
                distance_km: distance,
                relevance,
                connector_types,
                available,
                lat: lat_val,
                lng: lng_val,
            }
        })
        .collect();

    Ok(results)
}

pub fn configure_search_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1/driver")
            .route("/search", web::get().to(search_stations))
    );
}
