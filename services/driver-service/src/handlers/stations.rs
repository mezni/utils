use actix_web::{web, HttpResponse, Result};
use sqlx::postgres::PgPool;
use serde::{Deserialize, Serialize};
use crate::domain::gis::{Station, StationList, Pagination};
use crate::queries::bbox::BoundingBoxQueryService;

/// Handler for GET /api/v1/driver/stations
/// Lists stations with pagination support
pub async fn list_stations(
    pool: web::Data<PgPool>,
    query: web::Query<StationListQuery>,
) -> Result<HttpResponse> {
    let service = BoundingBoxQueryService::new(pool.into_inner());

    // Parse query parameters
    let min_lat = query.lat;
    let max_lat = query.lat + (query.radius as f64 / 2.0);
    let min_lon = query.lon - (query.radius as f64 / 2.0);
    let max_lon = query.lon + (query.radius as f64 / 2.0);

    // Find stations with pagination
    match service
        .find_with_pagination(
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            Some(query.radius as i32),
            query.page,
            query.limit,
        )
        .await
    {
        Ok((stations, total)) => {
            // Calculate total pages
            let total_pages = if query.limit > 0 {
                (total / (query.limit as u64)).max(1)
            } else {
                0
            };

            // Create pagination metadata
            let pagination = Pagination::new(query.page, query.limit, total);

            // Create station list response
            let station_list = StationList {
                data: stations,
                pagination,
            };

            Ok(HttpResponse::Ok().json(station_list))
        }
        Err(e) => {
            eprintln!("Error fetching stations: {}", e);
            Ok(HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to fetch stations",
                "message": e.to_string()
            })))
        }
    }
}

/// Query parameters for station list
#[derive(Debug, Deserialize)]
pub struct StationListQuery {
    /// Page number (default: 1)
    #[serde(default = "default_page")]
    pub page: u32,

    /// Items per page (default: 20, max: 100)
    #[serde(default = "default_limit")]
    pub limit: u32,

    /// Center latitude
    pub lat: f64,

    /// Center longitude
    pub lon: f64,

    /// Radius in meters
    pub radius: i32,
}

fn default_page() -> u32 {
    1
}

fn default_limit() -> u32 {
    20
}

/// Handler for GET /api/v1/driver/stations/{id}
/// Gets details for a specific station optimized for mobile map rendering
pub async fn get_station(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let station_id = path.into_inner();

    // Fetch station from database
    let station: Option<Station> = sqlx::query_as::<_, Station>(
        "SELECT id, station_name as name, latitude, longitude, amenity, power, connector_types, is_available, last_updated, created_at FROM gis.osm_charging_stations WHERE id = $1"
    )
    .bind(&station_id)
    .fetch_optional(pool.get_ref())
    .await
    .map_err(|e| {
        eprintln!("Error fetching station {}: {}", station_id, e);
        e
    })?;

    match station {
        Some(station) => {
            // Optimize for mobile map rendering
            let optimized_station = Station {
                id: station.id,
                name: station.name,
                latitude: station.latitude,
                longitude: station.longitude,
                distance: None,
                amenity: station.amenity,
                power: Some(station.power.unwrap_or_default()),
                connector_types: station.connector_types,
                is_available: station.is_available,
                last_updated: Some(station.last_updated.unwrap_or_default()),
                created_at: Some(station.created_at.unwrap_or_default()),
            };

            Ok(HttpResponse::Ok().json(optimized_station))
        }
        None => {
            Ok(HttpResponse::NotFound().json(serde_json::json!({
                "error": "Station not found",
                "message": format!("Station {} not found", station_id)
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_page() {
        assert_eq!(default_page(), 1);
    }

    #[test]
    fn test_default_limit() {
        assert_eq!(default_limit(), 20);
    }
}
