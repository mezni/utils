use actix_web::{web, HttpMessage, HttpRequest, HttpResponse, Responder};
use domain_types::favorites::{
    AddFavoriteRequest, FavoriteItem, FavoriteResponse, FavoritesListResponse,
    FavoritesMetadata, RemoveFavoriteRequest,
};
use domain_types::jwt::JwtClaims;
use crate::db::pool::PlatformDb;
use serde::Deserialize;
use sqlx::PgPool;
use tracing::{error, info};

#[derive(Debug, Deserialize)]
pub struct ListFavoritesQuery {
    page: Option<u32>,
    per_page: Option<u32>,
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

pub async fn list_favorites(
    req: HttpRequest,
    pool: web::Data<PlatformDb>,
    query: web::Query<ListFavoritesQuery>,
) -> impl Responder {
    let user_id = match extract_user_id(&req) {
        Ok(uid) => uid,
        Err(e) => return e,
    };
    let page = query.page.unwrap_or(1);
    let per_page = query.per_page.unwrap_or(50);
    let db = &pool.0;

    match get_favorites_for_user(db, &user_id).await {
        Ok(items) => {
            let total = items.len();
            let start = ((page.saturating_sub(1)) * per_page) as usize;
            let end = (start + per_page as usize).min(total);
            let page_data = if start < total {
                items[start..end].to_vec()
            } else {
                vec![]
            };

            HttpResponse::Ok().json(FavoritesListResponse {
                data: page_data,
                metadata: FavoritesMetadata {
                    total,
                    page,
                    per_page,
                },
            })
        }
        Err(e) => {
            error!("Failed to list favorites: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "failed_to_list_favorites",
                "message": e.to_string(),
            }))
        }
    }
}

pub async fn add_favorite(
    req: HttpRequest,
    pool: web::Data<PlatformDb>,
    body: web::Json<AddFavoriteRequest>,
) -> impl Responder {
    let user_id = match extract_user_id(&req) {
        Ok(uid) => uid,
        Err(e) => return e,
    };
    let station_id = body.station_id.trim().to_string();
    let db = &pool.0;

    if !station_id.starts_with("STA-") || station_id.len() != 16 {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "invalid_station_id",
            "message": "Station ID must be a valid STA-prefixed nanoid",
        }));
    }

    match add_favorite_for_user(db, &user_id, &station_id).await {
        Ok(added_at) => {
            info!(user_id = %user_id, station_id = %station_id, "Favorite added");
            HttpResponse::Created().json(FavoriteResponse {
                station_id,
                added_at,
            })
        }
        Err(e) => {
            if e.to_string().contains("already favorited") {
                HttpResponse::Conflict().json(serde_json::json!({
                    "error": "already_favorited",
                    "message": "Station is already in favorites",
                }))
            } else {
                error!("Failed to add favorite: {}", e);
                HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": "failed_to_add_favorite",
                    "message": e.to_string(),
                }))
            }
        }
    }
}

pub async fn remove_favorite(
    req: HttpRequest,
    pool: web::Data<PlatformDb>,
    body: web::Json<RemoveFavoriteRequest>,
) -> impl Responder {
    let user_id = match extract_user_id(&req) {
        Ok(uid) => uid,
        Err(e) => return e,
    };
    let station_id = body.station_id.trim().to_string();
    let db = &pool.0;

    match remove_favorite_for_user(db, &user_id, &station_id).await {
        Ok(true) => {
            info!(user_id = %user_id, station_id = %station_id, "Favorite removed");
            HttpResponse::Ok().json(serde_json::json!({
                "status": "removed",
                "station_id": station_id,
            }))
        }
        Ok(false) => HttpResponse::NotFound().json(serde_json::json!({
            "error": "favorite_not_found",
            "message": "Station was not in favorites",
        })),
        Err(e) => {
            error!("Failed to remove favorite: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "failed_to_remove_favorite",
                "message": e.to_string(),
            }))
        }
    }
}

async fn get_favorites_for_user(
    pool: &PgPool,
    user_id: &str,
) -> Result<Vec<FavoriteItem>, sqlx::Error> {
    let user_uuid: uuid::Uuid = user_id.parse().map_err(|_| {
        sqlx::Error::Protocol("Invalid user_id UUID".to_string())
    })?;

    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        r#"
        SELECT preferences
        FROM users
        WHERE id = $1
        "#,
    )
    .bind(user_uuid)
    .fetch_optional(pool)
    .await?;

    match row {
        Some((preferences,)) => {
            let favorites = preferences
                .get("favorites")
                .and_then(|f| serde_json::from_value::<Vec<FavoriteItem>>(f.clone()).ok())
                .unwrap_or_default();
            Ok(favorites)
        }
        None => Ok(vec![]),
    }
}

async fn add_favorite_for_user(
    pool: &PgPool,
    user_id: &str,
    station_id: &str,
) -> Result<chrono::DateTime<chrono::Utc>, sqlx::Error> {
    let user_uuid: uuid::Uuid = user_id.parse().map_err(|_| {
        sqlx::Error::Protocol("Invalid user_id UUID".to_string())
    })?;

    let now = chrono::Utc::now();

    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        r#"
        SELECT preferences
        FROM users
        WHERE id = $1
        "#,
    )
    .bind(user_uuid)
    .fetch_optional(pool)
    .await?;

    let mut prefs = row
        .map(|r| r.0)
        .unwrap_or(serde_json::json!({}));

    let mut favorites: Vec<FavoriteItem> = prefs
        .get("favorites")
        .and_then(|f| serde_json::from_value(f.clone()).ok())
        .unwrap_or_default();

    if favorites.iter().any(|f| f.station_id == station_id) {
        return Err(sqlx::Error::Protocol("already favorited".to_string()));
    }

    favorites.push(FavoriteItem {
        station_id: station_id.to_string(),
        added_at: now,
    });

    prefs["favorites"] = serde_json::to_value(&favorites).map_err(|_| {
        sqlx::Error::Protocol("Failed to serialize favorites".to_string())
    })?;

    sqlx::query(
        r#"
        UPDATE users
        SET preferences = $1::jsonb
        WHERE id = $2
        "#,
    )
    .bind(&prefs)
    .bind(user_uuid)
    .execute(pool)
    .await?;

    Ok(now)
}

async fn remove_favorite_for_user(
    pool: &PgPool,
    user_id: &str,
    station_id: &str,
) -> Result<bool, sqlx::Error> {
    let user_uuid: uuid::Uuid = user_id.parse().map_err(|_| {
        sqlx::Error::Protocol("Invalid user_id UUID".to_string())
    })?;

    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        r#"
        SELECT preferences
        FROM users
        WHERE id = $1
        "#,
    )
    .bind(user_uuid)
    .fetch_optional(pool)
    .await?;

    let mut prefs = match row {
        Some(r) => r.0,
        None => return Ok(false),
    };

    let mut favorites: Vec<FavoriteItem> = prefs
        .get("favorites")
        .and_then(|f| serde_json::from_value(f.clone()).ok())
        .unwrap_or_default();

    let initial_len = favorites.len();
    favorites.retain(|f| f.station_id != station_id);

    if favorites.len() == initial_len {
        return Ok(false);
    }

    prefs["favorites"] = serde_json::to_value(&favorites).map_err(|_| {
        sqlx::Error::Protocol("Failed to serialize favorites".to_string())
    })?;

    sqlx::query(
        r#"
        UPDATE users
        SET preferences = $1::jsonb
        WHERE id = $2
        "#,
    )
    .bind(&prefs)
    .bind(user_uuid)
    .execute(pool)
    .await?;

    Ok(true)
}

pub fn configure_favorites_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1/driver")
            .route("/favorites", web::get().to(list_favorites))
            .route("/favorites", web::post().to(add_favorite))
            .route("/favorites", web::delete().to(remove_favorite))
    );
}
