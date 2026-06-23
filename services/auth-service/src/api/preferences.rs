use actix_web::{web, HttpMessage, HttpRequest, HttpResponse, Responder};
use domain_types::jwt::JwtClaims;
use domain_types::preferences::{Preferences, PreferencesResponse, UpdatePreferencesRequest};
use sqlx::PgPool;
use tracing::{error, info};

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

pub async fn get_preferences(
    req: HttpRequest,
    pool: web::Data<PgPool>,
) -> impl Responder {
    let user_id = match extract_user_id(&req) {
        Ok(uid) => uid,
        Err(e) => return e,
    };

    match fetch_preferences(&pool, &user_id).await {
        Ok(prefs) => HttpResponse::Ok().json(PreferencesResponse { data: prefs }),
        Err(e) => {
            error!("Failed to fetch preferences: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "failed_to_fetch_preferences",
                "message": e.to_string(),
            }))
        }
    }
}

pub async fn update_preferences(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    body: web::Json<UpdatePreferencesRequest>,
) -> impl Responder {
    let user_id = match extract_user_id(&req) {
        Ok(uid) => uid,
        Err(e) => return e,
    };

    if let Err(validation_err) = validate_preferences(&body) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "validation_error",
            "message": validation_err,
        }));
    }

    match upsert_preferences(&pool, &user_id, &body, false).await {
        Ok(prefs) => {
            info!(user_id = %user_id, "Preferences updated (full replace)");
            HttpResponse::Ok().json(PreferencesResponse { data: prefs })
        }
        Err(e) => {
            error!("Failed to update preferences: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "failed_to_update_preferences",
                "message": e.to_string(),
            }))
        }
    }
}

pub async fn patch_preferences(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    body: web::Json<UpdatePreferencesRequest>,
) -> impl Responder {
    let user_id = match extract_user_id(&req) {
        Ok(uid) => uid,
        Err(e) => return e,
    };

    if let Err(validation_err) = validate_preferences(&body) {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "validation_error",
            "message": validation_err,
        }));
    }

    match upsert_preferences(&pool, &user_id, &body, true).await {
        Ok(prefs) => {
            info!(user_id = %user_id, "Preferences updated (partial patch)");
            HttpResponse::Ok().json(PreferencesResponse { data: prefs })
        }
        Err(e) => {
            error!("Failed to patch preferences: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "failed_to_patch_preferences",
                "message": e.to_string(),
            }))
        }
    }
}

pub fn validate_preferences(prefs: &UpdatePreferencesRequest) -> Result<(), String> {
    if let Some(ref ct) = prefs.connector_type {
        match ct.as_str() {
            "CCS" | "CHAdeMO" | "Type2" => {}
            _ => return Err(format!("Invalid connector_type: {}. Must be CCS, CHAdeMO, or Type2", ct)),
        }
    }
    if let Some(ref region) = prefs.last_region {
        if region.lat < -90.0 || region.lat > 90.0 {
            return Err("last_region.lat must be between -90 and 90".to_string());
        }
        if region.lng < -180.0 || region.lng > 180.0 {
            return Err("last_region.lng must be between -180 and 180".to_string());
        }
    }
    Ok(())
}

async fn fetch_preferences(
    pool: &PgPool,
    user_id: &str,
) -> Result<Preferences, sqlx::Error> {
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

    let preferences_value = row
        .map(|r| r.0)
        .unwrap_or(serde_json::json!({}));

    let prefs_section = preferences_value
        .get("preferences")
        .cloned()
        .unwrap_or(serde_json::json!({}));

    Ok(serde_json::from_value(prefs_section).unwrap_or(Preferences {
        connector_type: None,
        max_distance: None,
        last_region: None,
        map_filters: None,
    }))
}

async fn upsert_preferences(
    pool: &PgPool,
    user_id: &str,
    update: &UpdatePreferencesRequest,
    partial: bool,
) -> Result<Preferences, sqlx::Error> {
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

    let mut prefs = row
        .map(|r| r.0)
        .unwrap_or(serde_json::json!({}));

    let mut prefs_section: serde_json::Value = prefs
        .get("preferences")
        .cloned()
        .unwrap_or(serde_json::json!({}));

    if partial {
        if let Some(connector_type) = &update.connector_type {
            prefs_section["connector_type"] = serde_json::json!(connector_type);
        }
        if let Some(max_distance) = update.max_distance {
            prefs_section["max_distance"] = serde_json::json!(max_distance);
        }
        if let Some(ref region) = update.last_region {
            prefs_section["last_region"] = serde_json::json!(region);
        }
        if let Some(ref filters) = update.map_filters {
            prefs_section["map_filters"] = serde_json::json!(filters);
        }
    } else {
        if let Some(connector_type) = &update.connector_type {
            prefs_section["connector_type"] = serde_json::json!(connector_type);
        } else {
            prefs_section["connector_type"] = serde_json::Value::Null;
        }
        if let Some(max_distance) = update.max_distance {
            prefs_section["max_distance"] = serde_json::json!(max_distance);
        } else {
            prefs_section["max_distance"] = serde_json::Value::Null;
        }
        if let Some(ref region) = update.last_region {
            prefs_section["last_region"] = serde_json::json!(region);
        } else {
            prefs_section["last_region"] = serde_json::Value::Null;
        }
        if let Some(ref filters) = update.map_filters {
            prefs_section["map_filters"] = serde_json::json!(filters);
        } else {
            prefs_section["map_filters"] = serde_json::Value::Null;
        }
    }

    prefs["preferences"] = prefs_section.clone();

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

    let result: Preferences = serde_json::from_value(prefs_section).unwrap_or(Preferences {
        connector_type: None,
        max_distance: None,
        last_region: None,
        map_filters: None,
    });

    Ok(result)
}

pub fn configure_preferences_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("")
            .route("/api/v1/auth/preferences", web::get().to(get_preferences))
            .route("/api/v1/auth/preferences", web::put().to(update_preferences))
            .route("/api/v1/auth/preferences", web::patch().to(patch_preferences))
    );
}
