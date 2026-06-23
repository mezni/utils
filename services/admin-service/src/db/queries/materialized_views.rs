//! Materialized view queries for admin-service
//! Provides parameterized SELECT queries for materialized views

use sqlx::postgres::PgPool;

/// Refresh station_usage materialized view
pub async fn refresh_station_view(station_id: &str) -> Result<(), sqlx::Error> {
    let query = r#"
        REFRESH MATERIALIZED VIEW CONCURRENTLY station_usage
    "#;

    sqlx::query(query)
        .execute(station_id)
        .await
        .map_err(|e| {
            eprintln!("Error refreshing station_usage: {:?}", e);
            e
        })?;

    Ok(())
}

/// Refresh user_activity materialized view
pub async fn refresh_user_view(user_uuid: &str) -> Result<(), sqlx::Error> {
    let query = r#"
        REFRESH MATERIALIZED VIEW CONCURRENTLY user_activity
    "#;

    sqlx::query(query)
        .execute(user_uuid)
        .await
        .map_err(|e| {
            eprintln!("Error refreshing user_activity: {:?}", e);
            e
        })?;

    Ok(())
}

/// Refresh search_trends materialized view
pub async fn refresh_search_trends() -> Result<(), sqlx::Error> {
    let query = r#"
        REFRESH MATERIALIZED VIEW CONCURRENTLY search_trends
    "#;

    sqlx::query(query)
        .execute(&())
        .await
        .map_err(|e| {
            eprintln!("Error refreshing search_trends: {:?}", e);
            e
        })?;

    Ok(())
}

/// Get station usage count
pub async fn get_station_view_count(station_id: &str) -> Result<u64, sqlx::Error> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*) as count
        FROM station_usage
        WHERE station_id = $1
        "#,
    )
    .bind(station_id)
    .fetch_one(&())
    .await?;

    Ok(count as u64)
}

/// Get user activity count
pub async fn get_user_view_count(user_uuid: &str) -> Result<u64, sqlx::Error> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*) as count
        FROM user_activity
        WHERE user_uuid = $1
        "#,
    )
    .bind(user_uuid)
    .fetch_one(&())
    .await?;

    Ok(count as u64)
}

/// Get search trends count
pub async fn get_search_trends_count() -> Result<u64, sqlx::Error> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*) as count
        FROM search_trends
        "#,
    )
    .fetch_one(&())
    .await?;

    Ok(count as u64)
}

/// Get station usage by partner
pub async fn get_station_usage_by_partner(
    partner_id: &str,
    pool: &PgPool,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT
            station_id,
            station_views
        FROM station_usage
        WHERE station_id LIKE $1
        "#,
    )
    .bind(format!("{}%", partner_id))
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Get user activity by partner
pub async fn get_user_activity_by_partner(
    partner_id: &str,
    pool: &PgPool,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT
            user_uuid,
            total_views
        FROM user_activity
        WHERE user_uuid LIKE $1
        "#,
    )
    .bind(format!("{}%", partner_id))
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Get search trends by partner
pub async fn get_search_trends_by_partner(
    partner_id: &str,
    pool: &PgPool,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT
            query_text,
            search_count
        FROM search_trends
        WHERE query_text LIKE $1
        "#,
    )
    .bind(format!("{}%", partner_id))
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Validate station ID format (PREFIX-nanoid(12))
pub fn validate_station_id(station_id: &str) -> Result<(), String> {
    // PREFIX-nanoid(12) format: 3 uppercase letters + "-" + 12 characters
    let pattern = r"^[A-Z]{3}-[A-Za-z0-9]{12}$";

    if !regex::Regex::new(pattern).unwrap().is_match(station_id) {
        return Err(format!(
            "Station ID {} must be in PREFIX-nanoid(12) format (e.g., STA-abc123def456)",
            station_id
        ));
    }

    Ok(())
}

/// Validate partner ID format (PREFIX-nanoid(12))
pub fn validate_partner_id(partner_id: &str) -> Result<(), String> {
    // PREFIX-nanoid(12) format: 3 uppercase letters + "-" + 12 characters
    let pattern = r"^[A-Z]{3}-[A-Za-z0-9]{12}$";

    if !regex::Regex::new(pattern).unwrap().is_match(partner_id) {
        return Err(format!(
            "Partner ID {} must be in PREFIX-nanoid(12) format (e.g., STX-abc123def456)",
            partner_id
        ));
    }

    Ok(())
}

/// Validate user UUID format
pub fn validate_user_uuid(user_uuid: &str) -> Result<(), String> {
    if !uuid::Uuid::parse_str(user_uuid).is_ok() {
        return Err(format!("Invalid user UUID format: {}", user_uuid));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_station_id_valid() {
        assert!(validate_station_id("STA-abc123def456").is_ok());
        assert!(validate_station_id("CHG-k9x2lm8q1v7z").is_ok());
        assert!(validate_station_id("OPR-x91kd82m4p0a").is_ok());
    }

    #[test]
    fn test_validate_station_id_invalid() {
        assert!(validate_station_id("STAinvalid").is_err());
        assert!(validate_station_id("STA-123").is_err());
        assert!(validate_station_id("invalid-123456789012").is_err());
    }

    #[test]
    fn test_validate_partner_id_valid() {
        assert!(validate_partner_id("STX-abc123def456").is_ok());
        assert!(validate_partner_id("OPS-xyz789def456").is_ok());
    }

    #[test]
    fn test_validate_user_uuid_valid() {
        assert!(validate_user_uuid("550e8400-e29b-41d4-a716-446655440000").is_ok());
    }

    #[test]
    fn test_validate_user_uuid_invalid() {
        assert!(validate_user_uuid("invalid-uuid").is_err());
        assert!(validate_user_uuid("").is_err());
    }
}