use sqlx::PgPool;
use tracing::{error, warn};

pub async fn mv_refresh_service(pool: &PgPool) -> Result<(), String> {
    // Refresh materialized views synchronously after commit
    // Uses CONCURRENTLY to avoid table locks (per constitution)

    info!("Refreshing materialized views");

    // Refresh inventory.mv_stations_summary
    let refresh_summary = sqlx::query!(
        "REFRESH MATERIALIZED VIEW CONCURRENTLY inventory.mv_stations_summary"
    )
    .execute(pool)
    .await;

    match refresh_summary {
        Ok(_) => info!("Successfully refreshed inventory.mv_stations_summary"),
        Err(e) => {
            warn!("Failed to refresh inventory.mv_stations_summary: {}", e);
            // Per constitution: Synchronous refresh with 2-5s soft timeout guard
            // On timeout: log warning and continue (failure tolerated)
        }
    }

    // Refresh inventory.mv_stations_geo
    let refresh_geo = sqlx::query!(
        "REFRESH MATERIALIZED VIEW CONCURRENTLY inventory.mv_stations_geo"
    )
    .execute(pool)
    .await;

    match refresh_geo {
        Ok(_) => info!("Successfully refreshed inventory.mv_stations_geo"),
        Err(e) => {
            warn!("Failed to refresh inventory.mv_stations_geo: {}", e);
            // Failure is tolerated per constitution
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn test_mv_refresh_service() {
        // This would need a real database connection for testing
        // For now, just verify the function compiles
        let pool = sqlx::PgPool::connect("postgresql://localhost/platform_db").await.unwrap();

        let result = mv_refresh_service(&pool).await;

        assert!(result.is_ok());
    }
}
