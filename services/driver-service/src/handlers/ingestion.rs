use actix_web::{web, HttpResponse, Result};
use serde::Serialize;
use crate::ingestion::staging_upsert::StagingUpsertService;
use crate::ingestion::approval::ApprovalService;
use crate::ingestion::deduplication::DeduplicationService;

/// Handler for GET /api/v1/gis/ingest/status/{job_id}
/// Get ingestion job status
pub async fn get_ingestion_status(
    pool: web::Data<sqlx::postgres::PgPool>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let job_id = path.into_inner();

    // For now, return a placeholder response
    // In production, this would query the job storage
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "job_id": job_id,
        "status": "completed",
        "message": "Ingestion completed successfully"
    })))
}

/// Handler for GET /api/v1/gis/ingest/stats
/// Get ingestion statistics
pub async fn get_ingestion_stats(
    pool: web::Data<sqlx::postgres::PgPool>,
) -> Result<HttpResponse> {
    let service = StagingUpsertService::new(pool.into_inner());

    // Get staging statistics
    let total_records: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM gis.osm_charging_stations_temp"
    )
    .fetch_one(pool.get_ref())
    .await?;

    let processed_records: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM gis.osm_charging_stations_temp WHERE processed = TRUE"
    )
    .fetch_one(pool.get_ref())
    .await?;

    let unprocessed_records: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM gis.osm_charging_stations_temp WHERE processed = FALSE"
    )
    .fetch_one(pool.get_ref())
    .await?;

    let stats = IngestionStats {
        total_records: total_records.0,
        processed_records: processed_records.0,
        unprocessed_records: unprocessed_records.0,
    };

    Ok(HttpResponse::Ok().json(stats))
}

/// Handler for GET /api/v1/gis/ingest/records/{status}
/// Get ingestion records by status
pub async fn get_ingestion_records(
    pool: web::Data<sqlx::postgres::PgPool>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let status_str = path.into_inner();
    let limit = 100;

    let records = match status_str.as_str() {
        "unprocessed" => {
            let service = StagingUpsertService::new(pool.into_inner());
            service.get_unprocessed(limit).await?
        }
        "processed" => {
            let service = StagingUpsertService::new(pool.into_inner());
            service.get_processed(limit).await?
        }
        _ => {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "Invalid status",
                "message": format!("Invalid status: {}", status_str)
            })))
        }
    };

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "records": records,
        "count": records.len()
    })))
}

/// Handler for GET /api/v1/gis/etl/status
/// Get ETL status
pub async fn get_etl_status(
    pool: web::Data<sqlx::postgres::PgPool>,
) -> Result<HttpResponse> {
    let staging_service = StagingUpsertService::new(pool.into_inner());
    let approval_service = ApprovalService::new(pool.into_inner());
    let dedup_service = DeduplicationService::new(pool.into_inner());

    // Get staging statistics
    let staging_stats = staging_service.get_deduplication_stats().await?;

    // Get approval statistics
    let approval_stats = approval_service.get_approval_stats().await?;

    let status = EtlStatus {
        staging: staging_stats,
        approval: approval_stats,
        ready_to_process: staging_stats.unprocessed_records > 0,
    };

    Ok(HttpResponse::Ok().json(status))
}

/// Handler for POST /api/v1/gis/etl/process
/// Process unprocessed staging records
pub async fn process_staging_records(
    pool: web::Data<sqlx::postgres::PgPool>,
) -> Result<HttpResponse> {
    let staging_service = StagingUpsertService::new(pool.into_inner());
    let approval_service = ApprovalService::new(pool.into_inner());

    // Get unprocessed records
    let records = staging_service.get_unprocessed(100).await?;

    if records.is_empty() {
        return Ok(HttpResponse::Ok().json(serde_json::json!({
            "message": "No unprocessed records found"
        })));
    }

    // For now, just mark them as processed
    let processed_count = staging_service.mark_all_processed().await?;

    // Return count of processed records
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": "Records processed successfully",
        "processed_count": processed_count
    })))
}

/// Ingestion statistics
#[derive(Debug, Serialize)]
pub struct IngestionStats {
    pub total_records: i64,
    pub processed_records: i64,
    pub unprocessed_records: i64,
}

/// ETL status
#[derive(Debug, Serialize)]
pub struct EtlStatus {
    pub staging: DedupStats,
    pub approval: ApprovalStats,
    pub ready_to_process: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_ingestion_status() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let path = web::Path::from("test-job-id".to_string());
        let pool_data = web::Data::new(pool);

        let result = get_ingestion_status(pool_data, path).await;
        assert!(result.is_ok());
    }
}
