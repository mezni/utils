use actix_web::{web, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use chrono::Utc;
use std::sync::{Arc, Mutex};
use sqlx::postgres::PgPool;
use crate::domain::gis::Station;
use crate::telemetry::ingestion::{OsmIngestionService, OsmTag, IngestionJobStatus};
use crate::ingestion::osm_parser::OsmParser;
use crate::ingestion::tag_normalizer::TagNormalizer;
use crate::ingestion::staging_upsert::StagingUpsertService;
use crate::ingestion::deduplication::DeduplicationService;

/// Ingestion job storage
type JobStorage = Arc<Mutex<std::collections::HashMap<String, IngestionJob>>>;

/// Job context for async processing
pub struct IngestionJobContext {
    pub job_id: String,
    pub pool: PgPool,
    pub osm_parser: OsmParser,
    pub tag_normalizer: TagNormalizer,
    pub staging_service: StagingUpsertService,
    pub dedup_service: DeduplicationService,
}

/// Handler for POST /api/v1/gis/ingest
/// Triggers OSM data ingestion
pub async fn trigger_ingestion(
    pool: web::Data<PgPool>,
    query: web::Query<IngestionQuery>,
    job_storage: web::Data<JobStorage>,
) -> Result<HttpResponse> {
    let pool_ref = pool.into_inner();

    // Generate job ID
    let job_id = nanoid::nanoid!(12);

    // Create ingestion job
    let job = IngestionJob {
        job_id: job_id.clone(),
        status: IngestionJobStatus::Pending,
        started_at: Some(Utc::now().to_rfc3339()),
        completed_at: None,
        station_count: None,
        error_message: None,
    };

    // Store job
    {
        let mut jobs = job_storage.lock().unwrap();
        jobs.insert(job_id.clone(), job);
    }

    // Spawn async job
    let context = IngestionJobContext {
        job_id: job_id.clone(),
        pool: pool_ref,
        osm_parser: OsmParser,
        tag_normalizer: TagNormalizer,
        staging_service: StagingUpsertService::new(pool_ref),
        dedup_service: DeduplicationService::new(pool_ref),
    };

    tokio::spawn(async move {
        if let Err(e) = process_ingestion_job(context).await {
            eprintln!("Ingestion job {} failed: {}", job_id, e);

            // Update job status to failed
            let mut jobs = job_storage.lock().unwrap();
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = IngestionJobStatus::Failed;
                job.completed_at = Some(Utc::now().to_rfc3339());
                job.error_message = Some(e.to_string());
            }
        }
    });

    Ok(HttpResponse::Accepted().json(IngestionJobResponse {
        job_id: job_id.clone(),
        status: IngestionJobStatus::Pending,
        message: "Ingestion job started".to_string(),
    }))
}

/// Process ingestion job asynchronously
async fn process_ingestion_job(context: IngestionJobContext) -> Result<(), String> {
    // Update job status to processing
    {
        let mut jobs = context.job_storage.lock().unwrap();
        if let Some(job) = jobs.get_mut(&context.job_id) {
            job.status = IngestionJobStatus::Processing;
        }
    }

    // Fetch OSM data
    let ingestion_service = OsmIngestionService::new();
    let xml = ingestion_service
        .fetch_osm_data("area[name='BorneMap']->.searchArea;way(area.searchArea)[amenity~'charging_station|power'];(._;>;);out;")
        .await
        .map_err(|e| format!("Failed to fetch OSM data: {}", e))?;

    // Parse OSM XML
    let osm_tags = context
        .osm_parser
        .parse_osm_xml(&xml)
        .map_err(|e| format!("Failed to parse OSM XML: {}", e))?;

    if osm_tags.is_empty() {
        return Ok(());
    }

    // Validate and normalize tags
    let mut processed_count = 0;
    for osm_tag in &osm_tags {
        // Check for duplicates
        let should_ingest = context
            .dedup_service
            .should_ingest(osm_tag.osm_id)
            .await
            .map_err(|e| format!("Failed to check duplicates: {}", e))?;

        if should_ingest {
            // Normalize tags
            let normalized = context.tag_normalizer.normalize(&osm_tag.tags);

            // Upsert to staging
            context
                .staging_service
                .upsert_staging(
                    normalized,
                    osm_tag.osm_id,
                    Utc::now(),
                )
                .await
                .map_err(|e| format!("Failed to upsert staging record: {}", e))?;

            processed_count += 1;
        }
    }

    // Update job status to completed
    {
        let mut jobs = context.job_storage.lock().unwrap();
        if let Some(job) = jobs.get_mut(&context.job_id) {
            job.status = IngestionJobStatus::Completed;
            job.completed_at = Some(Utc::now().to_rfc3339());
            job.station_count = Some(processed_count as u64);
        }
    }

    Ok(())
}

/// Handler for GET /api/v1/gis/ingest/status/{job_id}
/// Get ingestion job status
pub async fn get_ingestion_status(
    job_storage: web::Data<JobStorage>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let job_id = path.into_inner();

    let jobs = job_storage.lock().unwrap();
    let job = jobs.get(&job_id).cloned().ok_or_else(|| {
        HttpResponse::NotFound().json(serde_json::json!({
            "error": "Job not found",
            "message": format!("Job {} not found", job_id)
        }))
    })?;

    Ok(HttpResponse::Ok().json(IngestionJobResponse {
        job_id: job.job_id,
        status: job.status,
        message: format!("Job status: {:?}", job.status),
    }))
}

/// Query parameters for ingestion trigger
#[derive(Debug, Deserialize)]
pub struct IngestionQuery {
    /// Optional dataset name
    #[serde(default)]
    pub dataset: Option<String>,
}

/// Response for ingestion job
#[derive(Debug, Serialize, Deserialize)]
pub struct IngestionJobResponse {
    pub job_id: String,
    pub status: IngestionJobStatus,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingestion_job_creation() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://test:test@localhost:5432/test")
            .await
            .expect("Failed to connect to test database");

        let job_storage = web::Data::new(JobStorage::new(Arc::new(Mutex::new(std::collections::HashMap::new()))));
        let pool_data = web::Data::new(pool);

        let query = IngestionQuery {
            dataset: Some("test".to_string()),
        };

        // This test validates the structure but can't run without a real async runtime
        // In production, this would use tokio::test
    }
}
