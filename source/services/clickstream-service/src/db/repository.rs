use chrono::Utc;
use sqlx::postgres::PgPool;
use tracing::error;

use crate::errors::AppError;
use crate::models::event::Event;

#[derive(Debug, Clone)]
pub struct AnalyticsDbRepo {
    pool: PgPool,
}

impl AnalyticsDbRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert_event(
        &self,
        event: &Event,
        batch_id: &str,
        ip: Option<&str>,
    ) -> Result<(), AppError> {
        let server_ts = Utc::now();
        sqlx::query(
            r#"INSERT INTO raw_events (batch_id, event_name, user_id, session_id, payload, client_ts, server_ts, ip_address)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"#,
        )
        .bind(batch_id)
        .bind(&event.event_name)
        .bind(&event.user_id)
        .bind(&event.session_id)
        .bind(&event.payload)
        .bind(event.client_ts)
        .bind(server_ts)
        .bind(ip)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            error!("DB insert_event failed: {}", e);
            AppError::db_error("Failed to persist event")
        })?;
        Ok(())
    }

    pub async fn insert_batch(
        &self,
        events: &[Event],
        batch_id: &str,
        ip: Option<&str>,
    ) -> Vec<Result<(), AppError>> {
        let mut results = Vec::with_capacity(events.len());
        for event in events {
            let result = self.insert_event(event, batch_id, ip).await;
            results.push(result);
        }
        results
    }

    pub async fn health_check(&self) -> Result<bool, AppError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| true)
            .map_err(|_| AppError::db_disconnected())
    }
}
