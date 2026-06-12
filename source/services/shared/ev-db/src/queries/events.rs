use ev_core::error::{AppError, FieldError};
use ev_core::event::{BatchEventResponse, CreateEventRequest, EventResponse};
use sqlx::PgPool;

pub fn validate_event(event: &CreateEventRequest, index: Option<usize>) -> Vec<FieldError> {
    let mut errors: Vec<FieldError> = Vec::new();
    let prefix = match index {
        Some(i) => format!("events[{}].", i),
        None => String::new(),
    };

    if event.event_type.is_empty() {
        errors.push(FieldError {
            field: format!("{}event_type", prefix),
            message: "Event type is required".into(),
        });
    }
    if event.event_type.len() > 50 {
        errors.push(FieldError {
            field: format!("{}event_type", prefix),
            message: "Event type must be 50 characters or fewer".into(),
        });
    }
    if event.session_id.is_empty() {
        errors.push(FieldError {
            field: format!("{}session_id", prefix),
            message: "Session ID is required".into(),
        });
    }

    errors
}

pub async fn insert_single_event(
    pool: &PgPool,
    event: &CreateEventRequest,
) -> Result<EventResponse, AppError> {
    let payload = event
        .payload
        .clone()
        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

    let row: (i64, String, String, chrono::NaiveDateTime, chrono::NaiveDateTime) = sqlx::query_as(
        r#"
        INSERT INTO raw_events (event_type, session_id, user_id, payload, occurred_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, event_type, session_id, occurred_at, ingested_at
        "#
    )
    .bind(&event.event_type)
    .bind(&event.session_id)
    .bind(&event.user_id)
    .bind(&payload)
    .bind(&event.occurred_at)
    .fetch_one(pool)
    .await
    .map_err(AppError::Database)?;

    Ok(EventResponse {
        id: row.0,
        event_type: row.1,
        session_id: row.2,
        occurred_at: row.3,
        ingested_at: row.4,
    })
}

pub async fn insert_batch_events(
    pool: &PgPool,
    events: &[CreateEventRequest],
) -> Result<BatchEventResponse, AppError> {
    let mut all_errors: Vec<FieldError> = Vec::new();

    for (i, event) in events.iter().enumerate() {
        let errors = validate_event(event, Some(i));
        all_errors.extend(errors);
    }

    if !all_errors.is_empty() {
        return Err(AppError::Validation {
            details: all_errors,
        });
    }

    let mut tx = pool.begin().await.map_err(AppError::Database)?;
    let count = events.len();

    for event in events {
        let payload = event
            .payload
            .clone()
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        sqlx::query(
            r#"
            INSERT INTO raw_events (event_type, session_id, user_id, payload, occurred_at)
            VALUES ($1, $2, $3, $4, $5)
            "#
        )
        .bind(&event.event_type)
        .bind(&event.session_id)
        .bind(&event.user_id)
        .bind(&payload)
        .bind(&event.occurred_at)
        .execute(&mut *tx)
        .await
        .map_err(AppError::Database)?;
    }

    tx.commit().await.map_err(AppError::Database)?;

    Ok(BatchEventResponse {
        ingested: count,
        message: format!("Successfully ingested {} events", count),
    })
}
