use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures_util::stream::StreamExt;
use lapin::{options::*, types::FieldTable, Channel};
use mongodb::bson::doc;

use crate::AnalyticsAppState;

pub struct HealthState {
    pub last_processed_at: std::sync::Mutex<Option<String>>,
    pub start_time: Instant,
    pub processed_count: AtomicU64,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            last_processed_at: std::sync::Mutex::new(None),
            start_time: Instant::now(),
            processed_count: AtomicU64::new(0),
        }
    }
}

pub async fn start(channel: Channel, state: AnalyticsAppState) {
    let health = Arc::new(HealthState::new());
    let health_web = Arc::clone(&health);

    let health_addr = std::env::var("HEALTH_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8181".to_string());

    tokio::spawn(async move {
        let app = actix_web::App::new().app_data(actix_web::web::Data::from(health_web))
            .route("/health", actix_web::web::get().to(health_handler));
        actix_web::HttpServer::new(move || app.clone())
            .bind(&health_addr)
            .unwrap()
            .run()
            .await
            .unwrap();
    });

    let mut consumer = match channel
        .basic_consume(
            "analytics.connections",
            "analytics_processor_worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            log::error!("Failed to start consumer: {}", e);
            return;
        }
    };

    log::info!("consumer started, waiting for messages");

    let collection = state.mongo_db.collection::<mongodb::bson::Document>("connection_aggregates");

    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok((_, delivery)) => {
                match serde_json::from_slice::<core::AnalyticsEvent>(&delivery.data) {
                    Ok(event) => {
                        let filter = doc! { "platform": &event.client_platform };
                        let update = doc! {
                            "$inc": { "total_connections_count": 1 },
                            "$set": {
                                "last_handshake_at": &event.connected_at,
                                "engine_version": &event.app_version,
                            }
                        };

                        match collection
                            .update_one(
                                filter,
                                update,
                                mongodb::options::UpdateOptions::builder()
                                    .upsert(true)
                                    .build(),
                            )
                            .await
                        {
                            Ok(_) => {
                                if let Ok(mut ts) = health.last_processed_at.lock() {
                                    *ts = Some(event.connected_at.clone());
                                }
                                health.processed_count.fetch_add(1, Ordering::Relaxed);
                                let _ = delivery.ack(BasicAckOptions::default()).await;
                            }
                            Err(e) => {
                                log::error!("MongoDB upsert failed: {}", e);
                                let _ = delivery.nack(BasicNackOptions::default()).await;
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to deserialize event: {}", e);
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                    }
                }
            }
            Err(e) => {
                log::error!("Consumer error: {}", e);
            }
        }
    }
}

async fn health_handler(
    health: actix_web::web::Data<Arc<HealthState>>,
) -> impl actix_web::Responder {
    let last = health
        .last_processed_at
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| "never".to_string());
    let uptime = health.start_time.elapsed().as_secs();
    let count = health.processed_count.load(Ordering::SeqCst);

    actix_web::HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "processed_count": count,
        "last_processed_at": last,
        "uptime_seconds": uptime,
    }))
}
