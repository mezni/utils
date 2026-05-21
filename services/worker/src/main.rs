use lapin::{
    options::BasicConsumeOptions,
    Consumer, Connection, ConnectionProperties, BasicProperties,
    message::Delivery,
};
use mongodb::{Client, bson::doc};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Deserialize, Serialize)]
struct TelemetryEvent {
    #[serde(rename = "type")]
    event_type: String,
    timestamp: String,
    screen: Option<String>,
    payload: Option<serde_json::Value>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("everest_worker=info".parse().unwrap()),
        )
        .init();

    dotenvy::dotenv().ok();

    let rabbitmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL not set");
    let mongo_url = env::var("MONGO_URL").expect("MONGO_URL not set");

    tracing::info!("Connecting to RabbitMQ at {}", rabbitmq_url);
    let conn = Connection::connect(&rabbitmq_url, ConnectionProperties::default()).await?;
    tracing::info!("Connected to RabbitMQ");

    let channel = conn.create_channel().await?;

    let consumer = channel
        .basic_consume(
            "telemetry.events.queue",
            "telemetry-consumer",
            BasicConsumeOptions::default(),
            lapin::FieldTable::default(),
        )
        .await?;

    tracing::info!("Connected to MongoDB at {}", mongo_url);
    let client_options = mongodb::options::ClientOptions::parse(&mongo_url).await?;
    let mongo_client = Client::with_options(client_options)?;
    let collection = mongo_client.database("everest_analytics").collection::<mongodb::bson::Document>("events");

    tracing::info!("Worker started, consuming telemetry events");

    let mut batch: Vec<mongodb::bson::Document> = Vec::with_capacity(100);

    while let Some(delivery) = consumer.recv().await {
        if let Err(e) = handle_delivery(delivery, &collection, &mut batch).await {
            tracing::error!("Error handling delivery: {}", e);
        }
    }

    Ok(())
}

async fn handle_delivery(
    delivery: Result<Delivery, lapin::Error>,
    collection: &mongodb::Collection<mongodb::bson::Document>,
    batch: &mut Vec<mongodb::bson::Document>,
) -> Result<(), Box<dyn std::error::Error>> {
    let delivery = delivery?;

    let body = String::from_utf8_lossy(&delivery.data);
    let event: TelemetryEvent = serde_json::from_str(&body)?;

    let doc = mongodb::bson::doc! {
        "event_type": &event.event_type,
        "timestamp": &event.timestamp,
        "screen": event.screen.as_deref().unwrap_or(""),
        "payload": serde_json::to_value(&event.payload).unwrap_or(serde_json::Value::Null),
        "ingested_at": chrono::Utc::now().to_rfc3339(),
    };

    batch.push(doc);

    if batch.len() >= 100 {
        let docs: Vec<_> = batch.drain(..).collect();
        match collection.insert_many(docs, None).await {
            Ok(result) => {
                tracing::info!("Inserted {} events into MongoDB", result.inserted_ids.len());
                delivery.ack(lapin::options::BasicAckOptions::default()).await?;
            }
            Err(e) => {
                tracing::error!("Failed to insert events: {}", e);
            }
        }
    }

    Ok(())
}
