// Import necessary modules
mod generator;
use chrono::Utc;
use generator::syslog::{SyslogMessage, SyslogMessageBatch};
use std::fs;
use std::sync::Arc;
use tokio;
use parquet::file::writer::{SerializedFileWriter};
use parquet::schema::types::{SchemaDescPtr, ColumnDescPtr, PrimitiveType, PhysicalType, Type};
use parquet::file::properties::{WriterProperties};
use parquet::compression::Compression;

// Define the main function
#[tokio::main]
async fn main() -> Result<(), String> {
    // Set up output directory
    let output_dir = "OUTPUT";
    fs::create_dir_all(output_dir)
        .map_err(|e| format!("Failed to create output directory: {}", e))?;

    // Load and generate syslog messages
    let mut batch = SyslogMessageBatch::new();
    batch.load(10_000).await?;
    let (open_out, close_out) = batch.generate().await?;

    // Write open and close syslog messages to Parquet files
    write_syslog_to_parquet(output_dir, &open_out, "open").await?;
    write_syslog_to_parquet(output_dir, &close_out, "close").await?;

    println!("Done!");
    Ok(())
}

