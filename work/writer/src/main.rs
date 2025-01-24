use deltalake::{
    kernel::{DataType, PrimitiveType, StructField, StructType},
    protocol::{SaveMode},
    DeltaOps, DeltaTableError,
};

use std::path::Path;
use tokio;

fn table_schema() -> StructType {
    StructType::new(vec![
        StructField::new(
            "session_id",
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "timestamp",
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
    ])
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Define the table path
    let table_uri = "delta-table"; // Update with a valid path
    let table_path = table_uri;

    // Create DeltaOps instance
    let mut delta_ops = DeltaOps::try_from_uri(table_path).await?;

    // Define the table schema
    let schema = table_schema();

    // Initialize the table if it doesn't exist
    let table = delta_ops
        .create()
        .with_table_name("some-table")
        .with_save_mode(SaveMode::Overwrite)
        .with_columns(schema.fields().cloned())
        .await?; // Await here is fine

    println!("Table created or initialized successfully: {:?}", table);

    Ok(())
}

