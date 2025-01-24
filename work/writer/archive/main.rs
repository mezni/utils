use deltalake::{
    kernel::{DataType, PrimitiveType, StructField, StructType},
    protocol::{SaveMode, Protocol,DeltaTableMetaData},
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps, DeltaTable, DeltaTableError, DeltaTableBuilder,
};

use arrow::{
    array::StringArray,
    datatypes::{DataType as ArrowDataType, Field, Schema},
    record_batch::RecordBatch,
    error::ArrowError,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio;
use std::error::Error;


// Define the table schema
fn table_schema() -> StructType {
    StructType::new(vec![
        StructField::new(
            "session_id",
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "timestamp",
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
    ])
}

// Load or create a Delta table
async fn initialize_or_load_table(
    table_path: &str,
) -> Result<DeltaTable, Box<dyn Error>> {
    let delta_log_path = Path::new(table_path).join("_delta_log");

    if delta_log_path.is_dir() {
        // Load existing table
        deltalake::open_table(table_path).await.map_err(|e| e.into())
    } else {
        // Create a new table
    let mut table = DeltaTableBuilder::from_uri(table_path.to_string())
        .build()
        .unwrap();

        let schema = table_schema();

    let mut commit_info = serde_json::Map::<String, serde_json::Value>::new();
    commit_info.insert(
        "operation".to_string(),
        serde_json::Value::String("CREATE TABLE".to_string()),
    );
    commit_info.insert(
        "userName".to_string(),
        serde_json::Value::String("test user".to_string()),
    );

    let protocol = Protocol {
        min_reader_version: 1,
        min_writer_version: 1,
    };

    let metadata = DeltaTableMetaData::new(None, None, None, schema, vec![], HashMap::new());

    table
        .create(metadata, protocol, Some(commit_info), None)
        .await
        .unwrap();

    table

    }
}

// Write data to the Delta table
async fn write_data_to_table(
    table: &mut DeltaTable,
) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", ArrowDataType::Utf8, false),
        Field::new("timestamp", ArrowDataType::Utf8, false),
    ]));

    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(StringArray::from(vec!["a", "a", "a"])),
        ],
    )?;

    let mut record_batch_writer = RecordBatchWriter::for_table(table)?;
    record_batch_writer.write(record_batch).await?;
    record_batch_writer.flush_and_commit(table).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let table_path = "delta-table";
    let mut table = initialize_or_load_table(table_path).await?;
    write_data_to_table(&mut table).await?;

    println!("Data written and committed successfully!");

    Ok(())
}