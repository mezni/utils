use arrow::{
    array::StringArray,
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use deltalake::kernel::{DataType, PrimitiveType};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::writer::{DeltaWriter, WriterConfig};
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use deltalake::{DeltaOps, DeltaTable};
use std::sync::Arc;
use tokio;

/// Creates a new Delta table.
async fn create_table(path: String) -> Result<DeltaTable, String> {
    CreateBuilder::new()
        .with_location(path)
        .with_column(
            "session_id",
            DataType::Primitive(PrimitiveType::String),
            false,
            Default::default(),
        )
        .with_column(
            "timestamp",
            DataType::Primitive(PrimitiveType::String),
            false,
            Default::default(),
        )
        .await
        .map_err(|e| e.to_string())
}

/// Creates a new Arrow schema.
fn create_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("session_id", ArrowDataType::Utf8, false),
        Field::new("timestamp", ArrowDataType::Utf8, false),
    ]))
}

/// Creates a new RecordBatch.
fn create_record_batch(schema: &Arc<ArrowSchema>) -> Result<RecordBatch, arrow::error::ArrowError> {
    let session_ids = StringArray::from(vec!["session1", "session2", "session3"]);
    let timestamps = StringArray::from(vec![
        "2025-01-01T00:00:00Z",
        "2025-01-01T00:01:00Z",
        "2025-01-01T00:02:00Z",
    ]);

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(session_ids) as Arc<dyn arrow::array::Array>,
            Arc::new(timestamps) as Arc<dyn arrow::array::Array>,
        ],
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let storage_location = "table1".to_string();
    let schema = DeltaSchema::from(batch.schema().clone());
    match create_table(storage_location.clone()).await {
        Ok(table) => {
    let write_config = WriterConfig::new(
        schema.clone(),      // schema
        vec![],                      // partition_by (empty vector if no partitioning)
        None,                        // properties (None for default)
        None,                        // batch_size (None)
        None,                        // max_files (None)
        10000,                       // max_row_group_size (example value)
        None,                        // compression (None for no compression)
    );
    let mut writer = DeltaWriter::new(schema.clone(), write_config);

        }
        Err(e) => eprintln!("Error creating Delta table: {}", e),
    }
}
