use arrow::array::StringArray;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio;

use deltalake::{DeltaTable};
use deltalake::operations::create::CreateBuilder;
use deltalake::kernel::{DataType, PrimitiveType};

async fn create_table(path: String) -> Result<DeltaTable, String> {
    let builder = CreateBuilder::new()
        .with_location(path)
        .with_column(
            "id",
            DataType::Utf8,  // Corrected type for string in Delta Lake
            false,
            Default::default(),
        )
        .with_column(
            "name",
            DataType::Utf8,  // Corrected type for string in Delta Lake
            false,
            Default::default(),
        );

    builder.await.map_err(|e| e.to_string())
}

fn create_record_batch() -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", ArrowDataType::Utf8, false),
        Field::new("timestamp", ArrowDataType::Utf8, false),
    ]));

    let session_ids = StringArray::from(vec!["session1", "session2", "session3"]);
    let timestamps = StringArray::from(vec![
        "2025-01-01T00:00:00Z",
        "2025-01-01T00:01:00Z",
        "2025-01-01T00:02:00Z",
    ]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(session_ids) as Arc<dyn arrow::array::Array>,
            Arc::new(timestamps) as Arc<dyn arrow::array::Array>,
        ],
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let s3_storage_location = "s3://delta-root/table1".to_string();

    match create_table(s3_storage_location).await {
        Ok(table) => {
            println!("Table created with version: {}", table.version());
        }
        Err(err) => {
            println!("Error creating table: {}", err);
        }
    }
}
