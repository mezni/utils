use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn create_record_batch() -> RecordBatch {
    // Define the schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Utf8, false),
    ]));

    // Create data for the "session_id" column
    let session_ids = StringArray::from(vec!["session1", "session2", "session3"]);

    // Create data for the "timestamp" column
    let timestamps = StringArray::from(vec![
        "2025-01-01T00:00:00Z",
        "2025-01-01T00:01:00Z",
        "2025-01-01T00:02:00Z",
    ]);

    // Create the RecordBatch
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(session_ids) as Arc<dyn arrow::array::Array>,
            Arc::new(timestamps) as Arc<dyn arrow::array::Array>,
        ],
    )
    .expect("Failed to create RecordBatch")
}

fn main() {
    let record_batch = create_record_batch();
    println!("{:?}", record_batch);
}
