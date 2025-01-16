use datafusion::prelude::*;
use datafusion::arrow::array::{StringArray, ArrayRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use uuid::Uuid;
use std::fs::File;
use std::io::{self, BufRead};
use std::sync::Arc;
use chrono::Utc;
use deltalake::DeltaTable;
use arrow::error::ArrowError;

/// Reads a CSV file and returns a vector of lines.
fn read_csv_file(file_path: &str) -> Result<Vec<String>, io::Error> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let mut lines = Vec::new();
    for line in reader.lines() {
        lines.push(line?);
    }
    Ok(lines)
}

/// Creates a RecordBatch from the schema, UUIDs, and additional columns.
fn create_record_batch(schema: Arc<Schema>, uuids: Vec<String>, lines: Vec<String>, file_path: &str, current_time: &str) -> Result<RecordBatch, ArrowError> {
    let id_array = StringArray::from(uuids);
    let line_array = StringArray::from(lines);
let file_name_array = StringArray::from(vec![file_path.to_string(); line_array.len()]);
let process_time_array = StringArray::from(vec![current_time.clone(); line_array.len()]);


    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(line_array) as ArrayRef,
            Arc::new(file_name_array) as ArrayRef,
            Arc::new(process_time_array) as ArrayRef,
        ],
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // File path and current time
    let file_path = "input.csv"; // Adjust the file path accordingly
    let current_time = Utc::now().to_rfc3339();

    // Define the schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("source_record", DataType::Utf8, false),
        Field::new("_file_name", DataType::Utf8, false),
        Field::new("_process_time", DataType::Utf8, false),
    ]));

    // Read file and populate lines
    let lines = read_csv_file(file_path)?;

    // Create UUIDs
    let uuids: Vec<String> = (0..lines.len())
        .map(|_| Uuid::new_v4().to_string())
        .collect();

    // Create the RecordBatch
    let batch = create_record_batch(schema.clone(), uuids, lines, file_path, &current_time)?;

    // Create a DataFrame
    let session_context = SessionContext::new();
    let df = session_context.read_batch(batch)?;

    // Show the first 10 records
    let df1 = df.limit(0, Some(10))?;
    df1.show().await?;

    // Write the DataFrame to a Delta table
    let table_path = "./delta_table"; // Path to your Delta table
    let mut table = DeltaTable::open(table_path).await?;

    let batches = df.collect().await?;
    for batch in batches {
        table.write_batch(&batch)?;
    }

    table.commit(None).await?;

    println!("Data successfully written to Delta table at {}", table_path);
    Ok(())
}