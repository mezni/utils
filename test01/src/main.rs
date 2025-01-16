use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::{StringArray, ArrayRef};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use uuid::Uuid;
use std::fs::File;
use std::io::{self, BufRead};
use std::sync::Arc;
use chrono::Utc;
use datafusion::prelude::{SessionContext, DataFrame};
use datafusion::dataframe::DataFrameWriteOptions;

/// Reads a CSV file and returns a vector of lines.
fn read_csv_file(file_path: &str) -> Result<Vec<String>, io::Error> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    reader.lines().collect()
}

/// Creates a RecordBatch from the schema, UUIDs, and additional columns.
fn create_record_batch(
    schema: Arc<Schema>,
    row_uuids: Vec<Uuid>,
    csv_lines: Vec<String>,
    file_path: &str,
    current_time: &str,
) -> Result<RecordBatch, ArrowError> {
    let id_array = StringArray::from(row_uuids.iter().map(|uuid| uuid.to_string()).collect::<Vec<_>>());
    let line_array = StringArray::from(csv_lines.clone());
    let file_name_array = StringArray::from(vec![file_path.to_string(); csv_lines.len()]);
    let process_time_array = StringArray::from(vec![current_time.to_string(); csv_lines.len()]);

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

/// Writes the DataFrame to a Parquet file.
async fn write_to_parquet(
    dataframe: DataFrame,
    parquet_path: &str,
) -> Result<Vec<RecordBatch>, datafusion::error::DataFusionError> {
    let write_options = DataFrameWriteOptions::default();
    let table_parquet_options = None;

    dataframe.write_parquet(parquet_path, write_options, table_parquet_options).await
}

#[tokio::main]
async fn main() -> Result<(), datafusion::error::DataFusionError> {
    // File path and current time
    let file_path = "input.csv";
    let current_time = Utc::now().to_rfc3339();

    // Read file and populate lines
    let csv_lines = read_csv_file(file_path)?;

    // Create UUIDs
    let row_uuids: Vec<Uuid> = (0..csv_lines.len()).map(|_| Uuid::new_v4()).collect();

    // Define the schema for the RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("source_record", DataType::Utf8, false),
        Field::new("_file_name", DataType::Utf8, false),
        Field::new("_process_time", DataType::Utf8, false),
    ]));

    // Create the RecordBatch
    let batch = create_record_batch(schema.clone(), row_uuids, csv_lines, file_path, &current_time)?;

    // Create a SessionContext
    let ctx = SessionContext::new();

    // Create a DataFrame from the RecordBatch
    let dataframe = ctx.read_batch(batch)?;

    // Define the path for the Parquet file
    let parquet_path = "output.parquet";

    // Write the DataFrame to a Parquet file
    write_to_parquet(dataframe, parquet_path).await?;

    println!("Data successfully written to Parquet file at {}", parquet_path);

    Ok(())
}