use datafusion::prelude::*;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::fs::File;
use std::io::{self, BufRead};
use std::sync::Arc;


#[tokio::main]
async fn main() -> Result<(), datafusion::error::DataFusionError> {
    // Define a schema for our DataFrame with a single column "line"
    let schema = Arc::new(Schema::new(vec![Field::new("source_record", DataType::Utf8, false)]));

    // Create a vector to hold the line data
    let mut lines = Vec::new();

    // Read the entire file line by line
    let file = File::open("input.csv")?; // Adjust the file path accordingly
    let reader = io::BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        lines.push(line);
    }

    // Create a StringArray from the Vec<String>
    let line_array = StringArray::from(lines);

    // Create a RecordBatch
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(line_array)])?;

    // Create a DataFrame from the RecordBatch
    let session_context = SessionContext::new();
    let df = session_context.read_batch(batch)?;

    // Show the records
    df.show().await?;

    Ok(())
}