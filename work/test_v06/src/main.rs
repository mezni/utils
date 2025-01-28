use std::sync::Arc;
use datafusion::arrow::{
    array::{Float64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::datasource::memory::MemTable;
use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::parquet::table::{TableParquetOptions, WriteOptions};
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    // Step 1: Define the schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    // Step 2: Create a RecordBatch
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let values = Float64Array::from(vec![10.0, 20.5, 30.8]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(names), Arc::new(values)])
        .map_err(|e| DataFusionError::ArrowError(e, None))?;

    // Step 3: Create a MemTable
    let table = MemTable::try_new(schema, vec![vec![batch]])?;

    // Step 4: Create a SessionContext and register the table
    let mut ctx = SessionContext::new();
    ctx.register_table("my_table", Arc::new(table))?;

    // Step 5: Query the DataFrame
    let df = ctx.sql("SELECT * FROM my_table").await?;

    // Step 6: Write the DataFrame to a Parquet file
    let parquet_path = "data.parquet";
    let options = DataFrameWriteOptions::parquet_default();
    let write_options = TableParquetOptions::default();
    df.write_parquet(parquet_path, options, Some(write_options)).await?;

    println!("Data written to {}", parquet_path);

    Ok(())
}