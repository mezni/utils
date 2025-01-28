use std::sync::Arc;
use datafusion::arrow::{
    array::{Float64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::prelude::*;
use datafusion::datasource::memory::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::dataframe::DataFrameWriteOptions;
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
    let ctx = SessionContext::new();
    ctx.register_table("my_table", Arc::new(table))?;

    // Step 5: Query the DataFrame
    let df = ctx.sql("SELECT * FROM my_table").await?;
    df.clone().show().await?;

    let target_path =  "data.parquet";
    df.write_parquet(
        target_path,
        DataFrameWriteOptions::new(),
        None, // writer_options
    ).await;

    let df = ctx.read_parquet("data.parquet", ParquetReadOptions::new()).await?;
    df.show().await?;

    Ok(())
}