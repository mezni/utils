use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Initialize execution context
    let ctx = SessionContext::new();

    // Define schema
    let schema = create_schema();

    // Create and write the "open" table
    let open_batch = create_record_batch_open(&schema)?;
    process_table(&ctx, "open", open_batch, "open.parquet").await?;

    // Create and write the "close" table
    let close_batch = create_record_batch_close(&schema)?;
    process_table(&ctx, "close", close_batch, "close.parquet").await?;

    // Read the "open" Parquet file into a DataFrame
    let df_open = read_parquet_to_dataframe(&ctx, "open.parquet").await?;
    df_open.clone().show().await?;
    // Read the "close" Parquet file into a DataFrame
    let df_close = read_parquet_to_dataframe(&ctx, "close.parquet").await?;
    let df_close = df_close.with_column_renamed("session-id-32", "session-id-32_close")?;
    let df_close = df_close.with_column_renamed("timestamp", "timestamp_close")?;
    let df_close = df_close.with_column_renamed("source-address", "source-address_close")?;
    df_close.clone().show().await?;
    let df_joined = df_open.join(
        df_close,
        JoinType::Inner,          // join type
        &["session-id-32"],       // left keys
        &["session-id-32_close"], // right keys
        None,                     // filter
    )?;
    df_joined.clone().show().await?;
    println!("DataFrames read from Parquet files");

    Ok(())
}

/// Define the schema for the data
fn create_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("session-id-32", DataType::Utf8, false),
        Field::new("source-address", DataType::Utf8, true),
    ]))
}

/// Create a record batch for the "open" dataset
fn create_record_batch_open(schema: &Arc<Schema>) -> datafusion::error::Result<RecordBatch> {
    create_record_batch(
        schema,
        vec![
            "2019-12-27T09:48:23.298Z",
            "2019-12-27T09:48:23.398Z",
            "2019-12-27T09:48:23.498Z",
            "2019-12-27T09:48:23.598Z",
        ],
    )
}

/// Create a record batch for the "close" dataset
fn create_record_batch_close(schema: &Arc<Schema>) -> datafusion::error::Result<RecordBatch> {
    create_record_batch(
        schema,
        vec![
            "2019-12-27T09:48:24.298Z",
            "2019-12-27T09:48:24.398Z",
            "2019-12-27T09:48:24.498Z",
            "2019-12-27T09:48:24.598Z",
        ],
    )
}

/// Helper to create a record batch from timestamp data
fn create_record_batch(
    schema: &Arc<Schema>,
    timestamps: Vec<&str>,
) -> datafusion::error::Result<RecordBatch> {
    let timestamp_values = Arc::new(StringArray::from(timestamps));
    let session_id_values = Arc::new(StringArray::from(vec![
        "23232322", "23232323", "23232324", "23232325",
    ]));
    let source_address_values = Arc::new(StringArray::from(vec![
        "21.56.78.2",
        "21.56.78.3",
        "21.56.78.4",
        "21.56.78.5",
    ]));

    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![timestamp_values, session_id_values, source_address_values],
    )?)
}

/// Process a table: register it and write it to a Parquet file
async fn process_table(
    ctx: &SessionContext,
    table_name: &str,
    batch: RecordBatch,
    output_path: &str,
) -> datafusion::error::Result<()> {
    // Register the batch as a table
    let schema = batch.schema();
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table(table_name, Arc::new(table))?;

    // Write to Parquet
    let df = ctx.table(table_name).await?;
    let options = DataFrameWriteOptions::new();
    df.write_parquet(output_path, options, None).await?;

    println!("Table '{}' written to '{}'.", table_name, output_path);
    Ok(())
}

/// Read a Parquet file into a DataFrame
async fn read_parquet_to_dataframe(
    ctx: &SessionContext,
    file_path: &str,
) -> datafusion::error::Result<DataFrame> {
    // Create an empty ParquetReadOptions object
    let options = ParquetReadOptions::default();

    // Read the Parquet file into a DataFrame
    let df = ctx.read_parquet(file_path, options).await?;

    Ok(df)
}
