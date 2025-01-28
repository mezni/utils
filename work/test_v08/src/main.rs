use datafusion::prelude::*;
use datafusion::error::Result;
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a session context
    let ctx = SessionContext::new();

    // Read the Parquet file asynchronously
    let parquet_file = "../minidl/RAW/CLOSE20250128195648.parquet";
    let df = ctx.read_parquet(parquet_file, ParquetReadOptions::new()).await?;

    // Show the DataFrame (useful for debugging)
    df.show().await?;

    Ok(())
}

