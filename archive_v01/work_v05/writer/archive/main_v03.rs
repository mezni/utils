use std::collections::HashMap;

use deltalake::{
    kernel::{DataType, PrimitiveType, StructField, StructType},
    open_table,
    protocol::SaveMode,
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps, DeltaTable, DeltaTableBuilder, DeltaTableError,
};


fn table_schema() -> StructType {
    StructType::new(vec![
        StructField::new(
            "session_id",
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "timestamp",
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
    ])
}

#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    let schema = table_schema();
    let table = DeltaOps::try_from_uri("./data/http_requests")
        .await?
        .create()
        .with_table_name("http") 
        .with_comment("HTTP Request logs")
        .with_columns(schema.fields().cloned()) 
        .with_save_mode(SaveMode::Append) 
        .with_partition_columns(vec!["session_id".to_string()])
        .await?;



    println!("{}", table);

    Ok(())
}