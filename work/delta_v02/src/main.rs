use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::collect_sendable_stream;
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use deltalake::{protocol::SaveMode, DeltaOps};

use std::sync::Arc;

fn create_batch() -> RecordBatch {
    // Define the schema
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("timestamp", ArrowDataType::Utf8, false),
        Field::new("session-id-32", ArrowDataType::Utf8, false),
        Field::new("source-address", ArrowDataType::Utf8, false),
    ]));

    // Create arrays
    let timestamp_values = StringArray::from(vec![
        "2019-12-27T09:48:23.298Z",
        "2019-12-27T09:48:23.398Z",
        "2019-12-27T09:48:23.498Z",
        "2019-12-27T09:48:23.598Z",
    ]);
    let session_id_values = StringArray::from(vec!["23232322", "23232323", "23232324", "23232325"]);
    let source_address_values =
        StringArray::from(vec!["21.56.78.2", "21.56.78.3", "21.56.78.4", "21.56.78.5"]);

    // Create a RecordBatch
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(timestamp_values),
            Arc::new(session_id_values),
            Arc::new(source_address_values),
        ],
    )
    .unwrap()
}

fn create_schema() -> Vec<StructField> {
    vec![
        StructField::new(
            String::from("timestamp"),
            DataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            String::from("session-id-32"),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            String::from("source-address"),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
    ]
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    let table_uri = Some("tablee"); // Replace with your desired value or None
    let ops = if let Some(uri) = table_uri {
        DeltaOps::try_from_uri(uri).await?
    } else {
        DeltaOps::new_in_memory()
    };

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let table = ops
        .create()
        .with_columns(create_schema())
        //.with_partition_columns(["timestamp"])
        .with_table_name("my_table")
        .with_comment("A table to show how delta-rs works")
        .await?;

    let batch = create_batch();

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    // To overwrite instead of append (which is the default), use `.with_save_mode`:
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Overwrite)
        .with_writer_properties(writer_properties)
        .await?;

    Ok(())
}
