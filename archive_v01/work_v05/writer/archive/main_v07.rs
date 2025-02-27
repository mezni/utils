use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField, StructType};
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{protocol::SaveMode, DeltaOps};

use std::sync::Arc;
use tokio;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    let delta_ops = DeltaOps::try_from_uri("some-table").await?;
    let mut table = delta_ops
        .create()
        .with_table_name("some-table")
        .with_save_mode(SaveMode::Append)
        .with_columns(
            StructType::new(vec![
                StructField::new(
                    "num".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    true,
                ),
                StructField::new(
                    "letter".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    true,
                ),
            ])
            .fields()
            .cloned(),
        )
        .with_property("delta.writter.invariants.enabled", "false")
        .await?;

    println!("Delta table created successfully!");
    let mut record_batch_writer = deltalake::writer::RecordBatchWriter::for_table(&mut table)?;

    record_batch_writer
        .write(RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("num", ArrowDataType::Utf8, true),
                Field::new("letter", ArrowDataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?)
        .await?;
    record_batch_writer.flush_and_commit(&mut table).await?;
    Ok(())
}
