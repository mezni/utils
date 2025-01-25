use deltalake::operations::create::CreateBuilder;
use deltalake::{DeltaTable};
use std::collections::HashMap;
use deltalake::kernel::{DataType, PrimitiveType, StructField, StructType};
use std::sync::Arc;
use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType as OtherDataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
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

async fn create_table(path: String, backend_config: HashMap<String, String>) -> DeltaTable {
    let schema = table_schema();
    let builder = CreateBuilder::new()
        .with_location(path)
        .with_storage_options(backend_config)
        .with_column(
            "session_id",
            DataType::Primitive(PrimitiveType::String),
            false,
            Default::default(),
        )
        .with_column(
            "timestamp",
            DataType::Primitive(PrimitiveType::String),
            false,
            Default::default(),
        );

     builder.await.unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut s3_storage_location = "DDDD".to_string();
    let mut backend_config: HashMap<String, String> = HashMap::new();
    backend_config.insert("AWS_REGION".to_string(), "XXXX".to_string());    
    println!("Creating table");
    let table = create_table(
        s3_storage_location, backend_config
    ).await;
    println!("Table created with version : {}", table.version());



    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Utf8, false),
    ]));

    let session_ids: Vec<String> = vec!["a", "b", "c"];
    let timestamps: Vec<String> = vec!["a", "b", "c"];

    let session_id_values = StringArray::from(session_ids);
    let timestamp_values = StringArray::from(timestamps);

    RecordBatch::try_new(schema, vec![Arc::new(session_id_values), Arc::new(timestamp_values)]).unwrap()

}
