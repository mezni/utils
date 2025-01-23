use deltalake::arrow::{
    array::{StringArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use std::sync::Arc;

fn create_batch() {
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
    let session_id_values = StringArray::from(vec![
        "23232322",
        "23232323",
        "23232324",
        "23232325",
    ]);
    let source_address_values = StringArray::from(vec![
        "21.56.78.2",
        "21.56.78.3",
        "21.56.78.4",
        "21.56.78.5",
    ]);

    // Create a RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(timestamp_values),
            Arc::new(session_id_values),
            Arc::new(source_address_values),
        ],
    )
    .expect("Failed to create RecordBatch");

    // Print the schema
    println!("Schema:\n{:#?}", schema);

    // Print RecordBatch data row-by-row
    println!("\nRecordBatch:");
    for i in 0..batch.num_rows() {
        let timestamp = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(i);
        let session_id = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(i);
        let source_address = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(i);
        println!(
            "Row {}: timestamp = {}, session-id = {}, source-address = {}",
            i, timestamp, session_id, source_address
        );
    }
}


fn main() {
    create_batch() 
}