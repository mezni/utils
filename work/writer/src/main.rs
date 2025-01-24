use arrow::{
    array::StringArray,
    datatypes::{DataType as ArrowDataType, Field, Schema},
    record_batch::RecordBatch,
};
use deltalake::{
    kernel::{DataType, PrimitiveType, StructField, StructType},
    open_table,
    protocol::SaveMode,
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps, DeltaTable, DeltaTableBuilder, DeltaTableError,
};
use std::{
    collections::HashMap,
    error::Error,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio;

/// Define the schema for the table
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

/// Struct to encapsulate Delta table handling logic
struct TableHandler {
    table_uri: PathBuf,
    table_name: String,
    schema: StructType,      // Schema of the table
    partitions: Vec<String>, // Partitioning columns
}

impl TableHandler {
    /// Creates a new TableHandler
    pub fn new(
        table_uri: &str,
        table_name: &str,
        schema: StructType,
        partitions: Vec<String>,
    ) -> Self {
        Self {
            table_uri: PathBuf::from(table_uri),
            table_name: table_name.to_string(),
            schema,
            partitions,
        }
    }

    /// Checks if the table is initialized
    pub fn is_initialized(&self) -> bool {
        self.table_uri.join("_delta_log").is_dir()
    }

    /// Opens the Delta table
    pub async fn open_table(&self) -> Result<DeltaTable, Box<dyn std::error::Error>> {
        let table_path = self
            .table_uri
            .to_str()
            .expect("Could not convert table path to a string");
        open_table(table_path).await.map_err(|e| e.into())
    }

    /// Creates a new Delta table
    pub async fn create_table(&self) -> Result<DeltaTable, Box<dyn std::error::Error>> {
        let table_path = self
            .table_uri
            .to_str()
            .expect("Could not convert table path to a string");
        let delta_ops = DeltaOps::try_from_uri(table_path).await?;

        // Create the table with schema and partitioning
        let mut table = delta_ops
            .create()
            .with_table_name(&self.table_name) // Use the table name here
            .with_save_mode(SaveMode::Append)
            .with_columns(self.schema.fields().cloned()) // Use provided schema
            //            .with_partitioning(self.partitions.clone())  // Add partitioning
            .await?;

        println!(
            "Delta Table '{}' created successfully at: {} with partitions {:?}",
            self.table_name, table_path, self.partitions
        );
        Ok(table)
    }

    /// Main logic to handle the table and return the DeltaTable instance
    pub async fn handle(&self) -> Result<DeltaTable, Box<dyn std::error::Error>> {
        if self.is_initialized() {
            println!("Opening the table '{}' for writing", self.table_name);
            let table = self.open_table().await?;
            println!(
                "Delta Table '{}' opened successfully: {:?}",
                self.table_name,
                table.version()
            );
            Ok(table)
        } else {
            println!("It doesn't look like our delta table has been created");
            let table = self.create_table().await?;
            println!(
                "Created a new Delta Table '{}' with version: {:?} and partitions {:?}",
                self.table_name,
                table.version(),
                self.partitions
            );
            Ok(table)
        }
    }
}

async fn write_data_to_table(table: &mut DeltaTable) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", ArrowDataType::Utf8, false),
        Field::new("timestamp", ArrowDataType::Utf8, false),
    ]));

    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(StringArray::from(vec!["a", "a", "a"])),
        ],
    )?;

    let mut record_batch_writer = RecordBatchWriter::for_table(table)?;
    record_batch_writer.write(record_batch).await?;
    record_batch_writer.flush_and_commit(table).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Hardcoded table URI, table name, schema, and partitions (you should replace these with valid values)
    let table_uri = "DDDD"; // Ensure this path is valid
    let table_name = "some-table"; // Define the table name

    // Define the schema (example schema provided)
    let schema = table_schema(); // Use the previously defined schema

    // Define the partitions (example: partition by session_id)
    let partitions = vec!["session_id".to_string()];

    println!("Using the location of: {:?}", table_uri);

    // Create a TableHandler with the table name, schema, and partitions, and call its handle method
    let handler = TableHandler::new(table_uri, table_name, schema, partitions);

    // Get the DeltaTable instance from handle()
    let mut table = handler.handle().await?;

    // Write data to the table
    write_data_to_table(&mut table).await?;

    Ok(())
}
