use deltalake::{open_table, DeltaTable};
use std::path::{Path, PathBuf};
use tokio;

/// Struct to encapsulate Delta table handling logic
struct TableHandler {
    table_uri: PathBuf,
}

impl TableHandler {
    /// Creates a new TableHandler
    pub fn new(table_uri: &str) -> Self {
        Self {
            table_uri: PathBuf::from(table_uri),
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

    /// Main logic to handle the table
    pub async fn handle(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_initialized() {
            println!("Opening the table for writing");
            let table = self.open_table().await?;
            println!("Delta Table opened successfully: {:?}", table.version());
        } else {
            println!("It doesn't look like our delta table has been created");
            println!("Skipping table initialization logic");
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Hardcoded table URI
    let table_uri = "DDDD";
    println!("Using the location of: {:?}", table_uri);

    // Create a TableHandler and call its handle method
    let handler = TableHandler::new(table_uri);
    handler.handle().await
}
