use serde::{Deserialize};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct FlowConfig {
    pub condition: String,
    pub tokens: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub close: FlowConfig,
    pub open: FlowConfig,
}

/// Reads a YAML file and deserializes it into a `Config` struct.
/// It also checks if the file exists before attempting to open it.
/// 
/// # Arguments
/// * `file_path` - The path to the YAML file.
/// 
/// # Returns
/// * `Result<Config, Box<dyn std::error::Error>>`
pub fn read_config(file_path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    // Check if the file exists
    let path = Path::new(file_path);
    if !path.exists() {
        return Err(Box::new(io::Error::new(io::ErrorKind::NotFound, "File not found")));
    }
    
    // Open the YAML file
    let mut file = File::open(file_path)?;
    
    // Read the file contents into a string
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    
    // Deserialize the YAML content into the Config struct
    let config: Config = serde_yaml::from_str(&contents)?;
    
    Ok(config)
}
