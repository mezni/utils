use serde::{Deserialize};
use std::fs::File;
use std::io::{self, Read};

#[derive(Debug, Deserialize)]
struct FlowConfig {
    condition: String,
    tokens: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Config {
    close: FlowConfig,
    open: FlowConfig,
}

/// Reads a YAML file and deserializes it into a `Config` struct.
///
/// # Arguments
/// * `file_path` - The path to the YAML file.
///
/// # Returns
/// * `Result<Config, Box<dyn std::error::Error>>`
fn read_config(file_path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    // Open the YAML file
    let mut file = File::open(file_path)?;
    
    // Read the file contents into a string
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    
    // Deserialize the YAML content into the Config struct
    let config: Config = serde_yaml::from_str(&contents)?;
    
    Ok(config)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read the YAML configuration
    let config = read_config("config.yaml")?;
    
    // Print the parsed configuration
    println!("{:#?}", config);

    Ok(())
}
