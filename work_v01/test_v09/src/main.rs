use datafusion::arrow::{
    array::{Float64Array, StringArray, Array},
};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Create some data
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let ages = Float64Array::from(vec![25.0, 30.0, 35.0]);

    // Print the arrays
    println!("Names Array:");
    println!("  Length: {}", names.len());
    println!("  Memory Usage: {} bytes", names.get_buffer_memory_size());
    println!("{:?}", names);

    println!("\nAges Array:");
    println!("  Length: {}", ages.len());
    println!("  Memory Usage: {} bytes", ages.get_buffer_memory_size());
    println!("{:?}", ages);

    Ok(())
}