use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

// Define the FileReader function
fn file_reader<P>(file_path: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    // Open the file
    let file = File::open(file_path)?;

    // Create a buffered reader to read the file line by line
    let reader = io::BufReader::new(file);

    // Process each line
    for line in reader.lines() {
        let line = line?; // Handle potential errors while reading lines
        process_line(&line); // Simulate sending the line as a "data packet"
    }

    Ok(())
}