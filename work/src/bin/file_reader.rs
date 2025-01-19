use std::fs::{self, File};
use std::io::{self, BufRead};
use std::path::Path;

// Simulate converting bytes to a human-readable format
fn human_readable_size(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if size >= GB {
        format!("{:.2} GB", size as f64 / GB as f64)
    } else if size >= MB {
        format!("{:.2} MB", size as f64 / MB as f64)
    } else if size >= KB {
        format!("{:.2} KB", size as f64 / KB as f64)
    } else {
        format!("{} bytes", size)
    }
}

// Define the FileReader function
fn file_reader<P>(file_path: P) -> io::Result<u64>
where
    P: AsRef<Path>,
{
    // Get the file metadata to get the file size
    let metadata = fs::metadata(file_path.as_ref())?;
    let file_size = metadata.len(); // Size in bytes


    // Print the file size in a human-readable format
    //println!("File size: {}", human_readable_size(file_size));
    println!("File size: {}", file_size);
    // Open the file
    let file = File::open(file_path)?;

    // Create a buffered reader to read the file line by line
    let reader = io::BufReader::new(file);

    // Initialize a sum for line lengths
    let mut total_line_length = 0;

    // Process each line
    for line in reader.lines() {
        let line = line?; // Handle potential errors while reading lines
        total_line_length += line.len() as u64; // Sum the lengths of the lines
        // process_line(&line); // Uncomment to simulate sending the line as a "data packet"
    }

    // Return the total length of lines
    Ok(total_line_length)
}

// Simulate a downstream process
fn process_line(data_packet: &str) {
    println!("Processing data packet: {}", data_packet);
}

// Main function to execute the FileReader
fn main() -> io::Result<()> {
    // Specify the file path
    let file_path = "cdr_records.csv"; // Replace with the path to your file

    // Call the FileReader function
    match file_reader(file_path) {
        Ok(total_length) => {
            println!("Total size of all lines: {} bytes", total_length);
            println!("File reading completed successfully.");
        },
        Err(e) => eprintln!("Error reading file: {}", e),
    }

    Ok(())
}