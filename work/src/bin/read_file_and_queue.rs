use sled::{Db, Tree};
use std::fs;
use std::io::{self, BufRead};
use std::time::Instant;
use tokio::task;

// Define a struct to represent a queue
pub struct SledQueue {
    db: Db,
    tree: Tree,
}

impl SledQueue {
    // Create a new queue
    pub fn new(path: &str) -> sled::Result<Self> {
        let db = sled::open(path)?;
        let tree = db.open_tree("my_queue")?;
        Ok(Self { db, tree })
    }

    // Enqueue items asynchronously in batches
    pub async fn enqueue_batch(&self, items: Vec<String>) -> sled::Result<()> {
        task::block_in_place(|| {
            for (index, item) in items.into_iter().enumerate() {
                self.tree.insert(index.to_string(), item.as_bytes())?;
            }
            Ok(())
        })
    }

    // Dequeue an item asynchronously
    pub async fn dequeue(&self) -> sled::Result<Option<String>> {
        task::block_in_place(|| {
            let front = self.tree.get("0")?;
            if front.is_none() {
                return Ok(None);
            }

            let item = self.tree.remove("0")?;

            // Shift remaining items down by one
            let len = self.tree.len(); // After removal, get current length
            for i in 0..len {
                let next_key = (i + 1).to_string();
                if let Some(next_item) = self.tree.remove(&next_key)? {
                    self.tree.insert(i.to_string(), next_item)?;
                }
            }

            Ok(item.map(|ivec| String::from_utf8_lossy(&ivec).into_owned()))
        })
    }
}

async fn file_reader(file_path: &str, queue: &SledQueue, batch_size: usize) -> io::Result<u64> {
    // Get the file size
    let metadata = fs::metadata(file_path)?;
    let file_size = metadata.len();
    println!("File size: {}", file_size);

    // Open the file
    let file = fs::File::open(file_path)?;
    let reader = io::BufReader::new(file);

    // Initialize a sum for line lengths
    let mut total_line_length = 0;
    let mut batch: Vec<String> = Vec::with_capacity(batch_size);

    // Process each line and enqueue them in batches
    for line in reader.lines() {
        let line = line?;
        total_line_length += line.len() as u64;
        batch.push(line);

        // If the batch is filled, enqueue it
        if batch.len() == batch_size {
            // Enqueue the batch and reset
            if let Err(e) = queue.enqueue_batch(batch.clone()).await {
                eprintln!("Error enqueuing batch: {}", e);
            }
            batch.clear(); // Clear the batch for the next set of lines
        }
    }

    // Enqueue any leftover lines after finishing the read
    if !batch.is_empty() {
        if let Err(e) = queue.enqueue_batch(batch).await {
            eprintln!("Error enqueuing remaining lines: {}", e);
        }
    }

    // Return the total length of lines
    Ok(total_line_length)
}

#[tokio::main]
async fn main() -> sled::Result<()> {
    let file_path = "cdr_records.csv";
    let queue = SledQueue::new("queue.db")?;

    let start_time = Instant::now();
    let batch_size = 1000; // Set your desired batch size
    file_reader(file_path, &queue, batch_size).await?;
    let end_time = Instant::now();

    let processing_time = end_time.duration_since(start_time);
    println!("Processing time: {:?}", processing_time);

    // Example of dequeueing to show that it works
    while let Some(item) = queue.dequeue().await? {
        println!("Dequeued item: {}", item);
    }

    Ok(())
}