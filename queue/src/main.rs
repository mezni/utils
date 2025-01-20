use sled::{Db, IVec};
use std::error::Error;

const QUEUES_PATH: &str = "QUEUES";

struct Queue {
    path: Db,
    queue: sled::Tree,
}

impl Queue {
    // Constructor to create a new Queue
    fn new(tree_name: &str) -> Result<Self, Box<dyn Error>> {
        // Open the Sled database
        let db = sled::open(QUEUES_PATH)?;
        // Open the specific tree within the database
        let tree = db.open_tree(tree_name)?;
        Ok(Queue { path: db, queue: tree })
    }

    // Method to enqueue an item into the queue
    fn enqueue(&self, key: &str, value: &str) -> Result<(), Box<dyn Error>> {
        // Insert the key-value pair into the tree
        self.queue.insert(key, value.as_bytes())?;
        println!("Added: {} -> {}", key, value);
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    // Create a new Queue instance with a specified tree name
    let queue = Queue::new("my_queue")?;

    // Enqueue some items into the queue
    let key1 = String::from("Key1");
    let val1 = String::from("Item 1");

    queue.enqueue(&key1, &val1)?; // Enqueue with val1
    queue.enqueue("Item2", "Item 2")?;
    queue.enqueue("Item3", "Item 3")?;

    println!("Queue initialized and items enqueued successfully!");

    Ok(())
}
