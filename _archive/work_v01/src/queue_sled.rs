use sled::{Db, IVec};
use serde::{Serialize, Deserialize};
use serde_json::{Value, from_value};
use std::path::Path;
use chrono::{Utc};

pub struct SledManager {
    db: Db,
}

impl SledManager {
    /// Creates a new SledManager, opening (or creating) a database at the given path
    pub fn new<P: AsRef<Path>>(path: P) -> sled::Result<Self> {
        let db = sled::open(path)?;
        Ok(SledManager { db })
    }

    /// Insert a JSON value
    pub fn insert_json<T: Serialize>(&self, key: &str, value: &T) -> sled::Result<Option<IVec>> {
        let json = serde_json::to_vec(value)?;
        self.db.insert(key.as_bytes(), json)
    }

    /// Get a value by key and deserialize it from JSON
    pub fn get_json<T: Deserialize<'static>>(&self, key: &str) -> sled::Result<Option<T>> {
        if let Some(value) = self.db.get(key.as_bytes())? {
            let json: T = serde_json::from_slice(&value)?;
            Ok(Some(json))
        } else {
            Ok(None)
        }
    }

    /// Remove a key-value pair
    pub fn remove(&self, key: &str) -> sled::Result<Option<IVec>> {
        self.db.remove(key.as_bytes())
    }

    /// Flush the database
    pub fn flush(&self) -> sled::Result<()> {
        self.db.flush() // Ensures data is written to disk
    }

    /// Execute a transaction with a closure
    pub fn transaction<F>(&self, f: F) -> sled::Result<()>
    where
        F: FnOnce(&sled::Transactional) -> sled::Result<()>,
    {
        self.db.transaction(f)
    }

    /// Close the database
    pub fn close(self) {
        // Sled automatically handles resource cleanup
    }
}

/// A Queue structure backed by Sled
pub struct Queue<'a> {
    manager: &'a SledManager,
    key_prefix: String,
}

impl<'a> Queue<'a> {
    pub fn new(manager: &'a SledManager, name: &str) -> Self {
        Queue {
            manager,
            key_prefix: name.to_string(),
        }
    }

    /// Enqueue an item to the queue
    pub fn enqueue<T: Serialize>(&self, item: &T) -> sled::Result<u64> {
        let id = Utc::now().timestamp_micros() as u64;
        self.manager.insert_json(&format!("{}:{}", self.key_prefix, id), item)?;
        // Store the ID for ordering
        self.manager.insert_json(&format!("{}:queue", self.key_prefix), &id)?;
        Ok(id)
    }

    /// Dequeue an item from the queue
    pub fn dequeue<T: Deserialize<'static>>(&self) -> sled::Result<Option<T>> {
        if let Some(IVec::Bytes(queue_key)) = self.manager.db.get(&format!("{}:queue", self.key_prefix))? {
            let key_bytes = queue_key.as_ref();
            if let Some(item) = self.manager.db.get(&String::from_utf8_lossy(key_bytes))? {
                let item_value: T = serde_json::from_slice(&item)?;
                
                // Remove the dequeued item from the DB
                self.manager.remove(&String::from_utf8_lossy(key_bytes))?;
                Ok(Some(item_value))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Clear the queue
    pub fn clear(&self) -> sled::Result<()> {
        let keys: Vec<String> = self
            .manager
            .db
            .iter()
            .filter_map(|result| {
                if let Ok((key, _)) = result {
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    if key_str.starts_with(&self.key_prefix) {
                        return Some(key_str);
                    }
                }
                None
            })
            .collect();

        for key in keys {
            self.manager.remove(&key)?;
        }
        Ok(())
    }
}

/// Item to store in the queue
#[derive(Serialize, Deserialize, Debug)]
struct Item {
    id: u64,
    message: Value, // Change message to serde_json::Value for dynamic JSON
}

fn main() -> sled::Result<()> {
    // Create a SledManager
    let manager = SledManager::new("my_database")?;

    // Create a queue named "items"
    let queue = Queue::new(&manager, "items");

    // Create an Item instance to store with a JSON message
    let message_json: Value = serde_json::json!({
        "text": "Hello, world!",
        "details": {
            "author": "Alice",
            "timestamp": Utc::now().to_string()
        }
    });

    let item = Item {
        id: Utc::now().timestamp_micros() as u64,
        message: message_json,
    };

    // Enqueue the item to the queue
    queue.enqueue(&item)?;

    // Dequeue the item from the queue
    if let Some(retrieved_item): Option<Item> = queue.dequeue()? {
        println!("Dequeued: {:?}", retrieved_item);
    } else {
        println!("Queue is empty.");
    }

    // Flush changes
    manager.flush()?;

    Ok(())
}