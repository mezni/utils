use sled::{Db, IVec, Error as SledError};
use std::{error::Error, thread, time::Duration};
use std::fmt;
use serde::{Serialize, Deserialize};
use serde_json;
use tokio::sync::Mutex;


// Define a custom error type
#[derive(Debug)]
pub enum QueueError {
    Sled(SledError),
    Utf8(std::string::FromUtf8Error),
    ParseInt(std::num::ParseIntError),
    SerdeJson(serde_json::Error),
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueueError::Sled(e) => write!(f, "Sled error: {}", e),
            QueueError::Utf8(e) => write!(f, "Utf8 error: {}", e),
            QueueError::ParseInt(e) => write!(f, "Parse int error: {}", e),
            QueueError::SerdeJson(e) => write!(f, "Serde JSON error: {}", e),
        }
    }
}

impl Error for QueueError {}

impl From<SledError> for QueueError {
    fn from(e: SledError) -> Self {
        QueueError::Sled(e)
    }
}

impl From<std::string::FromUtf8Error> for QueueError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        QueueError::Utf8(e)
    }
}

impl From<std::num::ParseIntError> for QueueError {
    fn from(e: std::num::ParseIntError) -> Self {
        QueueError::ParseInt(e)
    }
}

impl From<serde_json::Error> for QueueError {
    fn from(e: serde_json::Error) -> Self {
        QueueError::SerdeJson(e)
    }
}

const QUEUES_PATH: &str = "QUEUES";

// Define a Message struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: usize,
    pub content: String,
}

impl Message {
    // Constructor to create a new Message
    pub fn new(id: usize, content: &str) -> Self {
        Message {
            id,
            content: content.to_string(),
        }
    }
}

// Define a Queue struct
pub struct Queue {
    db: Db,           // Sled Database
    tree: sled::Tree, // Sled Tree for storing the queue
    head: usize,      // The index of the first element (FIFO)
    tail: usize,      // The index of the next empty slot
    mutex: Mutex<()>, // Mutex for async access
}

impl Queue {
    // Constructor to create a new Queue
    pub async fn new(tree_name: &str) -> Result<Self, QueueError> {
        let db = sled::open(QUEUES_PATH)?;
        let tree = db.open_tree(tree_name)?;

        // Load the head and tail indices, or default to 0 if not present
        let head = tree.get("head")?.map(|v| String::from_utf8(v.to_vec()).unwrap()).unwrap_or("0".to_string()).parse::<usize>()?;
        let tail = tree.get("tail")?.map(|v| String::from_utf8(v.to_vec()).unwrap()).unwrap_or("0".to_string()).parse::<usize>()?;

        // Return a Queue instance
        Ok(Queue {
            db,
            tree,
            head,
            tail,
            mutex: Mutex::new(()),
        })
    }

// Method to enqueue a Message into the queue
pub async fn enqueue(&mut self, message: &Message) -> Result<(), QueueError> {
    let _lock = self.mutex.lock().await;
    let key = self.tail.to_string(); // The key will be the tail index
    // Insert the message, which needs to be converted into IVec (bytes)
    let message_bytes = serde_json::to_vec(message)?;
    self.tree.insert(key.clone(), IVec::from(message_bytes))?;

    // Increment the tail and save it
    let new_tail = self.tail + 1;
    self.tree.insert("tail", IVec::from(new_tail.to_string().as_bytes()))?;
    self.tail = new_tail;
    Ok(())
}

// Method to dequeue a Message from the queue
pub async fn dequeue(&mut self) -> Result<Option<Message>, QueueError> {
    let _lock = self.mutex.lock().await;
    if self.head < self.tail {
        let key = self.head.to_string(); // The key will be the head index
        let message_bytes = self.tree.remove(key.clone())?.map(|v| v.to_vec());

        // Increment the head and save it
        let new_head = self.head + 1;
        self.tree.insert("head", IVec::from(new_head.to_string().as_bytes()))?;
        self.head = new_head;

        // Return the dequeued message, if any
        if let Some(message_bytes) = message_bytes {
            return Ok(Some(serde_json::from_slice(&message_bytes)?));
        }
    }
    Ok(None) // Return None if the queue is empty
}

    pub async fn size_in_bytes(&self) -> Result<usize, QueueError> {
        let _lock = self.mutex.lock().await;
        let mut size = 0;
        for key in self.tree.iter().keys() {
            if let Ok(key) = key {
                if key != "head" && key != "tail" {
                    if let Ok(value) = self.tree.get(key) {
                        size += value.as_slice().len();
                    }
                }
            }
        }
        Ok(size)
    }

    // Method to get the length of the queue
    pub async fn len(&self) -> Result<usize, QueueError> {
        let _lock = self.mutex.lock().await;
        Ok(self.tail - self.head)
    }
}

// Ensure that the database is flushed when the queue is dropped
impl Drop for Queue {
    fn drop(&mut self) {
        self.db.flush().unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), QueueError> {
    // Create a new queue instance
    let mut queue = Queue::new("queue_db").await?;

    // Enqueue some messages
    let message1 = Message::new(1, "First Task");
    let message2 = Message::new(2, "Second Task");
    queue.enqueue(&message1).await?;
    queue.enqueue(&message2).await?;

    let queue_size = queue.size_in_bytes().await?;
    println!("Queue size: {} bytes", queue_size);

//    println!("Sleeping for 2 seconds...");
//    thread::sleep(Duration::from_secs(2));

    // Print the length of the queue
    println!("Queue length: {}", queue.len().await?);

    // Dequeue and print the messages
    while let Some(message) = queue.dequeue().await? {
        println!("Dequeued: {:?}", message);
    }

    Ok(())
}