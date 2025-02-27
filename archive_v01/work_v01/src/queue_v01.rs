use sled::{Db, IVec};
use std::thread;
use std::time::Duration;

struct MessageQueue {
    db: Db,
}

impl MessageQueue {
    fn new(path: &str) -> Self {
        let db = sled::open(path).expect("Unable to open database");
        Self { db }
    }

    fn push(&self, message: &str) {
        self.db.insert(self.db.generate_id().unwrap(), message.as_bytes()).expect("Failed to push message");
    }

    fn pop(&self) -> Option<String> {
        let mut iter = self.db.iter();
        if let Some((key, value)) = iter.next() {
            self.db.remove(key).expect("Failed to remove message");
            return String::from_utf8(value.to_vec()).ok();
        }
        None
    }
}

fn main() {
    let queue = MessageQueue::new("my_queue");

    // Producer
    let producer = thread::spawn({
        let queue = queue.clone();
        move || {
            for i in 0..10 {
                queue.push(&format!("Message {}", i));
                println!("Produced: Message {}", i);
                thread::sleep(Duration::from_millis(100));
            }
        }
    });

    // Consumer
    let consumer = thread::spawn({
        let queue = queue.clone();
        move || {
            for _ in 0..10 {
                if let Some(message) = queue.pop() {
                    println!("Consumed: {}", message);
                }
                thread::sleep(Duration::from_millis(150));
            }
        }
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}