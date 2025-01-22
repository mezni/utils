// src/main.rs

mod queue;

use queue::Queue;

fn main() {
    let mut queue = Queue::new("queue_db");

    queue.enqueue("First Task".to_string()).expect("Enqueue failed");
    queue.enqueue("Second Task".to_string()).expect("Enqueue failed");

    while let Ok(Some(task)) = queue.dequeue() {
        println!("Dequeued: {}", task);
    }
}