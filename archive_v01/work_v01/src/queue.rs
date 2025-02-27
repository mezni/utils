// src/queue.rs

use sled::{Db, IVec};
use std::collections::HashMap;

pub struct Queue {
    db: Db,
    tree: sled::Tree,
    head: usize,
    tail: usize,
}

impl Queue {
    pub fn new(db_path: &str) -> Self {
        let db = sled::open(db_path).expect("Failed to open database");
        let tree = db.open_tree("queue_tree").expect("Failed to open tree");
        // Initialize head and tail
        let head = tree.get("head").unwrap_or_else(|_| Ok(IVec::from(0))).unwrap_or(IVec::from(0));
        let tail = tree.get("tail").unwrap_or_else(|_| Ok(IVec::from(0))).unwrap_or(IVec::from(0));

        let head = String::from_utf8(head.to_vec()).unwrap_or_default().parse::<usize>().unwrap_or(0);
        let tail = String::from_utf8(tail.to_vec()).unwrap_or_default().parse::<usize>().unwrap_or(0);

        Queue { db, tree, head, tail }
    }

    pub fn enqueue(&mut self, value: String) -> Result<(), String> {
        let key = self.tail.to_string();
        self.tree.insert(key.clone(), value.into()).map_err(|e| e.to_string())?;
        self.tail += 1;
        self.tree.insert("tail", self.tail.to_string().into()).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn dequeue(&mut self) -> Result<Option<String>, String> {
        if self.head < self.tail {
            let key = self.head.to_string();
            let value = self.tree.remove(key.clone()).map_err(|e| e.to_string())?;
            self.head += 1;
            self.tree.insert("head", self.head.to_string().into()).map_err(|e| e.to_string())?;
            return Ok(value.map(|v| String::from_utf8(v.to_vec()).unwrap())); // Handle UTF-8 conversion
        }
        Ok(None) // Queue is empty
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        self.db.flush().expect("Failed to flush DB");
    }
}