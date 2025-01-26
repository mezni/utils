mod generator;
use generator::syslog::{SyslogMessage, SyslogMessageBatch};

use serde::{Deserialize, Serialize};
use serde_json;
use sled::{open, Db, Tree};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("Sled error: {0}")]
    Sled(#[from] sled::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub struct Cache {
    inner_db: Db,
    trees: Mutex<HashMap<String, Tree>>,
}

impl Cache {
    pub fn new(path: &str) -> Result<Self, CacheError> {
        let db = open(path)?;
        Ok(Self {
            inner_db: db,
            trees: Mutex::new(HashMap::new()),
        })
    }

    pub fn create_table(&self, table_name: &str) -> Result<Tree, CacheError> {
        let mut trees = self.trees.lock().unwrap();
        if let Some(tree) = trees.get(table_name) {
            return Ok(tree.clone());
        }
        let tree = self.inner_db.open_tree(table_name)?;
        trees.insert(table_name.to_string(), tree.clone());
        Ok(tree)
    }

    pub fn table_length(&self, table_name: &str) -> Result<usize, CacheError> {
        let tree = self.create_table(table_name)?;
        Ok(tree.len())
    }

    pub fn insert<T: Serialize>(&self, table_name: &str, key: &str, value: &T) -> Result<(), CacheError> {
        let tree = self.create_table(table_name)?;
        let serialized = serde_json::to_vec(value)?;
        tree.insert(key.as_bytes(), serialized)?;
        Ok(())
    }

    pub fn get<T: for<'de> Deserialize<'de>>(&self, table_name: &str, key: &str) -> Result<Option<T>, CacheError> {
        let tree = self.create_table(table_name)?;
        if let Some(value) = tree.get(key.as_bytes())? {
            let deserialized: T = serde_json::from_slice(&value)?;
            Ok(Some(deserialized))
        } else {
            Ok(None)
        }
    }

    pub fn find<T: for<'de> Deserialize<'de>>(&self, table_name: &str, key: &str) -> Result<Option<T>, CacheError> {
        self.get(table_name, key)
    }

    pub fn delete_key(&self, table_name: &str, key: &str) -> Result<(), CacheError> {
        let tree = self.create_table(table_name)?;
        tree.remove(key.as_bytes())?;
        Ok(())
    }

    pub fn close(&self) {
        if let Err(e) = self.inner_db.flush() {
            eprintln!("Failed to flush database: {}", e);
        }
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        self.close();
    }
}

fn syslog_message_to_hashmap(message: &SyslogMessage) -> Result<HashMap<String, String>, serde_json::Error> {
    let json_value = serde_json::to_value(message)?;
    if let serde_json::Value::Object(map) = json_value {
        Ok(map.into_iter()
            .map(|(k, v)| (k, v.as_str().unwrap_or_default().to_string()))
            .collect())
    } else {
        Err(serde_json::Error::custom("Invalid JSON value"))
    }
}

fn main() -> Result<(), CacheError> {
    let cache = Cache::new("cache_db")?;
    insert_user(&cache)?;
    retrieve_user(&cache)?;
    println!("Table length: {}", cache.table_length("users")?);
    find_user(&cache)?;

    for i in 0..5 {
        let mut batch = SyslogMessageBatch::new();
        let messages = batch.generate();

        for message in messages {
            let hashmap = syslog_message_to_hashmap(&message)?;
            cache.insert("messages", hashmap.get("session_id").unwrap(), &hashmap)?;
        }
    }

    Ok(())
}


fn insert_user(cache: &Cache) -> Result<(), CacheError> {
    // Insert the HashMap into a "users" table
    let mut user_map = HashMap::new();
    user_map.insert("id", "1");
    user_map.insert("name", "John Doe");
    user_map.insert("email", "johndoe@example.com");
    cache.insert("users", "user_map", &user_map)?;
    println!("HashMap inserted into the cache.");
    Ok(())
}

fn retrieve_user(cache: &Cache) -> Result<(), CacheError> {
    // Retrieve the HashMap from the "users" table
    if let Some(retrieved_map) = cache.get::<HashMap<String, String>>("users", "user_map")? {
        println!("Retrieved HashMap: {:?}", retrieved_map);
    } else {
        println!("HashMap not found.");
    }
    Ok(())
}

fn find_user(cache: &Cache) -> Result<(), CacheError> {
    // Find the HashMap from the "users" table
    if let Some(found_map) = cache.find::<HashMap<String, String>>("users", "user_map")? {
        println!("Found HashMap: {:?}", found_map);
    } else {
        println!("HashMap not found.");
    }
    Ok(())
}