use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};
use serde_json;
use sled::{Db, IVec, Tree, Error};
use std::error::Error as StdError;
use std::fmt;

// Custom error type
#[derive(Debug)]
enum CacheError {
    SledError(Error),
    SerializationError(serde_json::Error),
    DeserializationError(serde_json::Error),
    CacheError(String),
}

impl StdError for CacheError {}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CacheError::SledError(err) => write!(f, "Sled error: {}", err),
            CacheError::SerializationError(err) => write!(f, "Serialization error: {}", err),
            CacheError::DeserializationError(err) => write!(f, "Deserialization error: {}", err),
            CacheError::CacheError(msg) => write!(f, "Cache error: {}", msg),
        }
    }
}

impl From<Error> for CacheError {
    fn from(err: Error) -> Self {
        CacheError::SledError(err)
    }
}

impl From<serde_json::Error> for CacheError {
    fn from(err: serde_json::Error) -> Self {
        CacheError::SerializationError(err)
    }
}

#[derive(Clone)]
pub struct CacheWrapper {
    db: Arc<RwLock<Db>>,
    tables: Arc<RwLock<BTreeMap<String, Tree>>>,
}

impl CacheWrapper {
    // Initialize a new database
    pub fn new(path: &str) -> Result<Self, CacheError> {
        let db = sled::open(path)?;
        let tables = Arc::new(RwLock::new(BTreeMap::new()));
        Ok(CacheWrapper {
            db: Arc::new(RwLock::new(db)),
            tables,
        })
    }

    // Create a new table (a new Tree in Sled) or directly access the table
    pub fn get_table(&self, table_name: &str) -> Result<Tree, CacheError> {
        let db = self.db.read().unwrap();
        let mut tables = self.tables.write().unwrap();

        // If the table is not cached, open it and insert it into the cache
        if !tables.contains_key(table_name) {
            let tree = db.open_tree(table_name)?;
            tables.insert(table_name.to_string(), tree.clone());
        }

        Ok(tables.get(table_name).unwrap().clone())
    }

    // Insert a key-value pair into a specified table (Tree)
    pub fn insert<T: Serialize>(&self, table_name: &str, key: &[u8], value: T) -> Result<(), CacheError> {
        let tree = self.get_table(table_name)?;
        let json_value = serde_json::to_vec(&value)?;
        tree.insert(key, json_value.as_slice())?;
        Ok(())
    }

    // Remove a key-value pair from a specified table (Tree)
    pub fn remove(&self, table_name: &str, key: &[u8]) -> Result<Option<IVec>, CacheError> {
        let tree = self.get_table(table_name)?;
        Ok(tree.remove(key)?)
    }

    // Find all key-value pairs in a specific table (Tree)
    pub fn find<T: Deserialize<'static>>(&self, table_name: &str) -> Result<Vec<(Vec<u8>, T)>, CacheError> {
        let tree = self.get_table(table_name)?;
        let mut results = Vec::new();
        for item in tree.iter() {
            let (key, value) = item?;
            let deserialized_value: T = serde_json::from_slice(&value.clone())?; // Cloned value to avoid lifetime issue
            results.push((key.to_vec(), deserialized_value));
        }
        Ok(results)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct User {
    name: String,
    email: String,
}

fn main() -> Result<(), CacheError> {
    let db = CacheWrapper::new("my_db")?;

    // Insert data into the "users" table
    db.insert("users", b"user1", User {
        name: "John Doe".to_string(),
        email: "john.doe@example.com".to_string(),
    })?;

    // Find all data in the "users" table
    let all_users: Vec<(Vec<u8>, User)> = db.find("users")?;
    println!("All users: {:?}", all_users);

    // Remove the "user1" entry from the "users" table
    db.remove("users", b"user1")?;

    // Find all data in the "users" table after removal
    let all_users_after_removal: Vec<(Vec<u8>, User)> = db.find("users")?;
    println!("All users after removal: {:?}", all_users_after_removal);

    Ok(())
}
