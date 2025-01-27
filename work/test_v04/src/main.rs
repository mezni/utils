use sled::{Db, Tree};
use std::path::Path;
use serde_json;
use std::collections::HashMap;
use tokio;

pub struct CacheManager {
    db: Db,
    tables: HashMap<String, Tree>,
}

impl CacheManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(CacheManager {
            db,
            tables: HashMap::new(),
        })
    }

    fn get_or_create_table(&mut self, name: &str) -> Result<Tree, sled::Error> {
        if let Some(tree) = self.tables.get(name) {
            return Ok(tree.clone());
        }
        let tree = self.db.open_tree(name)?;
        self.tables.insert(name.to_string(), tree.clone());
        Ok(tree)
    }

    async fn insert_async<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, table: &str, key: K, value: V) -> Result<(), sled::Error> {
        let tree = self.get_or_create_table(table)?;
        let value = value.as_ref().to_vec();
        tree.insert(key, value)?;
        Ok(())
    }

    async fn get_async<K: AsRef<[u8]>>(&mut self, table: &str, key: K) -> Result<Option<Vec<u8>>, sled::Error> {
        let tree = self.get_or_create_table(table)?;
        let value = tree.get(key)?;
        let value = value.map(|ivec| ivec.to_vec());
        Ok(value)
    }

    async fn delete_async<K: AsRef<[u8]>>(&mut self, table: &str, key: K) -> Result<(), sled::Error> {
        let tree = self.get_or_create_table(table)?;
        tree.remove(key)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), sled::Error> {
    let mut manager = CacheManager::new("my_cache")?;

    let user_key = "1";
    let user_value = "John Doe";
    manager.insert_async("users", user_key, user_value).await?;

    let user_value = manager.get_async("users", user_key).await?.unwrap();
    println!("User Value: {}", String::from_utf8(user_value).unwrap());

    manager.delete_async("users", user_key).await?;

    let item: HashMap<String, String> = [
        ("session_id".to_string(), "233".to_string()),
        ("timestamp".to_string(), "2014".to_string()),
        ("type".to_string(), "open".to_string())
    ].iter().cloned().collect();

    let session_key = item.get("session_id").unwrap().as_str();

    match serde_json::to_vec(&item) {
        Ok(session_value) => {
            manager.insert_async("sessions", session_key, session_value).await?;
            let session_value = manager.get_async("sessions", session_key).await?.unwrap();
            println!("Session Value: {}", String::from_utf8(session_value).unwrap());
        }
        Err(err) => {
            eprintln!("Error serializing item to JSON: {}", err);
        }
    }

    Ok(())
}