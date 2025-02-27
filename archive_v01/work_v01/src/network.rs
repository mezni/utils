use sled::{Db, Tree};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use bincode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkTechnology {
    id: u8,
    name: String,
    description: String,
}

pub struct NetworkTechnologyRepository {
    db: Db,
}

impl NetworkTechnologyRepository {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    pub fn create(&self, tech: NetworkTechnology) -> Result<(), sled::Error> {
        let key = tech.id.to_string();
        let value = bincode::serialize(&tech)?;
        self.db.insert(key, value)?;
        self.db.flush()?;
        Ok(())
    }

    pub fn read_by_name(&self, name: &str) -> Result<Vec<NetworkTechnology>, sled::Error> {
        let mut results = Vec::new();
        for item in self.db.iter() {
            let (_, value) = item?;
            let tech: NetworkTechnology = bincode::deserialize(&value)?;
            if tech.name == name {
                results.push(tech);
            }
        }
        Ok(results)
    }


    pub fn read_all(&self) -> Result<HashMap<u8, NetworkTechnology>, sled::Error> {
        let mut all_techs: HashMap<u8, NetworkTechnology> = HashMap::new();
        let mut iterator = self.db.tree().roots().next().unwrap().scan(0, |state, record| {
            if record.is_some() {
                *state += 1;
                if *state % 1000 == 0 { 
                    drop(record); 
                    return Some(Ok(0));
                }
            }
            record
        })?;

        for (key, value) in iterator {
            let tech: NetworkTechnology = bincode::deserialize(&value)?;
            all_techs.insert(tech.id, tech);
        }
        Ok(all_techs)
    }
}


pub struct NetworkTechnologyService {
    repository: NetworkTechnologyRepository,
}

impl NetworkTechnologyService {
    pub fn new(repository: NetworkTechnologyRepository) -> Self {
        Self { repository }
    }

    pub fn add_technology(&self, tech: NetworkTechnology) -> Result<String, String> {
        match self.repository.create(tech) {
            Ok(_) => Ok("Technology added successfully.".to_string()),
            Err(e) => Err(format!("Failed to add technology: {}", e)),
        }
    }

    pub fn find_technologies_by_name(&self, name: &str) -> Result<Vec<NetworkTechnology>, String> {
        match self.repository.read_by_name(name) {
            Ok(techs) => Ok(techs),
            Err(e) => Err(format!("Failed to search technologies: {}", e)),
        }
    }

    pub fn get_all_technologies(&self) -> Result<HashMap<u8, NetworkTechnology>, String> {
        match self.repository.read_all() {
            Ok(techs) => Ok(techs),
            Err(e) => Err(format!("Failed to retrieve technologies: {}", e)),
        }
    }
}

fn main() -> Result<(), sled::Error> {
    let db = sled::open("my_db")?;
    let repository = NetworkTechnologyRepository::new(db);
    let service = NetworkTechnologyService::new(repository);

    // Add technologies using the service
    for i in 0..5 {
        let tech = NetworkTechnology {
            id: i,
            name: format!("Network Technology {}", i % 3),  // Create duplicate names
            description: format!("Description for Network Technology {}", i).to_string(),
        };
        service.add_technology(tech)?;
    }

    // Retrieve all technologies
    let all_techs = service.get_all_technologies()?;
    println!("All Technologies: {:?}", all_techs);

    // Find technologies by name
    let searched_name = "Network Technology 1";
    let found_techs = service.find_technologies_by_name(searched_name)?;
    println!("Found technologies named '{}': {:?}", searched_name, found_techs);

    Ok(())
}   