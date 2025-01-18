use std::collections::HashSet;
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct NetworkTechnologyDTO {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkTechnology {
    pub id: u32,
    pub name: String,
    pub description: String,
}

impl NetworkTechnology {
    pub fn new(id: u32, name: &str, description: &str) -> Self {
        if name.is_empty() || description.is_empty() {
            panic!("Name and description cannot be empty");
        }
        Self {
            id,
            name: name.to_string(),
            description: description.to_string(),
        }
    }
}


pub struct NetworkTechnologyRepository {
    technologies: Vec<NetworkTechnology>,
    next_id: u32,
    existing_names: HashSet<String>, // For quick name uniqueness checks
}

impl NetworkTechnologyRepository {
    pub fn new() -> Self {
        Self {
            technologies: Vec::new(),
            next_id: 1,
            existing_names: HashSet::new(),
        }
    }

    // Accepts the DTO, adds an ID, and then pushes it into the repository.
    pub fn add_from_dto(&mut self, dto: NetworkTechnologyDTO) -> Result<(), String> {
        if self.existing_names.contains(&dto.name) {
            return Err("A technology with the same name already exists".to_string());
        }

        // Create a new NetworkTechnology with a generated ID
        let technology = NetworkTechnology::new(self.next_id, &dto.name, &dto.description);
        
        self.technologies.push(technology);
        self.existing_names.insert(dto.name);
        self.next_id += 1; // Increment ID for the next entry
        
        Ok(())
    }

    pub fn list_all(&self) -> &[NetworkTechnology] {
        &self.technologies
    }
}

pub struct NetworkTechnologyService {
    repository: NetworkTechnologyRepository,
}

impl NetworkTechnologyService {
    pub fn new(repository: NetworkTechnologyRepository) -> Self {
        Self { repository }
    }

    pub fn add_technology_from_dto(&mut self, dto: NetworkTechnologyDTO) -> Result<(), String> {
        self.repository.add_from_dto(dto)
    }

    pub fn list_technologies(&self) -> &[NetworkTechnology] {
        self.repository.list_all()
    }
}



fn main() {
    let repository = NetworkTechnologyRepository::new();
    let mut service = NetworkTechnologyService::new(repository);

    // Example JSON data
    let json_data = r#"
    [
        { "name": "2G", "description": "Second Generation Network" },
        { "name": "3G", "description": "Third Generation Network" },
        { "name": "4G", "description": "Fourth Generation Network" },
        { "name": "5G", "description": "Fifth Generation Network" }
    ]
    "#;

    // Deserialize into a vector of DTOs
    let technologies: Vec<NetworkTechnologyDTO> = serde_json::from_str(json_data).unwrap();

    // Add technologies to the service
    for dto in technologies {
        if let Err(e) = service.add_technology_from_dto(dto) {
            eprintln!("Error adding technology: {}", e);
        }
    }

    // List all technologies
    println!("All Technologies:");
    for tech in service.list_technologies() {
        println!("ID: {}, Name: {}, Description: {}", tech.id, tech.name, tech.description);
    }
}
