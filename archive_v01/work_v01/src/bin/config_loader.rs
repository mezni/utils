use sled::{Db, IVec};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkTechnology {
    id: u8,
    name: String,
    description: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open the Sled database
    let db: Db = sled::open("my_db")?;

    // An array of technology data to insert
    let technologies_data = [
        ("2G", "Second generation mobile telecommunications"),
        ("3G", "Third generation mobile telecommunications"),
        ("4G", "Fourth generation mobile telecommunications"),
        ("5G", "Fifth generation mobile telecommunications"),
    ];

    // Define the technology prefix
    let technology_prefix = "technology_";

    // Insert technologies into the database
    insert_technologies(&db, &technologies_data, technology_prefix)?;

    // Retrieve and print all stored technologies
    let stored_technologies = get_all_technologies(&db, technology_prefix)?;
    for tech in stored_technologies {
        println!("{:?}", tech);
    }

    Ok(())
}

/// Inserts a list of technologies into the Sled database.
fn insert_technologies(db: &Db, technologies_data: &[(&str, &str)], prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
    for (index, &(name, description)) in technologies_data.iter().enumerate() {
        let technology = NetworkTechnology {
            id: (index + 1) as u8, // IDs start from 1
            name: name.to_string(),
            description: description.to_string(),
        };

        insert_technology(db, technology, prefix)?;
    }
    db.flush()?;
    Ok(())
}

/// Inserts a single technology into the Sled database.
fn insert_technology(db: &Db, technology: NetworkTechnology, prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
    let key = format!("{}{}", prefix, technology.id); // Create a key with prefix
    let serialized_tech = bincode::serialize(&technology)?; // Serialize the technology

    // Insert into the database with the prefixed key
    db.insert(key, serialized_tech)?;
    Ok(())
}

/// Returns a vector of all stored technologies from the Sled database.
fn get_all_technologies(db: &Db, prefix: &str) -> Result<Vec<NetworkTechnology>, Box<dyn std::error::Error>> {
    let mut technologies = Vec::new();

    for technology in db.iter() {
        let (key, value) = technology?;
        let tech_key = String::from_utf8(key.to_vec())?;
        if tech_key.starts_with(prefix) { // Filter keys that start with the prefix
            let tech: NetworkTechnology = bincode::deserialize(&value)?; // Deserialize the value
            technologies.push(tech); // Collect the technology
        }
    }

    Ok(technologies) // Return the vector of technologies
}