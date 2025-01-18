pub struct NetworkTechnology {
    id: u8,
    name: String,
    description: String,
}

fn main() {
    let technologies = vec![
        NetworkTechnology {
            name: "2G".to_string(),
            description: "2G".to_string(),
        },
        NetworkTechnology {
            name: "3G".to_string(),
            description: "3G".to_string(),
        },
        NetworkTechnology {
            name: "3G".to_string(),
            description: "3G".to_string(),
        },
        NetworkTechnology {
            name: "3G".to_string(),
            description: "3G".to_string(),
        },
    ];
    for technology in technologies {
        println!("{:?}", user);
    }
}
