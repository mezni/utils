
/*
pub mod myfs;

fn main2() {
    println!("MAIN");
    myfs::test_create_dir();
}
*/

use sled::{open, IVec};

fn main() -> sled::Result<()> {
    // Open the Sled database
    let db = open("my_database")?;

    // Open a tree for our data
    let tree = db.open_tree("my_tree")?;

    // Insert some data into the tree
    tree.insert("key1", b"cats")?;
    tree.insert("key2", IVec::from("value2"))?;

    // Retrieve a value from the tree
    let value = tree.get("key1")?;
    
    // Deserialize the byte array into a string
    if let Some(bytes) = value {
        let str_value = String::from_utf8_lossy(&bytes).into_owned();
        println!("Value for 'key1': {}", str_value);
    } else {
        println!("No value found for 'key1'");
    }

    // Retrieve and print the value for key2
    let value2 = tree.get("key2")?;
    if let Some(bytes) = value2 {
        let str_value2 = String::from_utf8_lossy(&bytes).into_owned();
        println!("Value for 'key2': {}", str_value2);
    } else {
        println!("No value found for 'key2'");
    }

    Ok(())
}