use std::process::{Command, exit};
use chrono::Local; // For getting the current date

fn main() {
    let git_email = "mamezni@gmail.com";
    let git_name = "Mezni";

    // Set the global user email
    let email_result = Command::new("git")
        .arg("config")
        .arg("--global")
        .arg("user.email")
        .arg(git_email)
        .output();

    if let Err(error) = email_result {
        eprintln!("Failed to set Git user email: {}", error);
        exit(1);
    } else {
        println!("Git user email set to: {}", git_email);
    }

    // Set the global user name
    let name_result = Command::new("git")
        .arg("config")
        .arg("--global")
        .arg("user.name")
        .arg(git_name)
        .output();

    if let Err(error) = name_result {
        eprintln!("Failed to set Git user name: {}", error);
        exit(1);
    } else {
        println!("Git user name set to: {}", git_name);
    }

    // Stage changes using `git add -A`
    let add_result = Command::new("git")
        .arg("add")
        .arg("-A")
        .output();

    if let Err(error) = add_result {
        eprintln!("Failed to stage changes with `git add -A`: {}", error);
        exit(1);
    } else {
        println!("Staged all changes with `git add -A`.");
    }

    // Generate commit message with the current date
    let current_date = Local::now().format("%Y/%m/%d").to_string();
    let commit_message = format!("commit {}", current_date);

    // Create commit with `git commit -m`
    let commit_result = Command::new("git")
        .arg("commit")
        .arg("-m")
        .arg(&commit_message)
        .output();

    match commit_result {
        Ok(output) => {
            if output.status.success() {
                println!("Created a commit with message: \"{}\"", commit_message);
            } else {
                eprintln!(
                    "Git commit failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                exit(1);
            }
        }
        Err(error) => {
            eprintln!("Failed to create commit: {}", error);
            exit(1);
        }
    }
}
