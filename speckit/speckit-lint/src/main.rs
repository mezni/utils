use std::fs;

use clap::Parser;
use walkdir::WalkDir;

mod rules;
use rules::Rule;

#[derive(Parser)]
#[command(name = "speckit-lint", about = "CI validator for BorneMap architecture rules")]
struct Args {
    #[arg(short, long, default_value = ".")]
    path: String,

    #[arg(short, long)]
    verbose: bool,
}

fn main() {
    let args = Args::parse();
    let all_rules: Vec<Box<dyn Rule>> = vec![
        Box::new(rules::service_topology::ServiceTopologyRule),
        Box::new(rules::schema_isolation::SchemaIsolationRule),
        Box::new(rules::naming::NamingRule),
        Box::new(rules::openapi_first::OpenapiFirstRule),
        Box::new(rules::sqlx_safety::SqlxSafetyRule),
        Box::new(rules::frontend_boundary::FrontendBoundaryRule),
        Box::new(rules::migration_validation::MigrationValidationRule),
    ];

    let mut total_violations = 0;
    let mut total_files = 0;

    for entry in WalkDir::new(&args.path)
        .into_iter()
        .filter_entry(|e| {
            !e.file_name()
                .to_str()
                .map(|s| {
                    s == "target"
                        || s == "node_modules"
                        || s == ".git"
                        || s == ".specify"
                })
                .unwrap_or(false)
        })
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let rel_path = path
            .strip_prefix(&args.path)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();

        let Ok(content) = fs::read_to_string(path) else {
            continue;
        };
        total_files += 1;

        for rule in &all_rules {
            let violations = rule.check(&rel_path, &content);
            for violation in violations {
                total_violations += 1;
                if args.verbose {
                    println!("[{}] {}: {violation}", rule.name(), rel_path);
                }
            }
        }
    }

    println!(
        "Checked {total_files} files. Found {total_violations} violations."
    );

    if total_violations > 0 {
        std::process::exit(1);
    }
}
