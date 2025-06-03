mod storage;
mod sql;
mod execution;
mod transaction;

use std::io::{self, Write};

use log::{debug, info, warn};
use crate::storage::pager::Pager;
use crate::storage::btree::BTree;
use crate::sql::parser::parse_statement;
use crate::sql::ast::Statement;

fn main() -> io::Result<()> {
    // Initialize env_logger (reads RUST_LOG, etc.)
    env_logger::init();

    info!("Starting AeroDB v0.3.1 (with structured logging). Type .exit to quit.");

    // Open (or create) the database file
    let pager = Pager::new("data.aerodb")?;
    let mut btree = BTree::new(pager)?;

    loop {
        print!("aerodb> ");
        io::stdout().flush()?;

        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            // EOF (Ctrl+D)
            break;
        }
        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case(".exit") || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }

        // Pass a &str to parse_statement()
        match parse_statement(trimmed) {
            Ok(stmt) => match stmt {
                Statement::Insert { key, payload } => {
                    debug!("Insert called with key={} payload=\"{}\"", key, payload);
                    match btree.insert(key, &payload) {
                        Ok(()) => {
                            info!("Inserted key={} payload=\"{}\"", key, payload);
                        }
                        Err(e) => warn!("Error inserting key={} : {}", key, e),
                    }
                }
                Statement::Select { key } => {
                    debug!("Select called with key={}", key);
                    match btree.find(key) {
                        Ok(Some(row)) => {
                            info!("Found ▶ key={} payload=\"{}\"", row.key, row.payload);
                        }
                        Ok(None) => {
                            info!("Not found: key={}", key);
                        }
                        Err(e) => warn!("Error selecting key={} : {}", key, e),
                    }
                }
                Statement::Exit => break,
            },
            Err(e) => warn!("Parse error: {}", e),
        }
    }

    // Flush everything on exit
    btree.flush_all()?;
    info!("Goodbye!");
    Ok(())
}
