mod storage;
mod sql;
mod execution;
mod transaction;

use std::io::{self, Write};

use crate::storage::pager::Pager;
use crate::storage::btree::BTree;
use crate::sql::parser::parse_statement;
use crate::sql::ast::Statement;

fn main() -> io::Result<()> {
    println!("Welcome to AeroDB v0.3 (B-Tree extended). Type .exit to quit.\n");

    // Open (or create) the file "aerodb.db"
    let pager = Pager::new("db.aerodb")?;
    // Create a B-Tree on top of that pager
    let mut btree = BTree::new(pager)?;

    loop {
        print!("aerodb> ");
        io::stdout().flush()?;

        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            // EOF
            break;
        }
        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case(".exit") || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }

        match parse_statement(trimmed) {
            Ok(stmt) => match stmt {
                Statement::Insert { key, payload } => {
                    match btree.insert(key, &payload) {
                        Ok(()) => println!("Inserted key={} payload=\"{}\"", key, payload),
                        Err(e) => println!("Error inserting: {}", e),
                    }
                }
                Statement::Select { key } => match btree.find(key) {
                    Ok(Some(row)) => println!("Found ▶ key={} payload=\"{}\"", row.key, row.payload),
                    Ok(None) => println!("Not found key={}", key),
                    Err(e) => println!("Error selecting: {}", e),
                },
                Statement::Exit => break,
            },
            Err(e) => println!("Parse error: {}", e),
        }
    }

    // (Optional) Flush any cached pages on exit
    btree.flush_all()?;
    println!("Goodbye!");
    Ok(())
}
