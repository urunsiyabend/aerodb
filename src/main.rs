// src/main.rs
//
// The REPL binary links the `aerodb` library instead of re-compiling its
// modules, so the library is built (and unit-tested) once.

use std::io::{self, Write};
use log::{info, warn};

use aerodb::catalog::Catalog;
use aerodb::error::DbError;
use aerodb::execution::handle_statement;
use aerodb::sql::ast::Statement;
use aerodb::sql::parser::parse_statement;
use aerodb::storage::pager::Pager;

// const DATABASE_FILE: &str = "data.aerodb";
const DATABASE_FILE: &str = "data.aerodb";


fn main() -> io::Result<()> {
    env_logger::init();
    info!("AeroDB v0.5(Transaction with WAL). Type .exit to quit.");

    let mut catalog = Catalog::open(Pager::new(DATABASE_FILE)?)?;
    let mut transaction_manager = aerodb::transaction::TransactionManager::new();

    loop {
        print!("aerodb> ");
        io::stdout().flush()?;

        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            break; // EOF
        }
        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case(".exit") || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }

        match parse_statement(trimmed) {
            Ok(stmt) => {
                if let Statement::Exit = stmt {
                    break;
                }
                if let Err(e) =
                    transaction_manager.execute(&mut catalog, stmt, handle_statement)
                {
                    match e {
                        DbError::TableNotFound(t) => println!("Error: table '{}' not found", t),
                        DbError::DuplicateKey(k) => println!("Error: duplicate primary key {}", k),
                        DbError::Overflow => println!("Error: value out of range"),
                        DbError::ParseError(m) | DbError::InvalidValue(m) => println!("Error: {}", m),
                        DbError::ColumnNotFound(c) => println!("Error: column '{}' not found", c),
                        DbError::GroupByMismatch(c) => println!("Error: column '{}' must appear in GROUP BY or be aggregated", c),
                        DbError::NotFound(m) => println!("Error: {}", m),
                        DbError::NullViolation(c) => println!("Error: column '{}' cannot be NULL", c),
                        DbError::ForeignKeyViolation(m) => println!("Error: {}", m),
                        DbError::WriteConflict(k) => println!("Error: write conflict on logical key {}", k),
                        DbError::Io(err) => println!("IO error: {}", err),
                        other => println!("Error: {}", other),
                    }
                }
            }
            Err(e) => warn!("Parse error: {}", e),
        }
    }

    info!("Goodbye!");
    Ok(())
}
