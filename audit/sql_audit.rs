//! Audit adapter: execute the public Engine path, never discard SQL errors.
use aerodb::{engine::Engine, sql::parser::parse_statement};
use std::io::{self, BufRead};
fn main() {
    let dir = tempfile::tempdir().expect("temporary database directory");
    let mut engine = Engine::new(dir.path().join("audit.db").to_str().unwrap());
    for (i, line) in io::stdin().lock().lines().enumerate() {
        let sql = line.expect("read SQL");
        println!("@@BEGIN {i}");
        match parse_statement(&sql) {
            Err(e) => println!("@@STATUS PARSE_ERROR {e}"),
            Ok(stmt) => match engine.execute(stmt) {
                Ok(()) => println!("@@STATUS OK"),
                Err(e) => println!("@@STATUS EXEC_ERROR {e:?} :: {e}"),
            },
        }
        println!("@@END {i}");
    }
}
