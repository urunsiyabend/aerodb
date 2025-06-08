use aerodb::{catalog::Catalog, storage::pager::Pager, execution::runtime::{handle_statement}, sql::parser::parse_statement, sql::ast::Statement};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn parse_primary_key_column() {
    let stmt = parse_statement("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
    if let Statement::CreateTable { columns, .. } = stmt {
        assert!(columns[0].primary_key);
        assert!(columns[0].not_null);
    } else { panic!("expected create table"); }
}

#[test]
fn primary_key_enforces_constraints() {
    let filename = "test_pk.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, parse_statement("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();
    let dup = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, 'Bob')").unwrap());
    assert!(dup.is_err());
    let null_pk = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (NULL, 'Carol')").unwrap());
    assert!(null_pk.is_err());
}
