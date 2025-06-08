use aerodb::sql::parser::parse_statement;
use aerodb::sql::ast::{Statement};

#[test]
fn parse_auto_increment_column() {
    let stmt = parse_statement("CREATE TABLE users (id INT NOT NULL AUTO_INCREMENT, name TEXT)").unwrap();
    if let Statement::CreateTable { columns, .. } = stmt {
        assert!(columns[0].auto_increment);
        assert_eq!(columns[0].name, "id");
    } else { panic!("expected create table"); }
}

#[test]
fn auto_increment_non_integer_error() {
    let res = parse_statement("CREATE TABLE t (id TEXT AUTO_INCREMENT)");
    assert!(res.is_err());
}

use aerodb::{catalog::Catalog, storage::pager::Pager, execution::{handle_statement, execute_select_with_indexes, row_to_strings}};
use std::fs;

#[test]
fn auto_increment_basic_insert() {
    let filename = "test_auto_inc_basic.db";
    let _ = fs::remove_file(filename);
    let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

    let stmt = parse_statement("CREATE TABLE users (id INT NOT NULL AUTO_INCREMENT, name TEXT)").unwrap();
    if let Statement::CreateTable { table_name, columns, fks, if_not_exists } = stmt {
        handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, if_not_exists }).unwrap();
    }

    handle_statement(&mut catalog, parse_statement("INSERT INTO users (name) VALUES ('Alice')").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO users (name) VALUES ('Bob')").unwrap()).unwrap();

    let mut rows = Vec::new();
    execute_select_with_indexes(&mut catalog, "users", None, &mut rows).unwrap();
    let strings: Vec<Vec<String>> = rows.iter().map(|r| row_to_strings(r)).collect();
    assert_eq!(strings, vec![vec![String::from("1"), String::from("Alice")], vec![String::from("2"), String::from("Bob")]]);
}

#[test]
fn auto_increment_explicit_values() {
    let filename = "test_auto_inc_explicit.db";
    let _ = fs::remove_file(filename);
    let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

    let stmt = parse_statement("CREATE TABLE users (id INT NOT NULL AUTO_INCREMENT, name TEXT)").unwrap();
    if let Statement::CreateTable { table_name, columns, fks, if_not_exists } = stmt {
        handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, if_not_exists }).unwrap();
    }

    handle_statement(&mut catalog, parse_statement("INSERT INTO users (id, name) VALUES (10, 'Charlie')").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO users (name) VALUES ('David')").unwrap()).unwrap();

    let mut rows = Vec::new();
    execute_select_with_indexes(&mut catalog, "users", None, &mut rows).unwrap();
    let strings: Vec<Vec<String>> = rows.iter().map(|r| row_to_strings(r)).collect();
    assert_eq!(strings, vec![vec![String::from("10"), String::from("Charlie")], vec![String::from("11"), String::from("David")]]);
}
