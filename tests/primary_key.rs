use aerodb::{catalog::Catalog, storage::pager::Pager, sql::parser::parse_statement, execution::{handle_statement, execute_select_with_indexes, row_to_strings}};
use aerodb::sql::ast::Statement;
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn primary_key_happy_path() {
    let filename = "pk_basic.db";
    let mut catalog = setup_catalog(filename);
    let create = parse_statement("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
    if let Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists } = create {
        handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists }).unwrap();
    }
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1,'Alice')").unwrap()).unwrap();
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut catalog, "users", None, &mut rows).unwrap();
    let vals: Vec<Vec<String>> = rows.iter().map(|r| row_to_strings(r)).collect();
    assert_eq!(vals, vec![vec![String::from("1"), String::from("Alice")]]);
}

#[test]
fn primary_key_duplicate_error() {
    let filename = "pk_dup.db";
    let mut catalog = setup_catalog(filename);
    let create = parse_statement("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
    if let Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists } = create {
        handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists }).unwrap();
    }
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1,'Alice')").unwrap()).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1,'Bob')").unwrap());
    assert!(res.is_err());
}

#[test]
fn primary_key_null_error() {
    let filename = "pk_null.db";
    let mut catalog = setup_catalog(filename);
    let create = parse_statement("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
    if let Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists } = create {
        handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists }).unwrap();
    }
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (NULL,'Charlie')").unwrap());
    assert!(res.is_err());
}

#[test]
fn primary_key_composite() {
    let filename = "pk_comp.db";
    let mut catalog = setup_catalog(filename);
    let create = parse_statement("CREATE TABLE orders (order_id INT, item_id INT, PRIMARY KEY(order_id, item_id))").unwrap();
    if let Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists } = create {
        handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists }).unwrap();
    }
    handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (1,1)").unwrap()).unwrap();
    let dup = handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (1,1)").unwrap());
    assert!(dup.is_err());
    let null_err = handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (1,NULL)").unwrap());
    assert!(null_err.is_err());
}

#[test]
fn primary_key_persistence() {
    let filename = "pk_persist.db";
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    {
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();
        let create = parse_statement("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
        if let Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists } = create {
            handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists }).unwrap();
        }
        handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1,'Alice')").unwrap()).unwrap();
    }
    {
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();
        let res = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1,'Bob')").unwrap());
        assert!(res.is_err());
    }
}

