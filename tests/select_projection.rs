use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::runtime::{execute_select_statement, format_header}, storage::row::ColumnType};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn select_single_column() {
    let filename = "test_select_single.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false},
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false, constraints: Vec::new(),
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, 'bob')").unwrap()).unwrap();
    let stmt = parse_statement("SELECT name FROM users").unwrap();
    let mut out = Vec::new();
    let meta = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
    assert_eq!(format_header(&meta), "name TEXT");
    assert_eq!(out[0], vec!["bob"]);
}

#[test]
fn select_two_columns() {
    let filename = "test_select_two.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false},
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false, constraints: Vec::new(),
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, 'bob')").unwrap()).unwrap();
    let stmt = parse_statement("SELECT id, name FROM users").unwrap();
    let mut out = Vec::new();
    let meta = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
    assert_eq!(format_header(&meta), "id INTEGER | name TEXT");
    assert_eq!(out[0], vec!["1", "bob"]);
}
