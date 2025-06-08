use aerodb::{catalog::Catalog, storage::pager::Pager, execution::runtime::{handle_statement, execute_select_with_indexes}, sql::{parser::parse_statement, ast::{Statement, Expr}}, storage::row::{ColumnType, ColumnValue}};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn parse_insert_null() {
    let stmt = parse_statement("INSERT INTO users VALUES (1, NULL)").unwrap();
    if let Statement::Insert { values, .. } = stmt {
        assert!(matches!(values[1], Expr::Literal(ref v) if v == "NULL"));
    } else { panic!("expected insert"); }
}

#[test]
fn insert_and_retrieve_null() {
    let filename = "test_null.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "nickname".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, NULL)").unwrap()).unwrap();
    let mut out = Vec::new();
    execute_select_with_indexes(
        &mut catalog,
        "users",
        Some(Expr::Equals { left: "id".into(), right: "1".into() }),
        &mut out,
    ).unwrap();
    assert_eq!(out[0].data.0[1], ColumnValue::Null);
}
