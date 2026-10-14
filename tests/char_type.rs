use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{ast::{Statement, Expr}, parser::parse_statement, value::Value}, execution::runtime::{execute_select_statement, format_header}, storage::row::{ColumnType}};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn char_column_basic() {
    let filename = "test_char_basic.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "items".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "code".into(), col_type: ColumnType::Char(3), not_null: false, default_value: None, auto_increment: false, primary_key: false},
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false, constraints: Vec::new(),
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "items".into(), columns: None, rows: vec![vec![Expr::Literal(Value::Int(1)), Expr::Literal(Value::Text("A".into()))]] }).unwrap();
    let stmt = parse_statement("SELECT code FROM items").unwrap();
    let mut out = Vec::new();
    let meta = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
    assert_eq!(format_header(&meta), "code CHAR(3)");
    assert_eq!(out[0], vec!["A  "]);
}

#[test]
fn char_column_validate_length() {
    let filename = "test_char_len.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "items".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "code".into(), col_type: ColumnType::Char(3), not_null: false, default_value: None, auto_increment: false, primary_key: false},
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false, constraints: Vec::new(),
    }).unwrap();
    let res = aerodb::execution::handle_statement(
        &mut catalog,
        Statement::Insert {
            table_name: "items".into(),
            columns: None,
            rows: vec![vec![Expr::Literal(Value::Int(2)), Expr::Literal(Value::Text("SASASDADSA".into()))]],
        },
    );
    assert!(matches!(res, Err(aerodb::error::DbError::StringTooLong(_))), "{res:?}");
}
