use aerodb::{catalog::Catalog, storage::pager::Pager, execution::runtime::{handle_statement}, sql::parser::parse_statement, sql::ast::Statement, storage::row::ColumnValue};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn parse_create_not_null() {
    let stmt = parse_statement("CREATE TABLE persons (id INTEGER NOT NULL, last_name TEXT NOT NULL, first_name TEXT NOT NULL, age INTEGER)").unwrap();
    if let Statement::CreateTable { columns, .. } = stmt {
        assert_eq!(columns.len(), 4);
        assert!(columns[0].not_null);
        assert!(columns[1].not_null);
        assert!(columns[2].not_null);
        assert!(!columns[3].not_null);
    } else { panic!("expected create table"); }
}

#[test]
fn enforce_not_null() {
    let filename = "test_not_null.db";
    let mut catalog = setup_catalog(filename);
    let stmt = parse_statement("CREATE TABLE persons (id INTEGER NOT NULL, last_name TEXT NOT NULL, first_name TEXT NOT NULL, age INTEGER)").unwrap();
    if let Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists } = stmt {
        handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists }).unwrap();
    } else { panic!("expected create table"); }

    // valid insert with NULL age
    handle_statement(&mut catalog, parse_statement("INSERT INTO persons VALUES (1, 'siyo', 'siyo', NULL)").unwrap()).unwrap();
    // invalid insert with NULL last_name
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO persons VALUES (2, NULL, 'siyo', 12)").unwrap());
    assert!(res.is_err());
    // invalid insert with NULL first_name
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO persons VALUES (3, 'siyo', NULL, 12)").unwrap());
    assert!(res.is_err());

    // verify stored row
    let mut out = Vec::new();
    aerodb::execution::runtime::execute_select_with_indexes(
        &mut catalog,
        "persons",
        Some(aerodb::sql::ast::Expr::Equals { left: "id".into(), right: "1".into() }),
        &mut out,
    ).unwrap();
    assert!(matches!(out[0].data.0[3], ColumnValue::Null));
}
