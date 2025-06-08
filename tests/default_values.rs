use aerodb::{catalog::Catalog, storage::pager::Pager, execution::runtime::{handle_statement, execute_select_with_indexes}, sql::{parser::parse_statement, ast::{Statement, Expr}}, storage::row::{ColumnType, ColumnValue}};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn parse_default_values() {
    let stmt = parse_statement("CREATE TABLE t (id INTEGER, name TEXT DEFAULT 'anon', age INTEGER DEFAULT 18 NOT NULL)").unwrap();
    if let Statement::CreateTable { columns, .. } = stmt {
        use aerodb::sql::ast::{Expr};
        assert!(matches!(columns[1].default_value, Some(Expr::Literal(ref s)) if s == "anon"));
        assert!(matches!(columns[2].default_value, Some(Expr::Literal(ref s)) if s == "18"));
        assert!(columns[2].not_null);
    } else { panic!("expected create table"); }
}

#[test]
fn insert_with_defaults() {
    let filename = "test_defaults.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, parse_statement("CREATE TABLE t (id INTEGER, name TEXT DEFAULT 'anon', age INTEGER DEFAULT 18)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (1, DEFAULT, 20)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO t (id) VALUES (2)").unwrap()).unwrap();
    let mut out = Vec::new();
    execute_select_with_indexes(&mut catalog, "t", Some(Expr::Equals { left: "id".into(), right: "2".into() }), &mut out).unwrap();
    assert_eq!(out[0].data.0[1], ColumnValue::Text("anon".into()));
    assert_eq!(out[0].data.0[2], ColumnValue::Integer(18));
}

#[test]
fn unqualified_insert_wrong_count() {
    let filename = "test_defaults_error.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, parse_statement("CREATE TABLE t (id INTEGER, name TEXT)").unwrap()).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (1)").unwrap());
    assert!(res.is_err());
}
