use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::runtime::{handle_statement, execute_select_with_indexes}, storage::row::ColumnValue};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn parse_default_functions() {
    let stmt = parse_statement("CREATE TABLE logs (id INTEGER, created DATETIME DEFAULT CURRENT_TIMESTAMP, updated DATETIME DEFAULT GETUTCDATE())").unwrap();
    use aerodb::sql::ast::Expr;
    if let Statement::CreateTable { columns, .. } = stmt {
        assert!(matches!(columns[1].default_value, Some(Expr::FunctionCall { ref name, .. }) if name.eq_ignore_ascii_case("CURRENT_TIMESTAMP")));
        assert!(matches!(columns[2].default_value, Some(Expr::FunctionCall { ref name, .. }) if name.eq_ignore_ascii_case("GETUTCDATE")));
    } else { panic!("expected create table"); }
}

#[test]
fn insert_with_function_defaults() {
    let filename = "test_function_defaults.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, parse_statement("CREATE TABLE logs (id INTEGER, created DATETIME DEFAULT CURRENT_TIMESTAMP, utc_time DATETIME DEFAULT GETUTCDATE())").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO logs (id) VALUES (1)").unwrap()).unwrap();
    let mut out = Vec::new();
    execute_select_with_indexes(&mut catalog, "logs", Some(aerodb::sql::ast::Expr::Equals { left: "id".into(), right: "1".into() }), &mut out).unwrap();
    if let ColumnValue::DateTime(created) = out[0].data.0[1] {
        let now = chrono::Local::now().timestamp();
        assert!((created - now).abs() <= 5);
    } else { panic!("expected datetime"); }
    if let ColumnValue::DateTime(utc) = out[0].data.0[2] {
        let now = chrono::Utc::now().timestamp();
        assert!((utc - now).abs() <= 5);
    } else { panic!("expected datetime"); }
}

