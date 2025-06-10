use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::{Statement, Expr}}, execution::runtime::{handle_statement, execute_select_with_indexes}};
use aerodb::storage::row::ColumnValue;
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn not_null_violation_via_runtime() {
    let filename = "integration_nn.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, parse_statement("CREATE TABLE t (id INTEGER NOT NULL)").unwrap()).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (NULL)").unwrap());
    assert!(res.is_err());
}

#[test]
fn default_function_current_timestamp() {
    let filename = "integration_default.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, parse_statement("CREATE TABLE t (id INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO t (id) VALUES (1)").unwrap()).unwrap();
    let mut out = Vec::new();
    execute_select_with_indexes(&mut catalog, "t", Some(Expr::Equals { left: "id".into(), right: "1".into() }), &mut out).unwrap();
    if let ColumnValue::Text(_) = out[0].data.0[1] {
        // ok
    } else {
        panic!("default not applied");
    }
}

#[test]
fn foreign_key_cascade_delete_via_runtime() {
    let filename = "integration_fk.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, parse_statement("CREATE TABLE p (id INTEGER)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("CREATE TABLE c (id INTEGER, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id) ON DELETE CASCADE)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO p VALUES (1)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO c VALUES (1, 1)").unwrap()).unwrap();
    handle_statement(&mut catalog, Statement::Delete { table_name: "p".into(), selection: Some(Expr::Equals { left: "id".into(), right: "1".into() }) }).unwrap();
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut catalog, "c", None, &mut rows).unwrap();
    assert!(rows.is_empty());
}
