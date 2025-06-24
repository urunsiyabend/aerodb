use aerodb::{
    catalog::Catalog,
    storage::pager::Pager,
    sql::{parser::parse_statement, ast::Statement},
    execution::runtime::{handle_statement, execute_select_with_indexes},
    engine::Engine,
};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn multi_value_insert_happy_path() {
    let filename = "multi_insert_happy.db";
    let mut catalog = setup_catalog(filename);
    let create = parse_statement("CREATE TABLE nums (n INTEGER PRIMARY KEY)").unwrap();
    if let Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists } = create {
        handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists }).unwrap();
    }
    handle_statement(&mut catalog, parse_statement("INSERT INTO nums VALUES (1), (2), (3)").unwrap()).unwrap();
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut catalog, "nums", None, &mut rows).unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn multi_value_insert_rollback_on_error() {
    let filename = "multi_insert_rollback.db";
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    let mut engine = Engine::new(filename);
    engine.execute(parse_statement("CREATE TABLE nums (n INTEGER PRIMARY KEY)").unwrap()).unwrap();
    engine.execute(parse_statement("INSERT INTO nums VALUES (1), (2), (3)").unwrap()).unwrap();
    let res = engine.execute(parse_statement("INSERT INTO nums VALUES (4), (4), (5)").unwrap());
    assert!(res.is_err());
    drop(engine);
    let mut engine = Engine::new(filename);
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut engine.catalog, "nums", None, &mut rows).unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn multi_value_insert_column_list() {
    let filename = "multi_insert_cols.db";
    let mut catalog = setup_catalog(filename);
    let create = parse_statement("CREATE TABLE nums (n INTEGER PRIMARY KEY)").unwrap();
    if let Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists } = create {
        handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists }).unwrap();
    }
    handle_statement(&mut catalog, parse_statement("INSERT INTO nums (n) VALUES (6), (7)").unwrap()).unwrap();
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut catalog, "nums", None, &mut rows).unwrap();
    assert_eq!(rows.len(), 2);
}
