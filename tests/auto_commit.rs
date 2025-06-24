use aerodb::{engine::Engine, sql::parser::parse_statement, execution::runtime::execute_select_with_indexes};
use std::fs;

fn setup_engine(filename: &str) -> Engine {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Engine::new(filename)
}

fn open_engine(filename: &str) -> Engine {
    Engine::new(filename)
}

#[test]
fn insert_auto_commit_ok() {
    let filename = "auto_commit_ok.db";
    let mut engine = setup_engine(filename);
    engine.execute(parse_statement("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap()).unwrap();
    engine.execute(parse_statement("INSERT INTO t VALUES (1)").unwrap()).unwrap();
    drop(engine);

    let mut engine2 = open_engine(filename);
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut engine2.catalog, "t", None, &mut rows).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn insert_auto_commit_err() {
    let filename = "auto_commit_err.db";
    let mut engine = setup_engine(filename);
    engine.execute(parse_statement("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap()).unwrap();
    engine.execute(parse_statement("INSERT INTO t VALUES (1)").unwrap()).unwrap();
    let res = engine.execute(parse_statement("INSERT INTO t VALUES (1)").unwrap());
    assert!(res.is_err());
    drop(engine);

    let mut engine2 = open_engine(filename);
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut engine2.catalog, "t", None, &mut rows).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn begin_explicit_preserves_manual_flow() {
    let filename = "explicit_tx.db";
    let mut engine = setup_engine(filename);
    engine.execute(parse_statement("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap()).unwrap();
    engine.execute(parse_statement("BEGIN").unwrap()).unwrap();
    engine.execute(parse_statement("INSERT INTO t VALUES (1)").unwrap()).unwrap();

    // changes should not be visible before commit
    let mut other = open_engine(filename);
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut other.catalog, "t", None, &mut rows).unwrap();
    assert!(rows.is_empty());

    engine.execute(parse_statement("COMMIT").unwrap()).unwrap();
    drop(other);
    let mut check = open_engine(filename);
    let mut rows2 = Vec::new();
    execute_select_with_indexes(&mut check.catalog, "t", None, &mut rows2).unwrap();
    assert_eq!(rows2.len(), 1);
}
