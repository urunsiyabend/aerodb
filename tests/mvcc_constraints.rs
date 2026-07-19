//! PRIMARY KEY uniqueness validated against MVCC visibility.
//!
//! Uniqueness must be checked against the *visible* set of rows, not every
//! physical version: a key freed by a committed DELETE, or by a rolled-back
//! INSERT, must be reusable, while a live duplicate must still be rejected.
//! (FOREIGN KEY / NOT NULL are exercised at the constraint-module level in
//! `constraints_module.rs`; they are not exposed through the SQL parser.)

use aerodb::{
    engine::Engine, execution::runtime::execute_select_with_indexes, sql::parser::parse_statement,
    storage::row::Row,
};
use std::fs;

fn setup_engine(filename: &str) -> Engine {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Engine::new(filename)
}

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_statement(sql).unwrap()).unwrap();
}

fn try_exec(engine: &mut Engine, sql: &str) -> bool {
    engine.execute(parse_statement(sql).unwrap()).is_ok()
}

fn select_all(engine: &mut Engine, table: &str) -> Vec<Row> {
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut engine.catalog, table, None, &mut rows).unwrap();
    rows
}

#[test]
fn primary_key_rejects_live_duplicate() {
    let mut engine = setup_engine("mvcc_pk_duplicate.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");

    assert!(
        !try_exec(&mut engine, "INSERT INTO t VALUES (1, 'b')"),
        "a live duplicate key must be rejected"
    );
    assert_eq!(select_all(&mut engine, "t").len(), 1);
}

#[test]
fn primary_key_reusable_after_committed_delete() {
    let mut engine = setup_engine("mvcc_pk_after_delete.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
    exec(&mut engine, "DELETE FROM t WHERE id = 1");

    assert!(
        try_exec(&mut engine, "INSERT INTO t VALUES (1, 'reborn')"),
        "a key freed by a committed delete must be reusable"
    );
    let rows = select_all(&mut engine, "t");
    assert_eq!(rows.len(), 1, "the deleted version must not count as a duplicate");
    assert_eq!(rows[0].key, 1);
}

#[test]
fn primary_key_reusable_after_rolled_back_insert() {
    let mut engine = setup_engine("mvcc_pk_after_rollback.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");

    exec(&mut engine, "BEGIN");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'aborted')");
    exec(&mut engine, "ROLLBACK");

    assert!(
        try_exec(&mut engine, "INSERT INTO t VALUES (1, 'committed')"),
        "a key from a rolled-back insert must be reusable"
    );
    assert_eq!(select_all(&mut engine, "t").len(), 1);
}
