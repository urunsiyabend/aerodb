//! ROLLBACK behavior for DML and DDL under the transactional model.
//!
//! The engine runs one active transaction per process, so these use a single
//! `Engine` handle: BEGIN, mutate, then ROLLBACK or COMMIT, and assert the
//! resulting state. Covers row inserts/updates/deletes and transactional DDL
//! (CREATE TABLE, CREATE INDEX, DROP TABLE).
//!
//! DML rollback is now *logical*: aborted row versions are left in place and
//! hidden by MVCC visibility (see `mvcc_logical_abort.rs`), not physically
//! reverted. The observable outcome asserted here is unchanged. DDL rollback
//! still reverts the non-versioned schema catalog pages plus the in-memory
//! index snapshot.

use aerodb::{
    engine::Engine, execution::runtime::execute_select_with_indexes, sql::parser::parse_statement,
    storage::row::ColumnValue, storage::row::Row,
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

fn select_all(engine: &mut Engine, table: &str) -> Vec<Row> {
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut engine.catalog, table, None, &mut rows).unwrap();
    rows
}

#[test]
fn insert_rollback_discards_row() {
    let mut engine = setup_engine("mvcc_rollback_insert.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'keep')");

    exec(&mut engine, "BEGIN");
    exec(&mut engine, "INSERT INTO t VALUES (2, 'gone')");
    exec(&mut engine, "ROLLBACK");

    let rows = select_all(&mut engine, "t");
    assert_eq!(rows.len(), 1, "rolled-back insert must not survive");
    assert_eq!(rows[0].key, 1);
}

#[test]
fn update_rollback_restores_previous_value() {
    let mut engine = setup_engine("mvcc_rollback_update.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'original')");

    exec(&mut engine, "BEGIN");
    exec(&mut engine, "UPDATE t SET v = 'changed' WHERE id = 1");
    exec(&mut engine, "ROLLBACK");

    let rows = select_all(&mut engine, "t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data.0[1], ColumnValue::Text("original".to_string()));
}

#[test]
fn delete_rollback_restores_row() {
    let mut engine = setup_engine("mvcc_rollback_delete.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'alive')");

    exec(&mut engine, "BEGIN");
    exec(&mut engine, "DELETE FROM t WHERE id = 1");
    exec(&mut engine, "ROLLBACK");

    let rows = select_all(&mut engine, "t");
    assert_eq!(rows.len(), 1, "rolled-back delete must restore the row");
    assert_eq!(rows[0].data.0[1], ColumnValue::Text("alive".to_string()));
}

#[test]
fn create_table_rollback_removes_table() {
    let mut engine = setup_engine("mvcc_rollback_create_table.db");
    exec(&mut engine, "BEGIN");
    exec(&mut engine, "CREATE TABLE r (id INTEGER PRIMARY KEY)");
    exec(&mut engine, "ROLLBACK");

    assert!(
        engine.catalog.get_table("r").is_err(),
        "rolled-back CREATE TABLE must leave no table"
    );
}

#[test]
fn create_table_commit_persists_table() {
    let mut engine = setup_engine("mvcc_rollback_commit_table.db");
    exec(&mut engine, "BEGIN");
    exec(&mut engine, "CREATE TABLE c (id INTEGER PRIMARY KEY)");
    exec(&mut engine, "COMMIT");

    assert!(
        engine.catalog.get_table("c").is_ok(),
        "committed CREATE TABLE must persist"
    );
}

#[test]
fn create_index_rollback_removes_index() {
    let mut engine = setup_engine("mvcc_rollback_create_index.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    let before = engine.catalog.all_indexes().len();

    exec(&mut engine, "BEGIN");
    exec(&mut engine, "CREATE INDEX idx_t_v ON t (v)");
    exec(&mut engine, "ROLLBACK");

    assert_eq!(
        engine.catalog.all_indexes().len(),
        before,
        "rolled-back CREATE INDEX must not leave an in-memory index"
    );
    assert!(engine.catalog.find_index("t", "v").is_none());
}

#[test]
fn drop_table_rollback_restores_table() {
    let mut engine = setup_engine("mvcc_rollback_drop_table.db");
    exec(&mut engine, "CREATE TABLE d (id INTEGER PRIMARY KEY)");

    exec(&mut engine, "BEGIN");
    exec(&mut engine, "DROP TABLE d");
    exec(&mut engine, "ROLLBACK");

    assert!(
        engine.catalog.get_table("d").is_ok(),
        "rolled-back DROP TABLE must restore the table"
    );
}
