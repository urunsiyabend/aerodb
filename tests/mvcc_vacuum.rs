//! Vacuum (garbage collection of dead MVCC versions).
//!
//! Vacuum physically removes row versions whose deleting transaction committed
//! below the global xmin (no active snapshot can still observe them). These
//! tests confirm dead versions are pruned, live versions are retained, and the
//! visible result set is unchanged by a vacuum pass.

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
fn vacuum_prunes_committed_deleted_version() {
    let mut engine = setup_engine("mvcc_vacuum_delete.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
    exec(&mut engine, "INSERT INTO t VALUES (2, 'b')");
    exec(&mut engine, "INSERT INTO t VALUES (3, 'c')");

    exec(&mut engine, "BEGIN");
    exec(&mut engine, "DELETE FROM t WHERE id = 2");
    exec(&mut engine, "COMMIT");

    let report = engine.vacuum_table("t").unwrap();
    assert!(
        report.versions_removed >= 1,
        "the committed tombstone should be reclaimed"
    );

    let rows = select_all(&mut engine, "t");
    let keys: Vec<i32> = rows.iter().map(|r| r.key).collect();
    assert_eq!(keys, vec![1, 3], "only live rows survive the vacuum");
}

#[test]
fn vacuum_prunes_superseded_update_version() {
    let mut engine = setup_engine("mvcc_vacuum_update.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'old')");

    exec(&mut engine, "BEGIN");
    exec(&mut engine, "UPDATE t SET v = 'new' WHERE id = 1");
    exec(&mut engine, "COMMIT");

    let report = engine.vacuum_table("t").unwrap();
    assert!(
        report.versions_removed >= 1,
        "the superseded version should be reclaimed"
    );

    let rows = select_all(&mut engine, "t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data.0[1], ColumnValue::Text("new".to_string()));
}

#[test]
fn vacuum_retains_live_versions_and_is_noop_when_clean() {
    let mut engine = setup_engine("mvcc_vacuum_noop.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
    exec(&mut engine, "INSERT INTO t VALUES (2, 'b')");

    // No deletes/updates -> nothing to reclaim.
    let report = engine.vacuum_table("t").unwrap();
    assert_eq!(report.versions_removed, 0, "clean table has no dead versions");

    let rows = select_all(&mut engine, "t");
    assert_eq!(rows.len(), 2, "live rows must be untouched by vacuum");
}
