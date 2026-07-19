//! Logical abort: rolled-back work is hidden by MVCC visibility rather than
//! physically reverted, then reclaimed by vacuum.
//!
//! These tests distinguish the new mechanism from the old physical page revert:
//! an aborted version stays physically present (so vacuum can find and reclaim
//! it) while never being visible to any snapshot, including after a reopen that
//! may have flushed the aborted version to disk.

use aerodb::{
    engine::Engine, execution::runtime::execute_select_with_indexes, sql::parser::parse_statement,
    storage::row::Row,
};
use std::fs;

fn fresh(filename: &str) -> Engine {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    let _ = fs::remove_file(format!("{}.clog", filename));
    Engine::new(filename)
}

fn reopen(filename: &str) -> Engine {
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
fn aborted_insert_is_invisible_then_reclaimed_by_vacuum() {
    let mut engine = fresh("mvcc_logical_abort_vacuum.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");

    exec(&mut engine, "BEGIN");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'aborted')");
    exec(&mut engine, "ROLLBACK");

    // Invisible immediately after abort.
    assert!(
        select_all(&mut engine, "t").is_empty(),
        "aborted insert must not be visible"
    );

    // Logical abort left the version physically in place: vacuum finds and
    // reclaims it (a physical revert would have left nothing to reclaim).
    let report = engine.vacuum_table("t").unwrap();
    assert!(
        report.versions_removed >= 1,
        "vacuum must reclaim the aborted-creator version"
    );

    assert!(
        select_all(&mut engine, "t").is_empty(),
        "table stays empty after vacuum"
    );
}

#[test]
fn aborted_work_stays_invisible_across_reopen() {
    let filename = "mvcc_logical_abort_reopen.db";

    {
        let mut engine = fresh(filename);
        exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        exec(&mut engine, "INSERT INTO t VALUES (1, 'live')");

        exec(&mut engine, "BEGIN");
        exec(&mut engine, "INSERT INTO t VALUES (2, 'aborted')");
        exec(&mut engine, "ROLLBACK");

        // A later committed write shares the leaf, flushing the aborted version
        // to disk. It must remain invisible regardless.
        exec(&mut engine, "INSERT INTO t VALUES (3, 'after')");
    }

    let mut engine = reopen(filename);
    let keys: Vec<i32> = select_all(&mut engine, "t").iter().map(|r| r.key).collect();
    assert_eq!(
        keys,
        vec![1, 3],
        "aborted row must never be visible, even after reopen"
    );
}

#[test]
fn primary_key_reusable_after_logical_abort() {
    let mut engine = fresh("mvcc_logical_abort_pk.db");
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");

    exec(&mut engine, "BEGIN");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'aborted')");
    exec(&mut engine, "ROLLBACK");

    // The aborted version is still physically present, but the key must be
    // reusable because that version is invisible to the uniqueness check.
    exec(&mut engine, "INSERT INTO t VALUES (1, 'committed')");

    let rows = select_all(&mut engine, "t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, 1);
    assert_eq!(
        rows[0].data.0[1],
        aerodb::storage::row::ColumnValue::Text("committed".to_string())
    );
}
