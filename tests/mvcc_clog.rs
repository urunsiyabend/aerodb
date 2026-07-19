//! Durable commit log (clog) persistence across reopen.
//!
//! The WAL is truncated on every clean commit, so transaction status cannot live
//! there durably. The clog sidecar (`<db>.clog`) retains committed/aborted status
//! across reopen. This is observable through vacuum: reclaiming a tombstone
//! requires the deleting transaction to be recorded as committed, which — after a
//! reopen that truncated the WAL — is only true because the clog persisted it.

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
fn committed_delete_is_vacuumable_after_reopen() {
    let filename = "mvcc_clog_vacuum_reopen.db";

    {
        let mut engine = fresh(filename);
        exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
        exec(&mut engine, "INSERT INTO t VALUES (2, 'b')");
        exec(&mut engine, "INSERT INTO t VALUES (3, 'c')");
        // Committed delete; its tombstone must stay reclaimable after reopen.
        exec(&mut engine, "DELETE FROM t WHERE id = 2");
    }

    // Reopen: the WAL was truncated by the committed statements, so the only
    // durable witness that the delete's transaction committed is the clog.
    let mut engine = reopen(filename);
    let report = engine.vacuum_table("t").unwrap();
    assert!(
        report.versions_removed >= 1,
        "committed tombstone must remain reclaimable across reopen (clog persists Committed)"
    );

    let keys: Vec<i32> = select_all(&mut engine, "t").iter().map(|r| r.key).collect();
    assert_eq!(keys, vec![1, 3], "only live rows survive");
}

#[test]
fn committed_rows_visible_after_reopen() {
    let filename = "mvcc_clog_visible_reopen.db";

    {
        let mut engine = fresh(filename);
        exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        exec(&mut engine, "BEGIN");
        exec(&mut engine, "INSERT INTO t VALUES (1, 'x')");
        exec(&mut engine, "INSERT INTO t VALUES (2, 'y')");
        exec(&mut engine, "COMMIT");
    }

    let mut engine = reopen(filename);
    assert_eq!(
        select_all(&mut engine, "t").len(),
        2,
        "committed rows must remain visible after reopen"
    );
}
