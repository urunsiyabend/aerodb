//! Regression tests for the durable transaction-id counter (page-0 meta).
//!
//! The WAL is truncated on every commit, so before this counter was persisted
//! the id sequence restarted at 1 each session and collided with `created_tx`
//! values already written to pages by earlier sessions. These tests assert the
//! counter now advances monotonically across reopen and that rows written by a
//! prior session stay visible afterward.

use aerodb::{
    engine::Engine, execution::runtime::execute_select_with_indexes, sql::parser::parse_statement,
};
use std::fs;

fn fresh(filename: &str) -> Engine {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Engine::new(filename)
}

fn reopen(filename: &str) -> Engine {
    Engine::new(filename)
}

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_statement(sql).unwrap()).unwrap();
}

/// The id the engine would assign to the transaction started by the next BEGIN.
fn begin_and_read_tx_id(engine: &mut Engine) -> u64 {
    exec(engine, "BEGIN");
    let id = engine
        .catalog
        .current_snapshot()
        .expect("snapshot inside transaction")
        .current_tx_id
        .expect("current transaction id");
    id
}

#[test]
fn transaction_ids_are_monotonic_across_reopen() {
    let filename = "mvcc_txid_monotonic.db";

    let mut first = fresh(filename);
    let id1 = begin_and_read_tx_id(&mut first);
    exec(&mut first, "COMMIT");
    drop(first);

    let mut second = reopen(filename);
    let id2 = begin_and_read_tx_id(&mut second);
    exec(&mut second, "COMMIT");
    drop(second);

    let mut third = reopen(filename);
    let id3 = begin_and_read_tx_id(&mut third);
    exec(&mut third, "COMMIT");

    assert!(
        id2 > id1 && id3 > id2,
        "ids must strictly increase across reopen: {id1} -> {id2} -> {id3}"
    );
}

#[test]
fn reopened_session_does_not_reuse_prior_transaction_id() {
    let filename = "mvcc_txid_no_reuse.db";

    // Session 1: write a row inside an explicit transaction, so the row's
    // created_tx equals the session-1 transaction id.
    let mut first = fresh(filename);
    exec(&mut first, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    let id1 = begin_and_read_tx_id(&mut first);
    exec(&mut first, "INSERT INTO t VALUES (1, 'from-session-1')");
    exec(&mut first, "COMMIT");
    drop(first);

    // Session 2: the next transaction id must be past session 1's, so a new
    // row cannot be stamped with a created_tx that collides with the old one.
    let mut second = reopen(filename);
    let id2 = begin_and_read_tx_id(&mut second);
    assert!(id2 > id1, "reused transaction id {id2} <= {id1}");
    exec(&mut second, "INSERT INTO t VALUES (2, 'from-session-2')");
    exec(&mut second, "COMMIT");
    drop(second);

    // Both rows remain visible after another reopen.
    let mut third = reopen(filename);
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut third.catalog, "t", None, &mut rows).unwrap();
    assert_eq!(rows.len(), 2, "rows from both sessions must be visible");
}
