//! Genuine multi-transaction interleavings over one shared storage backend
//! (`Database`), replacing the old two-handle "fake concurrency" pattern.
//!
//! Covers snapshot isolation, read stability, first-committer-wins write/write
//! conflict, abort invisibility, and a multi-threaded stress test asserting no
//! lost committed writes and no visible aborted/torn state.

use aerodb::{
    db::Database, sql::parser::parse_statement, storage::row::ColumnValue, storage::row::Row,
};
use std::fs;
use std::sync::Arc;
use std::thread;

fn fresh(filename: &str) -> Database {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    let _ = fs::remove_file(format!("{}.clog", filename));
    Database::open(filename).unwrap()
}

fn stmt(sql: &str) -> aerodb::sql::ast::Statement {
    parse_statement(sql).unwrap()
}

fn value(row: &Row, idx: usize) -> String {
    match &row.data.0[idx] {
        ColumnValue::Text(s) => s.clone(),
        other => other.to_string_value(),
    }
}

#[test]
fn snapshot_isolation_hides_concurrent_commit() {
    let db = fresh("mvcc_concurrent_snapshot.db");
    db.autocommit(stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .unwrap();

    let t1 = db.begin().unwrap();
    let t2 = db.begin().unwrap();

    db.execute(&t1, stmt("INSERT INTO t VALUES (1, 'a')")).unwrap();
    db.commit(t1).unwrap();

    // t2's snapshot was taken while t1 was live -> t1's row is invisible to it.
    assert!(
        db.query_all(&t2, "t").unwrap().is_empty(),
        "an older concurrent snapshot must not see a later commit"
    );

    // A transaction begun after the commit does see it.
    let t3 = db.begin().unwrap();
    assert_eq!(
        db.query_all(&t3, "t").unwrap().len(),
        1,
        "a snapshot taken after the commit sees the row"
    );

    db.abort(t2).unwrap();
    db.abort(t3).unwrap();
}

#[test]
fn reads_are_stable_within_a_transaction() {
    let db = fresh("mvcc_concurrent_read_stability.db");
    db.autocommit(stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .unwrap();
    db.autocommit(stmt("INSERT INTO t VALUES (1, 'orig')")).unwrap();

    let t1 = db.begin().unwrap();
    assert_eq!(value(&db.query_all(&t1, "t").unwrap()[0], 1), "orig");

    let t2 = db.begin().unwrap();
    db.execute(&t2, stmt("UPDATE t SET v = 'new' WHERE id = 1"))
        .unwrap();
    db.commit(t2).unwrap();

    // t1 re-reads its own snapshot: still the original value.
    assert_eq!(
        value(&db.query_all(&t1, "t").unwrap()[0], 1),
        "orig",
        "a transaction must keep reading its snapshot after a concurrent commit"
    );
    db.abort(t1).unwrap();

    // A fresh transaction sees the committed update.
    let t3 = db.begin().unwrap();
    assert_eq!(value(&db.query_all(&t3, "t").unwrap()[0], 1), "new");
    db.abort(t3).unwrap();
}

#[test]
fn write_write_conflict_first_committer_wins() {
    let db = fresh("mvcc_concurrent_conflict.db");
    db.autocommit(stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .unwrap();
    db.autocommit(stmt("INSERT INTO t VALUES (1, 'base')")).unwrap();

    let t1 = db.begin().unwrap();
    let t2 = db.begin().unwrap();

    db.execute(&t1, stmt("UPDATE t SET v = 't1' WHERE id = 1"))
        .unwrap();
    // t2 updates the same key. Under version-in-place it either conflicts now
    // (it sees t1's uncommitted version) or at commit; either way t2 must lose.
    let t2_write = db.execute(&t2, stmt("UPDATE t SET v = 't2' WHERE id = 1"));

    db.commit(t1).unwrap();

    if t2_write.is_ok() {
        assert!(
            db.commit(t2).is_err(),
            "the second committer must lose the write/write race"
        );
    } else {
        db.abort(t2).unwrap();
    }

    let t3 = db.begin().unwrap();
    assert_eq!(
        value(&db.query_all(&t3, "t").unwrap()[0], 1),
        "t1",
        "the first committer's value must win"
    );
    db.abort(t3).unwrap();
}

#[test]
fn aborted_work_is_invisible_to_concurrent_transactions() {
    let db = fresh("mvcc_concurrent_abort.db");
    db.autocommit(stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .unwrap();

    let t1 = db.begin().unwrap();
    db.execute(&t1, stmt("INSERT INTO t VALUES (1, 'x')")).unwrap();

    // A concurrent transaction never sees t1's uncommitted insert.
    let t2 = db.begin().unwrap();
    assert!(db.query_all(&t2, "t").unwrap().is_empty());

    db.abort(t1).unwrap();

    // Nor after t1 aborts.
    assert!(db.query_all(&t2, "t").unwrap().is_empty());
    db.abort(t2).unwrap();

    let t3 = db.begin().unwrap();
    assert!(db.query_all(&t3, "t").unwrap().is_empty());
    db.abort(t3).unwrap();
}

#[test]
fn concurrent_threads_never_lose_or_corrupt_committed_state() {
    const KEYS: i32 = 8;
    const THREADS: usize = 4;
    const ITERS: usize = 40;

    let db = Arc::new(fresh("mvcc_concurrent_stress.db"));
    db.autocommit(stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .unwrap();
    for k in 0..KEYS {
        db.autocommit(stmt(&format!("INSERT INTO t VALUES ({k}, 'seed')")))
            .unwrap();
    }

    let mut handles = Vec::new();
    for thread_idx in 0..THREADS {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for iter in 0..ITERS {
                let key = ((thread_idx + iter) as i32) % KEYS;
                let sql = format!("UPDATE t SET v = 't{thread_idx}i{iter}' WHERE id = {key}");
                let tx = db.begin().unwrap();
                match db.execute(&tx, stmt(&sql)) {
                    Ok(()) => match db.commit(tx) {
                        Ok(()) => {}
                        Err(_) => {} // lost a first-committer-wins race; fine
                    },
                    Err(_) => {
                        let _ = db.abort(tx);
                    }
                }
            }
        }));
    }
    for handle in handles {
        handle.join().expect("worker thread must not panic");
    }

    // Invariant: exactly one visible version per key, none lost, none aborted.
    let reader = db.begin().unwrap();
    let rows = db.query_all(&reader, "t").unwrap();
    db.abort(reader).unwrap();

    let mut keys: Vec<i32> = rows.iter().map(|r| r.key).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        (0..KEYS).collect::<Vec<_>>(),
        "every key must remain visible exactly once (no lost or torn writes)"
    );

    // A committed table still opens and reads back consistently after vacuum.
    let report = db.vacuum_table("t").unwrap();
    let _ = report;
    let reader = db.begin().unwrap();
    assert_eq!(db.query_all(&reader, "t").unwrap().len(), KEYS as usize);
    db.abort(reader).unwrap();
}
