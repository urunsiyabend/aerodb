//! Vacuum-driven advancement of the frozen watermark (`frozen_xid`) and the
//! clog compaction it authorizes.
//!
//! The durable clog records a 2-bit status per transaction id forever, so without
//! reclamation it grows without bound. An all-tables vacuum pass proves that no
//! live snapshot and no remaining on-disk version still needs to distinguish
//! transactions below `global_xmin`, which lets the watermark advance and those
//! statuses be dropped. Dropping them is safe because MVCC visibility already
//! treats an id absent below the snapshot bound as committed.
//!
//! The watermark must never pass a live snapshot: doing so would freeze an
//! aborted transaction as committed and resurrect its rows.

use aerodb::{
    db::Database, engine::Engine, execution::runtime::execute_select_with_indexes,
    sql::parser::parse_statement, transaction::clog::Clog,
};
use std::fs;

fn fresh_engine(filename: &str) -> Engine {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    let _ = fs::remove_file(format!("{}.clog", filename));
    Engine::new(filename)
}

fn fresh_db(filename: &str) -> Database {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    let _ = fs::remove_file(format!("{}.clog", filename));
    Database::open(filename).unwrap()
}

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_statement(sql).unwrap()).unwrap();
}

fn stmt(sql: &str) -> aerodb::sql::ast::Statement {
    parse_statement(sql).unwrap()
}

fn keys(engine: &mut Engine, table: &str) -> Vec<i32> {
    let mut rows = Vec::new();
    execute_select_with_indexes(&mut engine.catalog, table, None, &mut rows).unwrap();
    rows.iter().map(|row| row.key).collect()
}

/// How many transaction statuses the durable clog still reports. Read directly
/// from the sidecar so the assertion is about the on-disk state, not the
/// in-memory table.
fn clog_entries(filename: &str) -> usize {
    let mut clog = Clog::open(&format!("{}.clog", filename)).unwrap();
    clog.load().unwrap().len()
}

#[test]
fn vacuum_all_compacts_clog_without_changing_visibility() {
    let filename = "mvcc_frozen_compaction.db";

    {
        let mut engine = fresh_engine(filename);
        exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
        exec(&mut engine, "INSERT INTO t VALUES (2, 'b')");
        exec(&mut engine, "INSERT INTO t VALUES (3, 'c')");
        // An aborted transaction: its status is exactly what compaction must not
        // drop before its versions are reclaimed.
        exec(&mut engine, "BEGIN");
        exec(&mut engine, "INSERT INTO t VALUES (4, 'never')");
        exec(&mut engine, "ROLLBACK");
        // A committed delete, leaving a reclaimable tombstone.
        exec(&mut engine, "DELETE FROM t WHERE id = 2");
    }

    let before = clog_entries(filename);
    assert!(
        before >= 2,
        "each committed/aborted transaction holds a durable status ({before} recorded)"
    );

    {
        let mut engine = Engine::new(filename);
        assert_eq!(
            engine.frozen_xid(),
            0,
            "nothing is frozen before the first all-tables vacuum"
        );

        let report = engine.vacuum_all().unwrap();
        assert!(
            report.versions_removed >= 2,
            "the tombstone and the aborted version are both reclaimable, got {report:?}"
        );
        assert!(
            engine.frozen_xid() > 0,
            "an all-tables vacuum advances the watermark"
        );
        assert_eq!(keys(&mut engine, "t"), vec![1, 3]);
    }

    let after = clog_entries(filename);
    assert!(
        after < before,
        "compaction must reclaim statuses below the watermark ({before} -> {after})"
    );

    // The whole point: reclaiming those statuses changes nothing observable. The
    // aborted row stays gone (its version was vacuumed first), the deleted row
    // stays deleted, the live rows stay visible.
    let mut engine = Engine::new(filename);
    assert_eq!(
        keys(&mut engine, "t"),
        vec![1, 3],
        "visibility is unchanged by compaction"
    );
}

#[test]
fn rolled_back_delete_survives_compaction_and_reopen() {
    // A rolled-back DELETE leaves the live version stamped with an *aborted*
    // deleter. Once compaction drops that deleter's status, an absent id below
    // the bound reads as committed -- so vacuum must clear the stamp first, or
    // the row silently disappears after reopen.
    let filename = "mvcc_frozen_aborted_deleter.db";

    {
        let mut engine = fresh_engine(filename);
        exec(
            &mut engine,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        );
        exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
        exec(&mut engine, "INSERT INTO t VALUES (2, 'b')");
        exec(&mut engine, "BEGIN");
        exec(&mut engine, "DELETE FROM t WHERE id = 1");
        exec(&mut engine, "ROLLBACK");
        exec(&mut engine, "BEGIN");
        exec(&mut engine, "UPDATE t SET v = 'z' WHERE id = 2");
        exec(&mut engine, "ROLLBACK");
        // A later commit truncates the WAL, so after reopen the clog is the only
        // witness of those aborts.
        exec(&mut engine, "INSERT INTO t VALUES (3, 'c')");
        assert_eq!(keys(&mut engine, "t"), vec![1, 2, 3]);

        engine.vacuum_all().unwrap();
        assert!(engine.frozen_xid() > 0);
        assert_eq!(keys(&mut engine, "t"), vec![1, 2, 3]);
    }

    let mut engine = Engine::new(filename);
    assert_eq!(
        keys(&mut engine, "t"),
        vec![1, 2, 3],
        "rows whose delete was rolled back must stay visible after compaction"
    );
}

#[test]
fn per_table_vacuum_does_not_advance_the_watermark() {
    let filename = "mvcc_frozen_per_table.db";
    let mut engine = fresh_engine(filename);
    exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
    exec(&mut engine, "INSERT INTO t VALUES (2, 'b')");
    exec(&mut engine, "DELETE FROM t WHERE id = 2");

    let report = engine.vacuum_table("t").unwrap();
    assert!(report.versions_removed >= 1, "the tombstone is reclaimable");
    assert_eq!(
        engine.frozen_xid(),
        0,
        "a single-table pass cannot prove other tables hold no versions for those ids"
    );
}

#[test]
fn watermark_never_passes_a_live_snapshot() {
    let filename = "mvcc_frozen_live_snapshot.db";
    let db = fresh_db(filename);
    db.autocommit(stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
        .unwrap();
    db.autocommit(stmt("INSERT INTO t VALUES (1, 'a')")).unwrap();
    db.autocommit(stmt("INSERT INTO t VALUES (2, 'b')")).unwrap();

    // A long-lived reader holds the oldest live snapshot.
    let reader = db.begin().unwrap();
    let before: Vec<i32> = db
        .query_all(&reader, "t")
        .unwrap()
        .iter()
        .map(|row| row.key)
        .collect();
    assert_eq!(before, vec![1, 2]);

    // Work commits and aborts underneath it, then a full vacuum runs.
    let writer = db.begin().unwrap();
    db.execute(&writer, stmt("DELETE FROM t WHERE id = 2")).unwrap();
    db.commit(writer).unwrap();

    let doomed = db.begin().unwrap();
    db.execute(&doomed, stmt("INSERT INTO t VALUES (3, 'never')"))
        .unwrap();
    db.abort(doomed).unwrap();

    db.vacuum_all().unwrap();

    let frozen = db.frozen_xid();
    assert!(frozen > 0, "the vacuum should still have advanced the watermark");
    assert!(
        frozen <= reader.id(),
        "watermark {} passed the live snapshot {}",
        db.frozen_xid(),
        reader.id()
    );

    // The reader's snapshot is still correct: it predates the delete and the
    // aborted insert, so it sees exactly what it saw at BEGIN.
    let after: Vec<i32> = db
        .query_all(&reader, "t")
        .unwrap()
        .iter()
        .map(|row| row.key)
        .collect();
    assert_eq!(
        after, before,
        "vacuum must not disturb a live snapshot's read set"
    );

    db.abort(reader).unwrap();
}

#[test]
fn frozen_xid_persists_across_reopen() {
    let filename = "mvcc_frozen_persist.db";

    let advanced = {
        let mut engine = fresh_engine(filename);
        exec(&mut engine, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
        exec(&mut engine, "DELETE FROM t WHERE id = 1");
        engine.vacuum_all().unwrap();
        let frozen = engine.frozen_xid();
        assert!(frozen > 0, "vacuum_all advanced the watermark");
        frozen
    };

    let mut reopened = Engine::new(filename);
    assert_eq!(
        reopened.frozen_xid(),
        advanced,
        "the watermark lives in page-0 meta and survives reopen"
    );

    // A second pass in a later session advances it further (ids have moved on)
    // and never regresses.
    exec(&mut reopened, "INSERT INTO t VALUES (2, 'b')");
    reopened.vacuum_all().unwrap();
    assert!(
        reopened.frozen_xid() > advanced,
        "the watermark advances monotonically across sessions"
    );
}
