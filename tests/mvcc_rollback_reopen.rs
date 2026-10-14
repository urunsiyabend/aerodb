//! A rolled-back transaction's id must never be reused after reopen: its
//! aborted row versions are on disk and its clog entry says Aborted.

use aerodb::{
    engine::Engine, execution::runtime::execute_select_with_indexes, sql::parser::parse_statement,
    storage::row::ColumnValue, storage::row::Row,
};
use std::fs;

fn setup_engine(filename: &str) -> Engine {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    let _ = fs::remove_file(format!("{}.clog", filename));
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

fn vals(rows: &[Row]) -> Vec<(i64, ColumnValue)> {
    let mut v: Vec<(i64, ColumnValue)> = rows
        .iter()
        .map(|r| (r.key as i64, r.data.0[1].clone()))
        .collect();
    v.sort_by_key(|(k, _)| *k);
    v
}

fn t(s: &str) -> ColumnValue {
    ColumnValue::Text(s.to_string())
}

#[test]
fn rolled_back_insert_then_reopen_does_not_reuse_tx_id() {
    let f = "mvcc_rollback_reopen_insert.db";
    let mut engine = setup_engine(f);
    exec(
        &mut engine,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
    );
    exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
    exec(&mut engine, "BEGIN");
    exec(&mut engine, "INSERT INTO t VALUES (2, 'x')");
    exec(&mut engine, "ROLLBACK");
    drop(engine);

    let mut engine = Engine::new(f);
    assert_eq!(vals(&select_all(&mut engine, "t")), vec![(1, t("a"))]);
    exec(&mut engine, "INSERT INTO t VALUES (2, 'y')");
    let expect = vec![(1, t("a")), (2, t("y"))];
    assert_eq!(vals(&select_all(&mut engine, "t")), expect);
    drop(engine);

    let mut engine = Engine::new(f);
    assert_eq!(vals(&select_all(&mut engine, "t")), expect);
}

#[test]
fn rolled_back_delete_then_reopen_keeps_row() {
    let f = "mvcc_rollback_reopen_delete.db";
    let mut engine = setup_engine(f);
    exec(
        &mut engine,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
    );
    exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
    exec(&mut engine, "BEGIN");
    exec(&mut engine, "INSERT INTO t VALUES (2, 'x')");
    exec(&mut engine, "DELETE FROM t WHERE id = 1");
    exec(&mut engine, "ROLLBACK");
    drop(engine);

    let mut engine = Engine::new(f);
    assert_eq!(vals(&select_all(&mut engine, "t")), vec![(1, t("a"))]);
    exec(&mut engine, "UPDATE t SET v = 'b' WHERE id = 1");
    exec(&mut engine, "INSERT INTO t VALUES (2, 'y')");
    drop(engine);

    let mut engine = Engine::new(f);
    assert_eq!(
        vals(&select_all(&mut engine, "t")),
        vec![(1, t("b")), (2, t("y"))]
    );
}

#[test]
fn failed_autocommit_multirow_insert_then_reopen() {
    let f = "mvcc_rollback_reopen_autocommit.db";
    let mut engine = setup_engine(f);
    exec(
        &mut engine,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
    );
    exec(&mut engine, "INSERT INTO t VALUES (1, 'a')");
    let res = engine.execute(parse_statement("INSERT INTO t VALUES (3, 'p'), (1, 'dup')").unwrap());
    assert!(res.is_err());
    drop(engine);

    let mut engine = Engine::new(f);
    assert_eq!(vals(&select_all(&mut engine, "t")), vec![(1, t("a"))]);
    exec(&mut engine, "INSERT INTO t VALUES (3, 'q')");
    drop(engine);

    let mut engine = Engine::new(f);
    assert_eq!(
        vals(&select_all(&mut engine, "t")),
        vec![(1, t("a")), (3, t("q"))]
    );
}
