//! B-Tree behaviour under MVCC version chains.
//!
//! Every UPDATE appends a new physical version with the same logical key, so a
//! frequently-updated key accumulates many versions that can straddle leaf
//! splits and produce repeated separator keys in internal nodes. Routing and
//! point lookups must stay correct when that happens.

use aerodb::{
    engine::Engine, execution::runtime::execute_select_with_indexes, sql::parser::parse_statement,
    storage::row::ColumnValue,
};
use std::fs;

fn fresh_engine(filename: &str) -> Engine {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    let _ = fs::remove_file(format!("{}.clog", filename));
    Engine::new(filename)
}

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_statement(sql).unwrap()).unwrap();
}

fn rows(engine: &mut Engine, table: &str) -> Vec<(i32, String)> {
    let mut out = Vec::new();
    execute_select_with_indexes(&mut engine.catalog, table, None, &mut out).unwrap();
    out.iter()
        .map(|row| {
            let v = match &row.data.0[1] {
                ColumnValue::Text(s) => s.clone(),
                other => format!("{other:?}"),
            };
            (row.key, v)
        })
        .collect()
}

/// A payload big enough that a handful of versions fills a 4 KiB leaf, so the
/// version chain of one key spans many splits.
fn payload(tag: usize) -> String {
    format!("{tag:04}{}", "x".repeat(300))
}

#[test]
fn heavily_updated_key_keeps_exactly_one_visible_version() {
    let filename = "btree_versions_hot_key.db";
    let mut engine = fresh_engine(filename);
    exec(
        &mut engine,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
    );
    for id in [1, 2, 3] {
        exec(
            &mut engine,
            &format!("INSERT INTO t VALUES ({id}, '{}')", payload(0)),
        );
    }

    for round in 1..=120 {
        exec(
            &mut engine,
            &format!("UPDATE t SET v = '{}' WHERE id = 2", payload(round)),
        );
        let visible = rows(&mut engine, "t");
        assert_eq!(
            visible.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "round {round}: every key has exactly one visible version"
        );
        assert_eq!(
            visible[1].1,
            payload(round),
            "round {round}: newest value wins"
        );
    }
}

#[test]
fn hot_key_survives_reopen_and_further_updates() {
    let filename = "btree_versions_hot_key_reopen.db";
    {
        let mut engine = fresh_engine(filename);
        exec(
            &mut engine,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        );
        for id in 1..=5 {
            exec(
                &mut engine,
                &format!("INSERT INTO t VALUES ({id}, '{}')", payload(0)),
            );
        }
        for round in 1..=60 {
            let id = if round % 3 == 0 { 4 } else { 3 };
            exec(
                &mut engine,
                &format!("UPDATE t SET v = '{}' WHERE id = {id}", payload(round)),
            );
        }
    }

    let mut engine = Engine::new(filename);
    exec(
        &mut engine,
        &format!("UPDATE t SET v = '{}' WHERE id = 3", payload(999)),
    );
    exec(&mut engine, "DELETE FROM t WHERE id = 4");
    let visible = rows(&mut engine, "t");
    assert_eq!(
        visible.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
        vec![1, 2, 3, 5]
    );
    assert_eq!(visible[2].1, payload(999));
}
