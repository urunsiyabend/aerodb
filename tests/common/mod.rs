//! Shared helpers for the SQL conformance test suites.
#![allow(dead_code)]

use aerodb::engine::Engine;
use aerodb::error::DbError;

/// A fresh engine backed by a database file inside a temporary directory.
pub fn engine() -> (tempfile::TempDir, Engine) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.db");
    let engine = Engine::new(path.to_str().expect("utf-8 path"));
    (dir, engine)
}

/// Execute a statement that must succeed.
pub fn exec(engine: &mut Engine, sql: &str) {
    engine
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("execute `{sql}`: {e}"));
}

/// Run a query and render every value as text (`NULL` for SQL NULL).
pub fn rows(engine: &mut Engine, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let result = engine.query(sql).map_err(|e| e.to_string())?;
    Ok(result
        .rows
        .iter()
        .map(|row| row.iter().map(|v| v.to_string()).collect())
        .collect())
}

/// Run a statement that must fail and return its error.
pub fn err(engine: &mut Engine, sql: &str) -> DbError {
    match engine.query(sql) {
        Err(e) => e,
        Ok(r) => panic!("expected `{sql}` to fail, got {:?}", r.rows),
    }
}

/// Sort rows so multiset comparisons ignore order.
pub fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

/// Build an expected row set from string slices.
pub fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}
