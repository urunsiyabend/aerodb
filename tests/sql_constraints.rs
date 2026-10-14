//! UNIQUE and CHECK constraints (with nullable UNIQUE semantics), their
//! catalog persistence, ON UPDATE CASCADE / RESTRICT, and explicit-transaction
//! statement atomicity (PostgreSQL-like failed-transaction policy, 25P02)
//! end to end (0.6.0 task 5).
mod common;

use aerodb::db::Database;
use aerodb::engine::Engine;
use aerodb::error::DbError;
use common::{engine, err, exec, rows, sorted, table};
use std::path::PathBuf;
use tempfile::TempDir;

fn q(e: &mut Engine, sql: &str) -> Vec<Vec<String>> {
    rows(e, sql).unwrap_or_else(|m| panic!("`{sql}` failed: {m}"))
}

fn q_set(e: &mut Engine, sql: &str) -> Vec<Vec<String>> {
    sorted(q(e, sql))
}

fn state(e: &mut Engine, sql: &str) -> &'static str {
    err(e, sql).sqlstate()
}

fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("test.db")
}

/// Drop `e` and open a fresh engine on the same database file.
fn reopen(dir: &TempDir, e: Engine) -> Engine {
    drop(e);
    Engine::new(db_path(dir).to_str().unwrap())
}

/// The error must be a UNIQUE violation of `constraint` (sqlstate 23505).
fn assert_unique(e: &DbError, constraint: &str) {
    match e {
        DbError::UniqueViolation { constraint: c, .. } => assert_eq!(c, constraint, "{e}"),
        other => panic!("expected UniqueViolation({constraint}), got {other:?}"),
    }
    assert_eq!(e.sqlstate(), "23505");
    assert!(
        e.to_string().starts_with(&format!("duplicate key value violates unique constraint {constraint}: ")),
        "{e}"
    );
}

/// The error must be a CHECK violation of `constraint` (sqlstate 23514).
fn assert_check(e: &DbError, constraint: &str) {
    match e {
        DbError::CheckViolation { constraint: c } => assert_eq!(c, constraint, "{e}"),
        other => panic!("expected CheckViolation({constraint}), got {other:?}"),
    }
    assert_eq!(e.sqlstate(), "23514");
    assert_eq!(e.to_string(), format!("new row violates check constraint {constraint}"));
}

fn assert_failed_tx(e: &DbError) {
    assert!(matches!(e, DbError::InFailedTransaction), "expected InFailedTransaction, got {e:?}");
    assert_eq!(e.sqlstate(), "25P02");
    assert_eq!(
        e.to_string(),
        "current transaction is aborted, commands ignored until end of transaction block"
    );
}

// ---- UNIQUE -------------------------------------------------------------------

#[test]
fn unique_audit_probe() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER NOT NULL UNIQUE)");
    exec(&mut e, "INSERT INTO d VALUES (1, 1)");
    let v = err(&mut e, "INSERT INTO d VALUES (2, 1)");
    assert_unique(&v, "d_n_key");
    assert_eq!(q(&mut e, "SELECT id, n FROM d"), table(&[&["1", "1"]]));
    exec(&mut e, "INSERT INTO d VALUES (2, 2)");
}

#[test]
fn nullable_unique_allows_two_nulls() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER UNIQUE)");
    exec(&mut e, "INSERT INTO d VALUES (1, NULL)");
    exec(&mut e, "INSERT INTO d VALUES (2, NULL)");
    exec(&mut e, "INSERT INTO d VALUES (3, NULL), (4, NULL)");
    exec(&mut e, "UPDATE d SET n = NULL WHERE id = 1");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM d WHERE n IS NULL"), table(&[&["4"]]));
}

#[test]
fn composite_unique_with_partial_null_allowed_twice() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE (a, b))");
    exec(&mut e, "INSERT INTO t VALUES (1, 1, NULL)");
    exec(&mut e, "INSERT INTO t VALUES (2, 1, NULL)");
    exec(&mut e, "INSERT INTO t VALUES (3, 1, 2)");
    exec(&mut e, "INSERT INTO t VALUES (4, 2, 1)");
    let v = err(&mut e, "INSERT INTO t VALUES (5, 1, 2)");
    assert_unique(&v, "t_a_b_key");
    assert!(v.to_string().contains("(1, 2)"), "{v}");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t"), table(&[&["4"]]));
}

#[test]
fn unique_duplicate_within_multi_row_insert_has_no_effect() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE u (id INTEGER PRIMARY KEY, n INTEGER UNIQUE)");
    let v = err(&mut e, "INSERT INTO u VALUES (1, 1), (2, 2), (3, 1)");
    assert_unique(&v, "u_n_key");
    assert!(q(&mut e, "SELECT id FROM u").is_empty());
    exec(&mut e, "INSERT INTO u VALUES (1, 1), (2, 2)");
    assert_eq!(q_set(&mut e, "SELECT n FROM u"), table(&[&["1"], &["2"]]));
}

#[test]
fn update_creating_unique_duplicate_is_rejected() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE u (id INTEGER PRIMARY KEY, n INTEGER UNIQUE)");
    exec(&mut e, "INSERT INTO u VALUES (1, 1), (2, 2), (3, 3)");
    let v = err(&mut e, "UPDATE u SET n = 1 WHERE id = 2");
    assert_unique(&v, "u_n_key");
    // Two rows updated onto one value in one statement.
    let v = err(&mut e, "UPDATE u SET n = 7 WHERE id > 1");
    assert_unique(&v, "u_n_key");
    assert_eq!(q_set(&mut e, "SELECT id, n FROM u"), table(&[&["1", "1"], &["2", "2"], &["3", "3"]]));
    // Re-assigning a row its own value is not a conflict.
    exec(&mut e, "UPDATE u SET n = n");
}

#[test]
fn update_swapping_unique_values_succeeds() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE x (id INTEGER PRIMARY KEY, n INTEGER UNIQUE)");
    exec(&mut e, "INSERT INTO x VALUES (1, 1), (2, 2)");
    exec(&mut e, "UPDATE x SET n = 3 - n");
    assert_eq!(q_set(&mut e, "SELECT id, n FROM x"), table(&[&["1", "2"], &["2", "1"]]));
}

#[test]
fn unique_violation_in_explicit_tx_then_rollback_leaves_no_trace() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE u (id INTEGER PRIMARY KEY, n INTEGER UNIQUE)");
    exec(&mut e, "BEGIN");
    exec(&mut e, "INSERT INTO u VALUES (1, 1)");
    let v = err(&mut e, "INSERT INTO u VALUES (2, 1)");
    assert_unique(&v, "u_n_key");
    exec(&mut e, "ROLLBACK");
    assert!(q(&mut e, "SELECT id FROM u").is_empty());
    exec(&mut e, "INSERT INTO u VALUES (2, 1)");
    assert_eq!(q(&mut e, "SELECT id, n FROM u"), table(&[&["2", "1"]]));
}

#[test]
fn foreign_key_may_reference_a_unique_column() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
    exec(&mut e, "CREATE TABLE ch (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES p(code))");
    exec(&mut e, "INSERT INTO p VALUES (1, 'a'), (2, 'b')");
    exec(&mut e, "INSERT INTO ch VALUES (1, 'a')");
    assert_eq!(state(&mut e, "INSERT INTO ch VALUES (2, 'zz')"), "23503");
    assert_eq!(state(&mut e, "DELETE FROM p WHERE code = 'a'"), "23503");
    exec(&mut e, "DELETE FROM p WHERE code = 'b'");
    // Composite UNIQUE target, and a column-level REFERENCES.
    exec(&mut e, "CREATE TABLE p2 (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE (a, b))");
    exec(&mut e, "CREATE TABLE ch2 (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES p2(a, b))");
    exec(&mut e, "CREATE TABLE ch3 (id INTEGER PRIMARY KEY, pcode TEXT REFERENCES p(code))");
    exec(&mut e, "INSERT INTO p2 VALUES (1, 1, 1)");
    exec(&mut e, "INSERT INTO ch2 VALUES (1, 1, 1)");
    assert_eq!(state(&mut e, "INSERT INTO ch2 VALUES (2, 1, 2)"), "23503");
    exec(&mut e, "INSERT INTO ch3 VALUES (1, 'a')");
    assert_eq!(state(&mut e, "INSERT INTO ch3 VALUES (2, 'q')"), "23503");
    // A column set that is neither the PRIMARY KEY nor UNIQUE is still rejected.
    exec(&mut e, "CREATE TABLE p3 (id INTEGER PRIMARY KEY, v INTEGER)");
    assert_eq!(state(&mut e, "CREATE TABLE ch4 (id INTEGER, v INTEGER, FOREIGN KEY (v) REFERENCES p3(v))"), "42830");
}

// ---- CHECK --------------------------------------------------------------------

#[test]
fn check_audit_probe() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))");
    let v = err(&mut e, "INSERT INTO d VALUES (1, -1)");
    assert_check(&v, "d_n_check");
    assert!(q(&mut e, "SELECT id FROM d").is_empty());
    exec(&mut e, "INSERT INTO d VALUES (1, 5)");
}

#[test]
fn check_null_is_unknown_and_passes() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))");
    exec(&mut e, "INSERT INTO d VALUES (1, NULL)");
    exec(&mut e, "INSERT INTO d (id) VALUES (2)");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM d"), table(&[&["2"]]));
}

#[test]
fn check_update_to_violating_value_is_rejected() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))");
    exec(&mut e, "INSERT INTO d VALUES (1, 1), (2, 2)");
    let v = err(&mut e, "UPDATE d SET n = n - 2");
    assert_check(&v, "d_n_check");
    assert_eq!(q_set(&mut e, "SELECT n FROM d"), table(&[&["1"], &["2"]]));
    exec(&mut e, "UPDATE d SET n = n + 10");
    assert_eq!(q_set(&mut e, "SELECT n FROM d"), table(&[&["11"], &["12"]]));
}

#[test]
fn table_check_across_two_columns() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE r (id INTEGER PRIMARY KEY, lo INTEGER, hi INTEGER, CHECK (lo <= hi))");
    exec(&mut e, "INSERT INTO r VALUES (1, 1, 2), (2, 3, 3), (3, NULL, 1)");
    let v = err(&mut e, "INSERT INTO r VALUES (4, 5, 4)");
    assert_check(&v, "r_check1");
    let v = err(&mut e, "UPDATE r SET hi = 0 WHERE id = 1");
    assert_check(&v, "r_check1");
    // Second unnamed table CHECK gets n = 2.
    exec(&mut e, "CREATE TABLE r2 (a INTEGER, b INTEGER, CHECK (a > 0), CHECK (b > 0))");
    let v = err(&mut e, "INSERT INTO r2 VALUES (1, 0)");
    assert_check(&v, "r2_check2");
}

#[test]
fn check_multi_row_insert_with_one_bad_row_inserts_nothing() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))");
    let v = err(&mut e, "INSERT INTO d VALUES (1, 1), (2, 0), (3, 3)");
    assert_check(&v, "d_n_check");
    assert!(q(&mut e, "SELECT id FROM d").is_empty());
}

#[test]
fn named_constraints_appear_in_errors() {
    let (_dir, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER CONSTRAINT positive CHECK (n > 0), \
         m INTEGER CONSTRAINT m_once UNIQUE, CONSTRAINT pair_ok CHECK (n <> m), CONSTRAINT nm UNIQUE (n, m))",
    );
    assert_check(&err(&mut e, "INSERT INTO d VALUES (1, 0, 1)"), "positive");
    assert_check(&err(&mut e, "INSERT INTO d VALUES (1, 2, 2)"), "pair_ok");
    exec(&mut e, "INSERT INTO d VALUES (1, 1, 2)");
    assert_unique(&err(&mut e, "INSERT INTO d VALUES (2, 3, 2)"), "m_once");
}

#[test]
fn check_referencing_another_tables_column_is_42703() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE o (x INTEGER)");
    assert_eq!(state(&mut e, "CREATE TABLE d (n INTEGER CHECK (n > o.x))"), "42703");
    assert_eq!(state(&mut e, "CREATE TABLE d (n INTEGER, CHECK (n > x))"), "42703");
    // The failed CREATE TABLE left no table behind.
    assert_eq!(state(&mut e, "SELECT n FROM d"), "42P01");
    // Qualifying with the table's own name is fine.
    exec(&mut e, "CREATE TABLE d (n INTEGER CHECK (d.n > 0))");
    assert_eq!(state(&mut e, "INSERT INTO d VALUES (0)"), "23514");
}

#[test]
fn check_with_subquery_or_aggregate_is_0a000() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE o (x INTEGER)");
    assert_eq!(state(&mut e, "CREATE TABLE d (n INTEGER CHECK (n > (SELECT MAX(x) FROM o)))"), "0A000");
    assert_eq!(state(&mut e, "CREATE TABLE d (n INTEGER CHECK (EXISTS (SELECT 1 FROM o)))"), "0A000");
    assert_eq!(state(&mut e, "CREATE TABLE d (n INTEGER, CHECK (n IN (SELECT x FROM o)))"), "0A000");
    assert_eq!(state(&mut e, "CREATE TABLE d (n INTEGER CHECK (SUM(n) > 0))"), "0A000");
}

// ---- persistence --------------------------------------------------------------

#[test]
fn unique_and_check_survive_reopen() {
    let (dir, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER UNIQUE CHECK (n > 0), \
         a INTEGER, b INTEGER, CONSTRAINT ab UNIQUE (a, b), CONSTRAINT ordered CHECK (a < b))",
    );
    exec(&mut e, "CREATE TABLE ch (id INTEGER PRIMARY KEY, n INTEGER, FOREIGN KEY (n) REFERENCES d(n) ON UPDATE CASCADE)");
    exec(&mut e, "INSERT INTO d VALUES (1, 1, 1, 2)");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1)");
    let mut e = reopen(&dir, e);
    assert_unique(&err(&mut e, "INSERT INTO d VALUES (2, 1, 5, 6)"), "d_n_key");
    assert_check(&err(&mut e, "INSERT INTO d VALUES (2, -1, 5, 6)"), "d_n_check");
    assert_unique(&err(&mut e, "INSERT INTO d VALUES (2, 2, 1, 2)"), "ab");
    assert_check(&err(&mut e, "INSERT INTO d VALUES (2, 2, 7, 6)"), "ordered");
    // The ON UPDATE CASCADE action and the UNIQUE FK target were reloaded too.
    exec(&mut e, "UPDATE d SET n = 9 WHERE id = 1");
    assert_eq!(q(&mut e, "SELECT n FROM ch"), table(&[&["9"]]));
    let mut e = reopen(&dir, e);
    assert_eq!(q(&mut e, "SELECT n FROM ch"), table(&[&["9"]]));
    assert_unique(&err(&mut e, "INSERT INTO d VALUES (3, 9, 5, 6)"), "d_n_key");
}

// ---- ON UPDATE CASCADE / RESTRICT -----------------------------------------------

#[test]
fn fk_update_cascade() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id) ON UPDATE CASCADE)");
    exec(&mut e, "INSERT INTO p VALUES (1)");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1)");
    exec(&mut e, "UPDATE p SET id = 2 WHERE id = 1");
    assert_eq!(q(&mut e, "SELECT pid FROM ch"), table(&[&["2"]]));
    assert_eq!(q(&mut e, "SELECT id FROM p"), table(&[&["2"]]));
}

#[test]
fn composite_on_update_cascade() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");
    exec(
        &mut e,
        "CREATE TABLE ch (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
         FOREIGN KEY (a, b) REFERENCES p(a, b) ON UPDATE CASCADE)",
    );
    exec(&mut e, "INSERT INTO p VALUES (1, 1), (1, 2)");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1, 1), (2, 1, 2), (3, 1, NULL)");
    exec(&mut e, "UPDATE p SET b = b + 10 WHERE a = 1");
    assert_eq!(
        q_set(&mut e, "SELECT id, a, b FROM ch"),
        table(&[&["1", "1", "11"], &["2", "1", "12"], &["3", "1", "NULL"]])
    );
    // Swapping keys within one statement cascades to the right children.
    exec(&mut e, "UPDATE p SET b = 23 - b");
    assert_eq!(
        q_set(&mut e, "SELECT id, a, b FROM ch"),
        table(&[&["1", "1", "12"], &["2", "1", "11"], &["3", "1", "NULL"]])
    );
}

#[test]
fn on_update_cascade_chain_grandparent_parent_child() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE gp (id INTEGER PRIMARY KEY)");
    exec(
        &mut e,
        "CREATE TABLE p (gid INTEGER PRIMARY KEY, FOREIGN KEY (gid) REFERENCES gp(id) ON UPDATE CASCADE)",
    );
    exec(
        &mut e,
        "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(gid) ON UPDATE CASCADE)",
    );
    exec(&mut e, "INSERT INTO gp VALUES (1), (2)");
    exec(&mut e, "INSERT INTO p VALUES (1), (2)");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1), (2, 2), (3, 1)");
    exec(&mut e, "UPDATE gp SET id = 10 WHERE id = 1");
    assert_eq!(q_set(&mut e, "SELECT id FROM gp"), table(&[&["10"], &["2"]]));
    assert_eq!(q_set(&mut e, "SELECT gid FROM p"), table(&[&["10"], &["2"]]));
    assert_eq!(q_set(&mut e, "SELECT id, pid FROM ch"), table(&[&["1", "10"], &["2", "2"], &["3", "10"]]));
}

#[test]
fn cascade_into_child_whose_check_rejects_fails_the_whole_update() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY, s TEXT)");
    exec(
        &mut e,
        "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER CONSTRAINT small CHECK (pid < 100), \
         FOREIGN KEY (pid) REFERENCES p(id) ON UPDATE CASCADE)",
    );
    exec(&mut e, "INSERT INTO p VALUES (1, 'a'), (2, 'b')");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1), (2, 2)");
    let v = err(&mut e, "UPDATE p SET id = id * 100, s = 'changed'");
    assert_check(&v, "small");
    assert_eq!(q_set(&mut e, "SELECT id, s FROM p"), table(&[&["1", "a"], &["2", "b"]]));
    assert_eq!(q_set(&mut e, "SELECT id, pid FROM ch"), table(&[&["1", "1"], &["2", "2"]]));
    // A cascaded child image is re-checked against its UNIQUE constraint
    // (here it stays unique, so the cascade goes through).
    exec(&mut e, "CREATE TABLE u (id INTEGER PRIMARY KEY, pid INTEGER UNIQUE, FOREIGN KEY (pid) REFERENCES p(id) ON UPDATE CASCADE)");
    exec(&mut e, "INSERT INTO p VALUES (3, 'c')");
    exec(&mut e, "INSERT INTO u VALUES (1, 3)");
    exec(&mut e, "UPDATE p SET id = 4 WHERE id = 3");
    assert_eq!(q(&mut e, "SELECT pid FROM u"), table(&[&["4"]]));
}

#[test]
fn on_update_restrict_rejects() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id) ON UPDATE RESTRICT)");
    exec(&mut e, "INSERT INTO p VALUES (1), (2)");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1)");
    assert_eq!(state(&mut e, "UPDATE p SET id = 5 WHERE id = 1"), "23503");
    // Unreferenced parent keys may change.
    exec(&mut e, "UPDATE p SET id = 6 WHERE id = 2");
    assert_eq!(q_set(&mut e, "SELECT id FROM p"), table(&[&["1"], &["6"]]));
    assert_eq!(q(&mut e, "SELECT pid FROM ch"), table(&[&["1"]]));
}

#[test]
fn no_action_allows_update_of_non_referenced_parent_column() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY, s TEXT)");
    exec(&mut e, "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id) ON UPDATE NO ACTION)");
    exec(&mut e, "INSERT INTO p VALUES (1, 'a')");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1)");
    exec(&mut e, "UPDATE p SET s = 'b' WHERE id = 1");
    assert_eq!(q(&mut e, "SELECT id, s FROM p"), table(&[&["1", "b"]]));
    assert_eq!(state(&mut e, "UPDATE p SET id = 2"), "23503");
}

#[test]
fn on_delete_restrict_behaves_like_no_action() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id) ON DELETE RESTRICT)");
    exec(&mut e, "INSERT INTO p VALUES (1), (2)");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1)");
    assert_eq!(state(&mut e, "DELETE FROM p WHERE id = 1"), "23503");
    exec(&mut e, "DELETE FROM p WHERE id = 2");
    // Deleting the parent together with its only child in one transaction works.
    exec(&mut e, "BEGIN");
    exec(&mut e, "DELETE FROM ch");
    exec(&mut e, "DELETE FROM p");
    exec(&mut e, "COMMIT");
    assert!(q(&mut e, "SELECT id FROM p").is_empty());
}

// ---- explicit transaction atomicity (25P02) ---------------------------------------

#[test]
fn failed_statement_aborts_explicit_tx() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    exec(&mut e, "BEGIN");
    exec(&mut e, "INSERT INTO t VALUES (1)");
    assert_eq!(state(&mut e, "INSERT INTO t VALUES (1)"), "23505");
    assert_failed_tx(&err(&mut e, "INSERT INTO t VALUES (2)"));
    assert_failed_tx(&err(&mut e, "SELECT id FROM t"));
    assert_failed_tx(&err(&mut e, "COMMIT"));
    assert!(q(&mut e, "SELECT id FROM t").is_empty());
    // The failed transaction is over: autocommit works again.
    exec(&mut e, "INSERT INTO t VALUES (3)");
    assert_eq!(q(&mut e, "SELECT id FROM t"), table(&[&["3"]]));
}

#[test]
fn rollback_after_failure_then_new_begin_works() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))");
    exec(&mut e, "BEGIN");
    exec(&mut e, "INSERT INTO t VALUES (1, 1)");
    assert_eq!(state(&mut e, "INSERT INTO t VALUES (2, -1)"), "23514");
    assert_failed_tx(&err(&mut e, "UPDATE t SET n = 2"));
    exec(&mut e, "ROLLBACK");
    exec(&mut e, "BEGIN");
    exec(&mut e, "INSERT INTO t VALUES (5, 5)");
    exec(&mut e, "COMMIT");
    assert_eq!(q(&mut e, "SELECT id, n FROM t"), table(&[&["5", "5"]]));
}

#[test]
fn parse_error_inside_explicit_tx_also_aborts_it() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    exec(&mut e, "BEGIN");
    exec(&mut e, "INSERT INTO t VALUES (1)");
    assert_eq!(state(&mut e, "INSERT INTO t VALUES (2"), "42601");
    assert_failed_tx(&err(&mut e, "INSERT INTO t VALUES (3)"));
    assert_failed_tx(&err(&mut e, "COMMIT"));
    assert!(q(&mut e, "SELECT id FROM t").is_empty());
}

#[test]
fn partial_multi_row_insert_in_explicit_tx_never_commits() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&mut e, "INSERT INTO t VALUES (1, 1)");
    exec(&mut e, "BEGIN");
    assert_eq!(state(&mut e, "INSERT INTO t VALUES (5, 30), (6, 31), (1, 99)"), "23505");
    assert_failed_tx(&err(&mut e, "COMMIT"));
    assert_eq!(q(&mut e, "SELECT id FROM t"), table(&[&["1"]]));
}

#[test]
fn autocommit_multi_insert_is_still_atomic() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, s TEXT)");
    exec(&mut e, "INSERT INTO t VALUES (1, 20, 'b'), (2, 10, 'a'), (3, NULL, 'c'), (4, 20, 'b')");
    assert_eq!(state(&mut e, "INSERT INTO t VALUES (5, 30, 'x'), (1, 99, 'dup')"), "23505");
    assert_eq!(q_set(&mut e, "SELECT id FROM t"), table(&[&["1"], &["2"], &["3"], &["4"]]));
    // Autocommit errors do not poison later statements.
    exec(&mut e, "INSERT INTO t VALUES (5, 30, 'x')");
}

#[test]
fn cascade_chain_failure_inside_explicit_tx_commits_nothing() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id) ON DELETE CASCADE)");
    exec(&mut e, "CREATE TABLE gc (id INTEGER PRIMARY KEY, cid INTEGER, FOREIGN KEY (cid) REFERENCES ch(id))");
    exec(&mut e, "INSERT INTO p VALUES (1), (2)");
    exec(&mut e, "INSERT INTO ch VALUES (10, 1), (20, 2)");
    exec(&mut e, "INSERT INTO gc VALUES (100, 20)");
    exec(&mut e, "BEGIN");
    exec(&mut e, "INSERT INTO p VALUES (3)");
    // p 1 → ch 10 cascades fine, p 2 → ch 20 is still referenced by gc 100.
    assert_eq!(state(&mut e, "DELETE FROM p"), "23503");
    assert_failed_tx(&err(&mut e, "COMMIT"));
    assert_eq!(q_set(&mut e, "SELECT id FROM p"), table(&[&["1"], &["2"]]));
    assert_eq!(q_set(&mut e, "SELECT id FROM ch"), table(&[&["10"], &["20"]]));
    assert_eq!(q(&mut e, "SELECT id FROM gc"), table(&[&["100"]]));
}

// ---- Database API -------------------------------------------------------------------

fn open_db(dir: &TempDir) -> Database {
    Database::open(db_path(dir).to_str().unwrap()).unwrap()
}

fn db_rows(db: &Database, sql: &str) -> Vec<Vec<String>> {
    let tx = db.begin().unwrap();
    let r = db.query(&tx, sql).unwrap();
    db.commit(tx).unwrap();
    sorted(r.rows.iter().map(|row| row.iter().map(|v| v.to_string()).collect()).collect())
}

#[test]
fn database_failed_statement_aborts_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(&dir);
    let setup = db.begin().unwrap();
    db.query(&setup, "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER UNIQUE)").unwrap();
    db.commit(setup).unwrap();

    let tx = db.begin().unwrap();
    db.query(&tx, "INSERT INTO t VALUES (1, 1)").unwrap();
    let v = db.query(&tx, "INSERT INTO t VALUES (2, 1)").unwrap_err();
    assert_unique(&v, "t_n_key");
    assert_failed_tx(&db.query(&tx, "INSERT INTO t VALUES (3, 3)").unwrap_err());
    assert_failed_tx(&db.query(&tx, "SELECT id FROM t").unwrap_err());
    assert_failed_tx(&db.commit(tx).unwrap_err());
    assert!(db_rows(&db, "SELECT id FROM t").is_empty());

    // abort() after a failure ends the transaction normally.
    let tx = db.begin().unwrap();
    db.query(&tx, "INSERT INTO t VALUES (4, 4)").unwrap();
    assert!(db.query(&tx, "INSERT INTO t VALUES (4, 5)").is_err());
    db.abort(tx).unwrap();
    assert!(db_rows(&db, "SELECT id FROM t").is_empty());
}

#[test]
fn database_delete_then_reinsert_same_pk_in_one_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(&dir);
    let setup = db.begin().unwrap();
    db.query(&setup, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    db.query(&setup, "INSERT INTO t VALUES (1, 'old')").unwrap();
    db.commit(setup).unwrap();

    // Committed row.
    let tx = db.begin().unwrap();
    db.query(&tx, "DELETE FROM t WHERE id = 1").unwrap();
    db.query(&tx, "INSERT INTO t VALUES (1, 'new')").unwrap();
    db.commit(tx).unwrap();
    assert_eq!(db_rows(&db, "SELECT id, v FROM t"), table(&[&["1", "new"]]));

    // Row inserted (uncommitted) by the same transaction.
    let tx = db.begin().unwrap();
    db.query(&tx, "INSERT INTO t VALUES (2, 'a')").unwrap();
    db.query(&tx, "DELETE FROM t WHERE id = 2").unwrap();
    db.query(&tx, "INSERT INTO t VALUES (2, 'b')").unwrap();
    db.commit(tx).unwrap();
    assert_eq!(db_rows(&db, "SELECT id, v FROM t"), table(&[&["1", "new"], &["2", "b"]]));
}

#[test]
fn database_concurrent_same_pk_inserts_second_commit_is_duplicate_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(&dir);
    let setup = db.begin().unwrap();
    db.query(&setup, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    db.query(&setup, "CREATE TABLE u (id INTEGER PRIMARY KEY, n INTEGER UNIQUE)").unwrap();
    db.commit(setup).unwrap();

    let t1 = db.begin().unwrap();
    let t2 = db.begin().unwrap();
    db.query(&t1, "INSERT INTO t VALUES (7, 'one')").unwrap();
    db.query(&t2, "INSERT INTO t VALUES (7, 'two')").unwrap();
    db.commit(t1).unwrap();
    match db.commit(t2) {
        Err(e @ DbError::DuplicateKey(_)) => assert_eq!(e.sqlstate(), "23505"),
        other => panic!("expected DuplicateKey, got {other:?}"),
    }
    assert_eq!(db_rows(&db, "SELECT id, v FROM t"), table(&[&["7", "one"]]));

    // The commit recheck enforces UNIQUE with the same function.
    let t1 = db.begin().unwrap();
    let t2 = db.begin().unwrap();
    db.query(&t1, "INSERT INTO u VALUES (1, 5)").unwrap();
    db.query(&t2, "INSERT INTO u VALUES (2, 5)").unwrap();
    db.commit(t1).unwrap();
    assert_unique(&db.commit(t2).unwrap_err(), "u_n_key");
    assert_eq!(db_rows(&db, "SELECT id FROM u"), table(&[&["1"]]));
}
