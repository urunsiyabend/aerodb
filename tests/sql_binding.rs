//! Static expression binding (0.6.0 task 7a, release gate G2): every
//! expression is checked for type-category compatibility before any row is
//! evaluated, so a mixed BOOLEAN/numeric/character expression is rejected with
//! TypeMismatch (42804) whatever the data is — on empty tables, when a branch
//! short-circuits, or when an operand is NULL. Valid lazy evaluation
//! (`COALESCE(7, 1/0)`) keeps working.
mod common;

use aerodb::db::Database;
use aerodb::engine::Engine;
use common::{engine, err, exec, rows, table};

fn q(e: &mut Engine, sql: &str) -> Vec<Vec<String>> {
    rows(e, sql).unwrap_or_else(|m| panic!("`{sql}` failed: {m}"))
}

/// `sql` must fail with TypeMismatch (42804).
fn mismatch(e: &mut Engine, sql: &str) {
    let v = err(e, sql);
    assert_eq!(v.sqlstate(), "42804", "`{sql}`: expected 42804, got {v:?}");
}

/// t(id, v INTEGER, s TEXT, b BOOLEAN, d DATE, x DECIMAL(5,2), c CHAR(3)).
fn typed_table(e: &mut Engine) {
    exec(
        e,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, s TEXT, b BOOLEAN, d DATE, \
         x DECIMAL(5,2), c CHAR(3))",
    );
}

fn fill(e: &mut Engine) {
    exec(
        e,
        "INSERT INTO t VALUES (1, 0, 'a', TRUE, '2024-01-02', 1.50, 'ab'), \
         (2, 5, 'b', FALSE, '2024-03-04', 2.25, 'cd'), (3, NULL, NULL, NULL, NULL, NULL, NULL)",
    );
}

// ---- the supervisor's independent cases ----------------------------------------

#[test]
fn independent_binding_cases_error() {
    let (_dir, mut e) = engine();
    for sql in [
        "SELECT COALESCE(1, TRUE)",
        "SELECT CASE WHEN TRUE THEN 1 ELSE FALSE END",
        "SELECT 1 IN (1, TRUE)",
        "SELECT NULL LIKE TRUE",
    ] {
        mismatch(&mut e, sql);
    }
}

#[test]
fn null_operands_and_short_circuits_do_not_hide_a_mismatch() {
    let (_dir, mut e) = engine();
    for sql in [
        "SELECT NULL IN (1, TRUE)",
        "SELECT COALESCE(NULL, 1, TRUE)",
        "SELECT CASE WHEN FALSE THEN TRUE ELSE 1 END",
        "SELECT CASE WHEN TRUE THEN 1 WHEN FALSE THEN 'x' END",
        "SELECT CASE 1 WHEN TRUE THEN 1 END",
        "SELECT NULLIF(1, TRUE)",
        "SELECT 1 BETWEEN 0 AND TRUE",
        "SELECT FALSE AND 1",
        "SELECT TRUE OR 'x'",
        "SELECT NOT 1",
        "SELECT 1 + TRUE",
        "SELECT -'a'",
        "SELECT 'a' LIKE 1",
        "SELECT 1 = 'a'",
        "SELECT NULL + TRUE",
    ] {
        mismatch(&mut e, sql);
    }
}

// ---- column-derived types ------------------------------------------------------

#[test]
fn column_types_are_bound_even_on_an_empty_table() {
    let (_dir, mut e) = engine();
    typed_table(&mut e);
    for sql in [
        "SELECT CASE WHEN id > 0 THEN v ELSE s END FROM t",
        "SELECT id FROM t WHERE v IN (1, 'a')",
        "SELECT COALESCE(v, b) FROM t",
        "SELECT COALESCE(s, x) FROM t",
        "SELECT id FROM t WHERE s LIKE v",
        "SELECT id FROM t WHERE b LIKE 'a%'",
        "SELECT id FROM t WHERE v BETWEEN 1 AND s",
        "SELECT id FROM t WHERE v",
        "SELECT id FROM t WHERE b AND v",
        "SELECT v + s FROM t",
        "SELECT id FROM t WHERE v = s",
        "SELECT id FROM t WHERE b = 1",
        "SELECT id FROM t WHERE d = 5",
        "SELECT id FROM t WHERE v IN (SELECT s FROM t)",
        "SELECT NULLIF(v, s) FROM t",
        "SELECT t.id FROM t JOIN t AS u ON t.v",
        "SELECT id FROM t GROUP BY id HAVING COUNT(*)",
        "SELECT SUM(s) FROM t",
        "SELECT AVG(b) FROM t",
        "SELECT CASE v WHEN 'a' THEN 1 END FROM t",
        "SELECT CASE WHEN v THEN 1 END FROM t",
        "SELECT id FROM t WHERE (SELECT b FROM t AS u WHERE u.id = t.id) = 1",
        "SELECT UPPER(v) FROM t",
        "SELECT ABS(s) FROM t",
        "SELECT v & x FROM t",
        "SELECT id FROM t ORDER BY v + s",
        "SELECT v FROM t UNION SELECT b FROM t",
        "SELECT CAST(b AS DATE) FROM t",
        "SELECT CAST(d AS INTEGER) FROM t",
    ] {
        mismatch(&mut e, sql);
    }
}

#[test]
fn mismatch_is_reported_before_any_row_is_evaluated() {
    let (_dir, mut e) = engine();
    typed_table(&mut e);
    fill(&mut e);
    // Row 1 has v = 0: evaluating the WHEN would divide by zero (22012), but
    // the THEN/ELSE categories are incompatible, which binding reports first.
    mismatch(&mut e, "SELECT CASE WHEN 1 / v = 0 THEN 1 ELSE TRUE END FROM t");
    // A short-circuiting branch that is never taken still binds.
    mismatch(&mut e, "SELECT CASE WHEN TRUE THEN v ELSE b END FROM t");
    mismatch(&mut e, "SELECT COALESCE(v, s) FROM t WHERE id = 2");
    mismatch(&mut e, "SELECT id FROM t WHERE id = 1 OR v = s");
    // Same queries over rows and an empty filter result.
    mismatch(&mut e, "SELECT CASE WHEN id > 0 THEN v ELSE s END FROM t");
    mismatch(&mut e, "SELECT CASE WHEN id > 0 THEN v ELSE s END FROM t WHERE id > 100");
}

#[test]
fn views_derived_tables_and_correlated_subqueries_are_bound() {
    let (_dir, mut e) = engine();
    typed_table(&mut e);
    exec(&mut e, "CREATE VIEW vw AS SELECT v, s, b FROM t");
    mismatch(&mut e, "SELECT CASE WHEN TRUE THEN v ELSE s END FROM vw");
    mismatch(&mut e, "SELECT id FROM t WHERE v IN (SELECT b FROM vw)");
    mismatch(&mut e, "SELECT CASE WHEN TRUE THEN q.v ELSE q.s END FROM (SELECT v, s FROM t) AS q");
    mismatch(&mut e, "SELECT q.n + 1 FROM (SELECT b AS n FROM t) AS q");
    mismatch(&mut e, "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t AS u WHERE u.v = t.s)");
    mismatch(&mut e, "SELECT (SELECT COALESCE(u.v, t.b) FROM t AS u) FROM t");
    // A view whose query mixes categories cannot be created.
    mismatch(&mut e, "CREATE VIEW bad AS SELECT COALESCE(v, b) AS z FROM t");
    assert_eq!(err(&mut e, "SELECT z FROM bad").sqlstate(), "42P01");
}

#[test]
fn dml_where_set_and_values_are_bound() {
    let (_dir, mut e) = engine();
    typed_table(&mut e);
    mismatch(&mut e, "UPDATE t SET v = 1 WHERE s");
    mismatch(&mut e, "UPDATE t SET v = CASE WHEN TRUE THEN 1 ELSE 'a' END");
    mismatch(&mut e, "DELETE FROM t WHERE v LIKE 'a'");
    mismatch(&mut e, "INSERT INTO t (id, v) VALUES (1, COALESCE(1, TRUE))");
    mismatch(&mut e, "INSERT INTO t (id, v) VALUES (1, 1), (2, 1 + TRUE)");
    assert!(q(&mut e, "SELECT id FROM t").is_empty());
    fill(&mut e);
    mismatch(&mut e, "UPDATE t SET v = CASE WHEN id > 100 THEN TRUE ELSE 1 END");
    mismatch(&mut e, "DELETE FROM t WHERE id = 1 AND b = 1");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t"), table(&[&["3"]]));
    assert_eq!(q(&mut e, "SELECT v FROM t WHERE id = 2"), table(&[&["5"]]));
}

#[test]
fn database_api_binds_too() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("b.db").to_str().unwrap()).unwrap();
    let tx = db.begin().unwrap();
    db.query(&tx, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, b BOOLEAN)").unwrap();
    db.commit(tx).unwrap();
    let tx = db.begin().unwrap();
    let v = db.query(&tx, "SELECT COALESCE(v, b) FROM t").unwrap_err();
    assert_eq!(v.sqlstate(), "42804");
    db.abort(tx).unwrap();
}

// ---- CHECK constraints must be boolean -------------------------------------------

#[test]
fn check_expression_must_be_boolean() {
    let (_dir, mut e) = engine();
    mismatch(&mut e, "CREATE TABLE c1 (n INTEGER CHECK (n))");
    mismatch(&mut e, "CREATE TABLE c1 (n INTEGER, CHECK (n + 1))");
    mismatch(&mut e, "CREATE TABLE c1 (n INTEGER, s TEXT, CHECK (s))");
    mismatch(&mut e, "CREATE TABLE c1 (n INTEGER CHECK (n = 'a'))");
    mismatch(&mut e, "CREATE TABLE c1 (n INTEGER CHECK (CASE WHEN n > 0 THEN 1 ELSE FALSE END))");
    assert_eq!(err(&mut e, "SELECT n FROM c1").sqlstate(), "42P01");
    // Boolean forms are accepted.
    exec(
        &mut e,
        "CREATE TABLE c1 (n INTEGER CHECK (n > 0 OR n IS NULL), f BOOLEAN CHECK (f), \
         s TEXT CHECK (s LIKE 'a%'), m INTEGER CHECK (m IN (1, 2)), k INTEGER CHECK (k BETWEEN 1 AND 5), \
         CHECK (TRUE), CHECK (NULL), CHECK (NOT (n = 7)), CHECK (f IS NOT FALSE))",
    );
    exec(&mut e, "INSERT INTO c1 VALUES (1, TRUE, 'ab', 1, 2)");
    assert_eq!(err(&mut e, "INSERT INTO c1 VALUES (1, FALSE, 'ab', 1, 2)").sqlstate(), "23514");
    // ALTER TABLE ... ADD COLUMN ... CHECK is bound the same way.
    mismatch(&mut e, "ALTER TABLE c1 ADD COLUMN z INTEGER CHECK (z)");
    exec(&mut e, "ALTER TABLE c1 ADD COLUMN z INTEGER CHECK (z <> 3)");
}

#[test]
fn default_expressions_are_bound() {
    let (_dir, mut e) = engine();
    mismatch(&mut e, "CREATE TABLE d (n INTEGER DEFAULT COALESCE(1, TRUE))");
    assert_eq!(err(&mut e, "SELECT n FROM d").sqlstate(), "42P01");
    exec(&mut e, "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER DEFAULT COALESCE(NULL, 2))");
    mismatch(&mut e, "ALTER TABLE d ADD COLUMN m INTEGER DEFAULT 1 + TRUE");
    exec(&mut e, "INSERT INTO d (id) VALUES (1)");
    assert_eq!(q(&mut e, "SELECT n FROM d"), table(&[&["2"]]));
}

// ---- valid expressions keep working ----------------------------------------------

#[test]
fn valid_lazy_evaluation_still_works() {
    let (_dir, mut e) = engine();
    assert_eq!(q(&mut e, "SELECT COALESCE(7, 1/0)"), table(&[&["7"]]));
    assert_eq!(q(&mut e, "SELECT CASE WHEN TRUE THEN 8 ELSE 1/0 END"), table(&[&["8"]]));
    assert_eq!(q(&mut e, "SELECT FALSE AND 1/0 = 1"), table(&[&["false"]]));
    assert_eq!(q(&mut e, "SELECT TRUE OR 1/0 = 1"), table(&[&["true"]]));
    assert_eq!(err(&mut e, "SELECT COALESCE(NULL, 1/0)").sqlstate(), "22012");
}

#[test]
fn compatible_categories_are_accepted() {
    let (_dir, mut e) = engine();
    typed_table(&mut e);
    fill(&mut e);
    assert_eq!(q(&mut e, "SELECT 1 = 1.0, 2.5 > 2, CAST(1.5 AS DOUBLE) < 2"), table(&[&["true", "true", "true"]]));
    assert_eq!(q(&mut e, "SELECT COALESCE(NULL, 1), CASE WHEN TRUE THEN 1 ELSE 2.5 END"), table(&[&["1", "1"]]));
    assert_eq!(q(&mut e, "SELECT CASE WHEN FALSE THEN NULL ELSE 'x' END"), table(&[&["x"]]));
    assert_eq!(q(&mut e, "SELECT 'a' || 1"), table(&[&["a1"]]));
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE c = 'ab'"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE c IN ('cd', s)"), table(&[&["2"]]));
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE x > v"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE x BETWEEN 1 AND 2.0"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT COALESCE(x, v) FROM t WHERE id = 2"), table(&[&["2.25"]]));
    // DATE column against a character literal: implicit literal coercion.
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE d = '2024-01-02'"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE d BETWEEN '2024-01-01' AND '2024-02-01'"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE d IN ('2024-03-04')"), table(&[&["2"]]));
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t WHERE b"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE s LIKE 'a%' AND c LIKE 'a%'"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE v IN (SELECT v FROM t WHERE v > 1)"), table(&[&["2"]]));
    assert_eq!(q(&mut e, "SELECT CAST(v AS TEXT) || s FROM t WHERE id = 2"), table(&[&["5b"]]));
    assert_eq!(q(&mut e, "SELECT MIN(s), MAX(d), SUM(x), AVG(v) FROM t"), table(&[&["a", "2024-03-04", "3.75", "2.5"]]));
    assert_eq!(q(&mut e, "SELECT v FROM t WHERE id = 2 UNION SELECT x FROM t WHERE id = 1"), table(&[&["5"], &["1.50"]]));
    exec(&mut e, "UPDATE t SET v = v + 1 WHERE b IS TRUE");
    exec(&mut e, "DELETE FROM t WHERE s IS NULL");
    assert_eq!(q(&mut e, "SELECT id, v FROM t ORDER BY id"), table(&[&["1", "1"], &["2", "5"]]));
}
