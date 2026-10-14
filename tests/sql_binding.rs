//! Static expression binding (0.6.0 task 7a, release gate G2): every
//! expression is checked for type-category compatibility before any row is
//! evaluated, so a mixed BOOLEAN/numeric/character expression is rejected with
//! TypeMismatch (42804) whatever the data is — on empty tables, when a branch
//! short-circuits, or when an operand is NULL. Valid lazy evaluation
//! (`COALESCE(7, 1/0)`) keeps working.
mod common;

use aerodb::db::Database;
use aerodb::engine::Engine;
use aerodb::sql::value::Value;
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
    mismatch(
        &mut e,
        "SELECT CASE WHEN 1 / v = 0 THEN 1 ELSE TRUE END FROM t",
    );
    // A short-circuiting branch that is never taken still binds.
    mismatch(&mut e, "SELECT CASE WHEN TRUE THEN v ELSE b END FROM t");
    mismatch(&mut e, "SELECT COALESCE(v, s) FROM t WHERE id = 2");
    mismatch(&mut e, "SELECT id FROM t WHERE id = 1 OR v = s");
    // Same queries over rows and an empty filter result.
    mismatch(&mut e, "SELECT CASE WHEN id > 0 THEN v ELSE s END FROM t");
    mismatch(
        &mut e,
        "SELECT CASE WHEN id > 0 THEN v ELSE s END FROM t WHERE id > 100",
    );
}

#[test]
fn views_derived_tables_and_correlated_subqueries_are_bound() {
    let (_dir, mut e) = engine();
    typed_table(&mut e);
    exec(&mut e, "CREATE VIEW vw AS SELECT v, s, b FROM t");
    mismatch(&mut e, "SELECT CASE WHEN TRUE THEN v ELSE s END FROM vw");
    mismatch(&mut e, "SELECT id FROM t WHERE v IN (SELECT b FROM vw)");
    mismatch(
        &mut e,
        "SELECT CASE WHEN TRUE THEN q.v ELSE q.s END FROM (SELECT v, s FROM t) AS q",
    );
    mismatch(&mut e, "SELECT q.n + 1 FROM (SELECT b AS n FROM t) AS q");
    mismatch(
        &mut e,
        "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t AS u WHERE u.v = t.s)",
    );
    mismatch(
        &mut e,
        "SELECT (SELECT COALESCE(u.v, t.b) FROM t AS u) FROM t",
    );
    // A view whose query mixes categories cannot be created.
    mismatch(
        &mut e,
        "CREATE VIEW bad AS SELECT COALESCE(v, b) AS z FROM t",
    );
    assert_eq!(err(&mut e, "SELECT z FROM bad").sqlstate(), "42P01");
}

#[test]
fn dml_where_set_and_values_are_bound() {
    let (_dir, mut e) = engine();
    typed_table(&mut e);
    mismatch(&mut e, "UPDATE t SET v = 1 WHERE s");
    mismatch(
        &mut e,
        "UPDATE t SET v = CASE WHEN TRUE THEN 1 ELSE 'a' END",
    );
    mismatch(&mut e, "DELETE FROM t WHERE v LIKE 'a'");
    mismatch(
        &mut e,
        "INSERT INTO t (id, v) VALUES (1, COALESCE(1, TRUE))",
    );
    mismatch(&mut e, "INSERT INTO t (id, v) VALUES (1, 1), (2, 1 + TRUE)");
    assert!(q(&mut e, "SELECT id FROM t").is_empty());
    fill(&mut e);
    mismatch(
        &mut e,
        "UPDATE t SET v = CASE WHEN id > 100 THEN TRUE ELSE 1 END",
    );
    mismatch(&mut e, "DELETE FROM t WHERE id = 1 AND b = 1");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t"), table(&[&["3"]]));
    assert_eq!(q(&mut e, "SELECT v FROM t WHERE id = 2"), table(&[&["5"]]));
}

#[test]
fn database_api_binds_too() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("b.db").to_str().unwrap()).unwrap();
    let tx = db.begin().unwrap();
    db.query(
        &tx,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, b BOOLEAN)",
    )
    .unwrap();
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
    mismatch(
        &mut e,
        "CREATE TABLE c1 (n INTEGER CHECK (CASE WHEN n > 0 THEN 1 ELSE FALSE END))",
    );
    assert_eq!(err(&mut e, "SELECT n FROM c1").sqlstate(), "42P01");
    // Boolean forms are accepted.
    exec(
        &mut e,
        "CREATE TABLE c1 (n INTEGER CHECK (n > 0 OR n IS NULL), f BOOLEAN CHECK (f), \
         s TEXT CHECK (s LIKE 'a%'), m INTEGER CHECK (m IN (1, 2)), k INTEGER CHECK (k BETWEEN 1 AND 5), \
         CHECK (TRUE), CHECK (NULL), CHECK (NOT (n = 7)), CHECK (f IS NOT FALSE))",
    );
    exec(&mut e, "INSERT INTO c1 VALUES (1, TRUE, 'ab', 1, 2)");
    assert_eq!(
        err(&mut e, "INSERT INTO c1 VALUES (1, FALSE, 'ab', 1, 2)").sqlstate(),
        "23514"
    );
    // ALTER TABLE ... ADD COLUMN ... CHECK is bound the same way.
    mismatch(&mut e, "ALTER TABLE c1 ADD COLUMN z INTEGER CHECK (z)");
    exec(&mut e, "ALTER TABLE c1 ADD COLUMN z INTEGER CHECK (z <> 3)");
}

#[test]
fn default_expressions_are_bound() {
    let (_dir, mut e) = engine();
    mismatch(
        &mut e,
        "CREATE TABLE d (n INTEGER DEFAULT COALESCE(1, TRUE))",
    );
    assert_eq!(err(&mut e, "SELECT n FROM d").sqlstate(), "42P01");
    exec(
        &mut e,
        "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER DEFAULT COALESCE(NULL, 2))",
    );
    mismatch(
        &mut e,
        "ALTER TABLE d ADD COLUMN m INTEGER DEFAULT 1 + TRUE",
    );
    exec(&mut e, "INSERT INTO d (id) VALUES (1)");
    assert_eq!(q(&mut e, "SELECT n FROM d"), table(&[&["2"]]));
}

// ---- valid expressions keep working ----------------------------------------------

#[test]
fn valid_lazy_evaluation_still_works() {
    let (_dir, mut e) = engine();
    assert_eq!(q(&mut e, "SELECT COALESCE(7, 1/0)"), table(&[&["7"]]));
    assert_eq!(
        q(&mut e, "SELECT CASE WHEN TRUE THEN 8 ELSE 1/0 END"),
        table(&[&["8"]])
    );
    assert_eq!(q(&mut e, "SELECT FALSE AND 1/0 = 1"), table(&[&["false"]]));
    assert_eq!(q(&mut e, "SELECT TRUE OR 1/0 = 1"), table(&[&["true"]]));
    assert_eq!(
        err(&mut e, "SELECT COALESCE(NULL, 1/0)").sqlstate(),
        "22012"
    );
}

#[test]
fn compatible_categories_are_accepted() {
    let (_dir, mut e) = engine();
    typed_table(&mut e);
    fill(&mut e);
    assert_eq!(
        q(&mut e, "SELECT 1 = 1.0, 2.5 > 2, CAST(1.5 AS DOUBLE) < 2"),
        table(&[&["true", "true", "true"]])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT COALESCE(NULL, 1), CASE WHEN TRUE THEN 1 ELSE 2.5 END"
        ),
        table(&[&["1", "1"]])
    );
    assert_eq!(
        q(&mut e, "SELECT CASE WHEN FALSE THEN NULL ELSE 'x' END"),
        table(&[&["x"]])
    );
    assert_eq!(q(&mut e, "SELECT 'a' || 1"), table(&[&["a1"]]));
    assert_eq!(
        q(&mut e, "SELECT id FROM t WHERE c = 'ab'"),
        table(&[&["1"]])
    );
    assert_eq!(
        q(&mut e, "SELECT id FROM t WHERE c IN ('cd', s)"),
        table(&[&["2"]])
    );
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE x > v"), table(&[&["1"]]));
    assert_eq!(
        q(&mut e, "SELECT id FROM t WHERE x BETWEEN 1 AND 2.0"),
        table(&[&["1"]])
    );
    assert_eq!(
        q(&mut e, "SELECT COALESCE(x, v) FROM t WHERE id = 2"),
        table(&[&["2.25"]])
    );
    // DATE column against a character literal: implicit literal coercion.
    assert_eq!(
        q(&mut e, "SELECT id FROM t WHERE d = '2024-01-02'"),
        table(&[&["1"]])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT id FROM t WHERE d BETWEEN '2024-01-01' AND '2024-02-01'"
        ),
        table(&[&["1"]])
    );
    assert_eq!(
        q(&mut e, "SELECT id FROM t WHERE d IN ('2024-03-04')"),
        table(&[&["2"]])
    );
    assert_eq!(
        q(&mut e, "SELECT COUNT(*) FROM t WHERE b"),
        table(&[&["1"]])
    );
    assert_eq!(
        q(&mut e, "SELECT id FROM t WHERE s LIKE 'a%' AND c LIKE 'a%'"),
        table(&[&["1"]])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT id FROM t WHERE v IN (SELECT v FROM t WHERE v > 1)"
        ),
        table(&[&["2"]])
    );
    assert_eq!(
        q(&mut e, "SELECT CAST(v AS TEXT) || s FROM t WHERE id = 2"),
        table(&[&["5b"]])
    );
    assert_eq!(
        q(&mut e, "SELECT MIN(s), MAX(d), SUM(x), AVG(v) FROM t"),
        table(&[&["a", "2024-03-04", "3.75", "2.5"]])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT v FROM t WHERE id = 2 UNION SELECT x FROM t WHERE id = 1"
        ),
        table(&[&["5"], &["1.50"]])
    );
    exec(&mut e, "UPDATE t SET v = v + 1 WHERE b IS TRUE");
    exec(&mut e, "DELETE FROM t WHERE s IS NULL");
    assert_eq!(
        q(&mut e, "SELECT id, v FROM t ORDER BY id"),
        table(&[&["1", "1"], &["2", "5"]])
    );
}

// ---- fix round 1: character literals next to date/time kinds -----------------------

/// `sql` must fail with an invalid-datetime / invalid-cast class (22007 / 22018).
fn bad_literal(e: &mut Engine, sql: &str) {
    let v = err(e, sql);
    assert!(
        matches!(v.sqlstate(), "22007" | "22018"),
        "`{sql}`: expected 22007/22018, got {v:?}"
    );
}

fn dates(e: &mut Engine, sql: &str) -> Vec<Vec<Value>> {
    e.query(sql)
        .unwrap_or_else(|m| panic!("`{sql}` failed: {m}"))
        .rows
}

#[test]
fn character_literal_next_to_a_date_is_converted_at_bind_time() {
    let (_dir, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE dt (id INTEGER PRIMARY KEY, d DATE, s TEXT, ts TIMESTAMP)",
    );
    // An invalid literal is rejected at bind time, on an empty table too.
    for sql in [
        "SELECT COALESCE(d, 'never') FROM dt",
        "SELECT CASE WHEN id > 0 THEN 'abc' ELSE d END FROM dt",
        "SELECT id FROM (SELECT id, COALESCE(d, 'zzz') AS z FROM dt) q ORDER BY z",
        "SELECT COALESCE(d, 'never') FROM dt UNION SELECT d FROM dt",
        "SELECT d FROM dt UNION SELECT 'never'",
        "SELECT id FROM dt WHERE d = 'never'",
        "SELECT id FROM dt WHERE d IN ('2024-01-01', 'never')",
        "SELECT COALESCE(ts, 'x') FROM dt",
    ] {
        bad_literal(&mut e, sql);
    }
    // A non-literal character expression never mixes with a date kind.
    for sql in [
        "SELECT COALESCE(d, s) FROM dt",
        "SELECT CASE WHEN id > 0 THEN s ELSE d END FROM dt",
        "SELECT d FROM dt UNION SELECT s FROM dt",
        "SELECT COALESCE(d, UPPER('2024-01-01')) FROM dt",
    ] {
        mismatch(&mut e, sql);
    }
    exec(
        &mut e,
        "INSERT INTO dt VALUES (1, '2024-01-02', 'x', NULL), (2, NULL, 'y', NULL)",
    );
    bad_literal(&mut e, "SELECT COALESCE(d, 'never') FROM dt");
    // A valid literal takes the date kind: every value of the column is a DATE.
    let r = dates(
        &mut e,
        "SELECT id, COALESCE(d, '2024-01-01') FROM dt ORDER BY id",
    );
    assert_eq!(r.len(), 2);
    assert!(
        r.iter().all(|row| matches!(row[1], Value::Date(_))),
        "{r:?}"
    );
    let r = dates(
        &mut e,
        "SELECT CASE WHEN id = 1 THEN d ELSE '2023-05-06' END FROM dt ORDER BY id",
    );
    assert!(
        r.iter().all(|row| matches!(row[0], Value::Date(_))),
        "{r:?}"
    );
    // A converted derived column compares as a DATE.
    assert_eq!(
        q(
            &mut e,
            "SELECT id FROM (SELECT id, COALESCE(d, '2023-12-31') AS z FROM dt) q WHERE z > '2024-01-01'"
        ),
        table(&[&["1"]])
    );
    // Set operations use the same rule as CASE / COALESCE.
    let r = dates(
        &mut e,
        "SELECT d FROM dt WHERE id = 1 UNION SELECT '2024-01-01'",
    );
    assert_eq!(r.len(), 2);
    assert!(
        r.iter().all(|row| matches!(row[0], Value::Date(_))),
        "{r:?}"
    );
    let r = dates(
        &mut e,
        "SELECT COALESCE(d, '2024-01-02') FROM dt UNION SELECT d FROM dt WHERE id = 1",
    );
    assert_eq!(r, vec![vec![Value::Date(19724)]]);
    let r = dates(
        &mut e,
        "SELECT '2024-01-02' UNION SELECT d FROM dt WHERE id = 1",
    );
    assert_eq!(r, vec![vec![Value::Date(19724)]]);
    // Without a date kind a character literal stays character.
    assert_eq!(
        dates(&mut e, "SELECT COALESCE(NULL, 'never')"),
        vec![vec![Value::Text("never".into())]]
    );
    assert_eq!(
        q(&mut e, "SELECT 'a' UNION SELECT 'b' ORDER BY 1"),
        table(&[&["a"], &["b"]])
    );
    // A view keeps the conversion.
    exec(
        &mut e,
        "CREATE VIEW dv AS SELECT id, COALESCE(d, '2024-01-01') AS z FROM dt",
    );
    let r = dates(&mut e, "SELECT z FROM dv ORDER BY id");
    assert!(
        r.iter().all(|row| matches!(row[0], Value::Date(_))),
        "{r:?}"
    );
    // DML: the literal is converted before any row is touched.
    bad_literal(
        &mut e,
        "UPDATE dt SET s = 'z' WHERE COALESCE(d, 'never') > '2024-01-01'",
    );
    exec(&mut e, "UPDATE dt SET d = COALESCE(d, '2020-02-02')");
    assert_eq!(
        q(&mut e, "SELECT d FROM dt ORDER BY id"),
        table(&[&["2024-01-02"], &["2020-02-02"]])
    );
}

// ---- fix round 1: expression identity keeps the literal's representation ------------

#[test]
fn decimal_literal_scale_is_part_of_expression_identity() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE dx (x DECIMAL(5,2))");
    exec(&mut e, "INSERT INTO dx VALUES (1.25), (2.50)");
    // Two aggregates differing only in a literal's scale are distinct.
    assert_eq!(
        q(&mut e, "SELECT SUM(x * 1.0), SUM(x * 1.00) FROM dx"),
        table(&[&["3.750", "3.7500"]])
    );
    // GROUP BY x + 1.00 does not cover the select-list expression x + 1.0.
    assert_eq!(
        err(&mut e, "SELECT x + 1.0 FROM dx GROUP BY x + 1.00").sqlstate(),
        "42803"
    );
    assert_eq!(
        q(&mut e, "SELECT x + 1.0 FROM dx GROUP BY x + 1.0 ORDER BY 1"),
        table(&[&["2.25"], &["3.50"]])
    );
    // SQL value equality stays numeric.
    assert_eq!(q(&mut e, "SELECT 1.0 = 1.00"), table(&[&["true"]]));
    assert_eq!(q(&mut e, "SELECT 1.0 UNION SELECT 1.00").len(), 1);
    exec(&mut e, "INSERT INTO dx VALUES (1.250)");
    assert_eq!(
        q(&mut e, "SELECT COUNT(DISTINCT x) FROM dx"),
        table(&[&["2"]])
    );
    assert_eq!(
        q(&mut e, "SELECT x, COUNT(*) FROM dx GROUP BY x ORDER BY x"),
        table(&[&["1.25", "2"], &["2.50", "1"]])
    );
}

// ---- fix round 1: INSERT / UPDATE assignments are category-checked ------------------

#[test]
fn assignments_must_match_the_column_category() {
    let (_dir, mut e) = engine();
    typed_table(&mut e);
    // Empty table: still rejected.
    for sql in [
        "UPDATE t SET d = s",
        "UPDATE t SET v = b",
        "UPDATE t SET b = v",
        "UPDATE t SET d = v",
        "UPDATE t SET v = d",
        "UPDATE t SET x = CASE WHEN id > 0 THEN b END",
        "INSERT INTO t (id, v) VALUES (1, TRUE)",
        "INSERT INTO t (id, d) VALUES (1, 5)",
        "INSERT INTO t (id, d) VALUES (1, UPPER('2024-01-02'))",
        "INSERT INTO t (id, b) VALUES (1, 2.5)",
        "INSERT INTO t VALUES (1, 0, 'a', 7, NULL, NULL, NULL)",
        "INSERT INTO t (id, v) VALUES (1, 1), (2, FALSE)",
    ] {
        mismatch(&mut e, sql);
    }
    assert!(q(&mut e, "SELECT id FROM t").is_empty());
    fill(&mut e);
    mismatch(&mut e, "UPDATE t SET d = s");
    mismatch(&mut e, "UPDATE t SET v = b WHERE id = 3");
    assert_eq!(
        q(&mut e, "SELECT v, d FROM t WHERE id = 1"),
        table(&[&["0", "2024-01-02"]])
    );
    // Compatible assignments, NULL / DEFAULT and the legacy literal coercions still work.
    exec(&mut e, "UPDATE t SET x = v, v = NULL, c = s WHERE id = 2");
    assert_eq!(
        q(&mut e, "SELECT x, v, c FROM t WHERE id = 2"),
        table(&[&["5.00", "NULL", "b  "]])
    );
    exec(
        &mut e,
        "INSERT INTO t (id, v, x, d) VALUES (4, '42', '3.335', '2024-05-06')",
    );
    assert_eq!(
        q(&mut e, "SELECT v, x, d FROM t WHERE id = 4"),
        table(&[&["42", "3.34", "2024-05-06"]])
    );
    assert_eq!(
        err(&mut e, "INSERT INTO t (id, v) VALUES (5, 'x')").sqlstate(),
        "22018"
    );
    bad_literal(&mut e, "INSERT INTO t (id, d) VALUES (5, 'never')");
}

// ---- fix round 2: the assignment rule ------------------------------------------------

#[test]
fn assignment_accepts_literals_year_and_anything_into_character() {
    let (_dir, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE a (id INTEGER PRIMARY KEY, y YEAR, b BOOLEAN, s TEXT, c CHAR(4), vc VARCHAR(10), \
         n INTEGER, x DECIMAL(5,2), d DATE)",
    );
    exec(&mut e, "INSERT INTO a (id, y) VALUES (1, 2024)");
    exec(&mut e, "INSERT INTO a (id, b) VALUES (2, 'true')");
    exec(&mut e, "INSERT INTO a (id, s) VALUES (3, 5)");
    exec(&mut e, "INSERT INTO a (id, c) VALUES (4, 12)");
    exec(&mut e, "INSERT INTO a (id, vc) VALUES (5, 1.5)");
    exec(
        &mut e,
        "INSERT INTO a (id, n, x, d) VALUES (6, '7', '1.25', '2024-02-03')",
    );
    assert_eq!(
        q(&mut e, "SELECT y FROM a WHERE id = 1"),
        table(&[&["2024"]])
    );
    assert_eq!(
        q(&mut e, "SELECT b FROM a WHERE id = 2"),
        table(&[&["true"]])
    );
    assert_eq!(q(&mut e, "SELECT s FROM a WHERE id = 3"), table(&[&["5"]]));
    assert_eq!(
        q(&mut e, "SELECT c FROM a WHERE id = 4"),
        table(&[&["12  "]])
    );
    assert_eq!(
        q(&mut e, "SELECT vc FROM a WHERE id = 5"),
        table(&[&["1.5"]])
    );
    assert_eq!(
        q(&mut e, "SELECT n, x, d FROM a WHERE id = 6"),
        table(&[&["7", "1.25", "2024-02-03"]])
    );
    exec(&mut e, "UPDATE a SET b = 'false' WHERE id = 2");
    assert_eq!(
        q(&mut e, "SELECT b FROM a WHERE id = 2"),
        table(&[&["false"]])
    );
    exec(&mut e, "UPDATE a SET s = d WHERE id = 6");
    assert_eq!(
        q(&mut e, "SELECT s FROM a WHERE id = 6"),
        table(&[&["2024-02-03"]])
    );
    exec(
        &mut e,
        "UPDATE a SET s = b, vc = n, y = n + 2000 WHERE id = 6",
    );
    assert_eq!(
        q(&mut e, "SELECT y, s, vc FROM a WHERE id = 6"),
        table(&[&["2007", "NULL", "7"]])
    );
    exec(&mut e, "UPDATE a SET y = '1999' WHERE id = 1");
    assert_eq!(
        q(&mut e, "SELECT y FROM a WHERE id = 1"),
        table(&[&["1999"]])
    );
    // Length rules still apply at runtime.
    assert_eq!(
        err(&mut e, "INSERT INTO a (id, c) VALUES (7, 12345)").sqlstate(),
        "22001"
    );
}

#[test]
fn assignment_rejects_cross_category_values_and_invalid_literals_on_empty_table() {
    let (_dir, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE a (id INTEGER PRIMARY KEY, y YEAR, b BOOLEAN, s TEXT, n INTEGER, d DATE, tm TIME)",
    );
    for sql in [
        "UPDATE a SET d = s",
        "UPDATE a SET n = s",
        "UPDATE a SET b = s",
        "UPDATE a SET n = UPPER('1')",
        "INSERT INTO a (id, d) VALUES (1, 'a' || 'b')",
        "INSERT INTO a (id, b) VALUES (1, 'a' || 'b')",
        "INSERT INTO a (id, n) VALUES (1, TRUE)",
        "INSERT INTO a (id, d) VALUES (1, TRUE)",
        "INSERT INTO a (id, b) VALUES (1, 1)",
        "UPDATE a SET n = d",
        "UPDATE a SET n = tm",
        "UPDATE a SET y = 1.5",
        "UPDATE a SET y = b",
    ] {
        mismatch(&mut e, sql);
    }
    for sql in [
        "INSERT INTO a (id, n) VALUES (1, 'abc')",
        "UPDATE a SET b = 'maybe'",
        "INSERT INTO a (id, y) VALUES (1, 'zzz')",
    ] {
        let v = err(&mut e, sql);
        assert!(v.sqlstate().starts_with("22"), "`{sql}`: {v:?}");
    }
    bad_literal(&mut e, "INSERT INTO a (id, d) VALUES (1, 'never')");
    // NULL and DEFAULT are always allowed.
    exec(
        &mut e,
        "INSERT INTO a (id, n, b, d) VALUES (1, NULL, DEFAULT, NULL)",
    );
    exec(&mut e, "UPDATE a SET y = NULL, n = DEFAULT");
}
