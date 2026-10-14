//! End-to-end SQL semantics through the public Engine (audit `base` fixture).
mod common;

use aerodb::engine::Engine;
use common::{engine, err, exec, rows, sorted, table};
use tempfile::TempDir;

/// The audit `base` fixture plus the `u` table used by join/subquery probes.
fn base() -> (TempDir, Engine) {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, s TEXT)");
    exec(&mut e, "INSERT INTO t VALUES (1, 20, 'b'), (2, 10, 'a'), (3, NULL, 'c'), (4, 20, 'b')");
    exec(&mut e, "CREATE TABLE u (id INTEGER PRIMARY KEY, n TEXT)");
    exec(&mut e, "INSERT INTO u VALUES (2, 'two'), (5, 'five')");
    (dir, e)
}

fn q(e: &mut Engine, sql: &str) -> Vec<Vec<String>> {
    rows(e, sql).unwrap_or_else(|m| panic!("`{sql}` failed: {m}"))
}

fn q_set(e: &mut Engine, sql: &str) -> Vec<Vec<String>> {
    sorted(q(e, sql))
}

fn single(e: &mut Engine, sql: &str) -> String {
    let r = q(e, sql);
    assert_eq!(r.len(), 1, "`{sql}` returned {r:?}");
    assert_eq!(r[0].len(), 1, "`{sql}` returned {r:?}");
    r[0][0].clone()
}

fn state(e: &mut Engine, sql: &str) -> &'static str {
    err(e, sql).sqlstate()
}

// ---- NULL and three-valued logic -------------------------------------------

#[test]
fn null_eq() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE v = NULL"), table(&[]));
}

#[test]
fn null_neq() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE v <> NULL"), table(&[]));
}

#[test]
fn null_arithmetic() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT v + 1 FROM t WHERE id = 3"), table(&[&["NULL"]]));
}

#[test]
fn null_predicate() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE v IS NULL"), table(&[&["3"]]));
}

#[test]
fn three_value_or() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE v = NULL OR id = 2"), table(&[&["2"]]));
}

#[test]
fn three_value_and() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE v = NULL AND id = 3"), table(&[]));
}

fn truth(v: Option<bool>) -> &'static str {
    match v {
        Some(true) => "TRUE",
        Some(false) => "FALSE",
        None => "NULL",
    }
}

fn rendered(v: Option<bool>) -> &'static str {
    match v {
        Some(true) => "true",
        Some(false) => "false",
        None => "NULL",
    }
}

#[test]
fn truth_tables() {
    let (_d, mut e) = engine();
    let vals = [Some(true), Some(false), None];
    for p in vals {
        for r in vals {
            let and = match (p, r) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            };
            let or = match (p, r) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            };
            let sql = format!("SELECT {} AND {}", truth(p), truth(r));
            assert_eq!(single(&mut e, &sql), rendered(and), "{sql}");
            let sql = format!("SELECT {} OR {}", truth(p), truth(r));
            assert_eq!(single(&mut e, &sql), rendered(or), "{sql}");
        }
        let sql = format!("SELECT NOT {}", truth(p));
        assert_eq!(single(&mut e, &sql), rendered(p.map(|b| !b)), "{sql}");
    }
}

#[test]
fn null_comparisons_are_unknown() {
    let (_d, mut e) = engine();
    assert_eq!(single(&mut e, "SELECT NULL = NULL"), "NULL");
    assert_eq!(single(&mut e, "SELECT NULL <> 1"), "NULL");
}

#[test]
fn not_excludes_unknown_rows() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE NOT (v = 20)"), table(&[&["2"]]));
}

#[test]
fn in_list_with_null_member() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE v NOT IN (10, NULL)"), table(&[]));
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE v IN (10, NULL)"), table(&[&["2"]]));
}

#[test]
fn in_list() {
    let (_d, mut e) = base();
    assert_eq!(q_set(&mut e, "SELECT id FROM t WHERE id IN (1, 4)"), table(&[&["1"], &["4"]]));
}

#[test]
fn like() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE s LIKE 'a%'"), table(&[&["2"]]));
}

#[test]
fn like_underscore_and_escape() {
    let (_d, mut e) = base();
    assert_eq!(
        q_set(&mut e, "SELECT id FROM t WHERE s LIKE '_'"),
        table(&[&["1"], &["2"], &["3"], &["4"]])
    );
    assert_eq!(single(&mut e, "SELECT 'a%' LIKE 'a!%' ESCAPE '!'"), "true");
    assert_eq!(single(&mut e, "SELECT 'ab' LIKE 'a!%' ESCAPE '!'"), "false");
    assert_eq!(single(&mut e, "SELECT 'abc' LIKE 'a_c'"), "true");
    assert_eq!(single(&mut e, "SELECT NULL LIKE 'a%'"), "NULL");
}

#[test]
fn between_with_null() {
    let (_d, mut e) = base();
    assert_eq!(
        q_set(&mut e, "SELECT id FROM t WHERE v BETWEEN 10 AND 20"),
        table(&[&["1"], &["2"], &["4"]])
    );
    assert_eq!(single(&mut e, "SELECT NULL BETWEEN 1 AND 2"), "NULL");
    assert_eq!(single(&mut e, "SELECT 5 BETWEEN NULL AND 1"), "false");
    assert_eq!(single(&mut e, "SELECT 5 BETWEEN NULL AND 9"), "NULL");
}

// ---- DISTINCT / ORDER BY ----------------------------------------------------

#[test]
fn distinct() {
    let (_d, mut e) = base();
    assert_eq!(
        q_set(&mut e, "SELECT DISTINCT v FROM t"),
        sorted(table(&[&["20"], &["10"], &["NULL"]]))
    );
}

#[test]
fn order_desc() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id FROM t ORDER BY id DESC"),
        table(&[&["4"], &["3"], &["2"], &["1"]])
    );
}

#[test]
fn order_nonselected() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT s FROM t ORDER BY id DESC"),
        table(&[&["b"], &["c"], &["a"], &["b"]])
    );
}

#[test]
fn order_expr() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id FROM t ORDER BY 0 - id"),
        table(&[&["4"], &["3"], &["2"], &["1"]])
    );
}

#[test]
fn order_nulls() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT v FROM t ORDER BY v NULLS FIRST"),
        table(&[&["NULL"], &["10"], &["20"], &["20"]])
    );
    assert_eq!(
        q(&mut e, "SELECT v FROM t ORDER BY v"),
        table(&[&["10"], &["20"], &["20"], &["NULL"]])
    );
    assert_eq!(
        q(&mut e, "SELECT v FROM t ORDER BY v DESC"),
        table(&[&["NULL"], &["20"], &["20"], &["10"]])
    );
    assert_eq!(
        q(&mut e, "SELECT v FROM t ORDER BY v DESC NULLS LAST"),
        table(&[&["20"], &["20"], &["10"], &["NULL"]])
    );
}

#[test]
fn order_by_multiple_keys() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id FROM t ORDER BY s DESC, id"),
        table(&[&["3"], &["1"], &["4"], &["2"]])
    );
}

#[test]
fn order_by_alias() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id AS k FROM t ORDER BY k DESC"),
        table(&[&["4"], &["3"], &["2"], &["1"]])
    );
}

#[test]
fn order_by_ordinal() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT s, id FROM t ORDER BY 2 DESC"),
        table(&[&["b", "4"], &["c", "3"], &["a", "2"], &["b", "1"]])
    );
    assert_eq!(state(&mut e, "SELECT s FROM t ORDER BY 3"), "42601");
}

#[test]
fn order_by_on_join_with_aggregate() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT t.s, COUNT(*) FROM t JOIN u ON t.id = u.id GROUP BY t.s ORDER BY t.s"),
        table(&[&["a", "1"]])
    );
}

#[test]
fn distinct_order_by_must_use_select_list() {
    let (_d, mut e) = base();
    assert_eq!(state(&mut e, "SELECT DISTINCT s FROM t ORDER BY id"), "42601");
}

#[test]
fn limit_and_offset_apply_after_order() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id FROM t ORDER BY id DESC LIMIT 2 OFFSET 1"),
        table(&[&["3"], &["2"]])
    );
}

// ---- Aggregates -------------------------------------------------------------

#[test]
fn aggregate_count() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT COUNT(*), COUNT(v) FROM t"), table(&[&["4", "3"]]));
}

#[test]
fn aggregate_sum() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT SUM(v) FROM t"), table(&[&["50"]]));
}

#[test]
fn aggregate_avg() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT AVG(v) FROM t"), table(&[&["16.666666666666668"]]));
}

#[test]
fn aggregate_empty() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t WHERE id = 99"),
        table(&[&["0", "NULL", "NULL", "NULL", "NULL"]])
    );
}

#[test]
fn all_null_aggregate() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM t WHERE id = 3"),
        table(&[&["0", "NULL", "NULL", "NULL", "NULL"]])
    );
}

#[test]
fn aggregate_distinct() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT COUNT(DISTINCT v) FROM t"), table(&[&["2"]]));
}

#[test]
fn having() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT s, COUNT(*) FROM t GROUP BY s HAVING COUNT(*) > 1"),
        table(&[&["b", "2"]])
    );
}

#[test]
fn group_by_violation() {
    let (_d, mut e) = base();
    assert_eq!(state(&mut e, "SELECT id, COUNT(*) FROM t GROUP BY s"), "42803");
}

// ---- Subqueries -------------------------------------------------------------

#[test]
fn scalar_one() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT (SELECT v FROM t WHERE id = 1)"), table(&[&["20"]]));
}

#[test]
fn scalar_empty() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT (SELECT v FROM t WHERE id = 99)"), table(&[&["NULL"]]));
}

#[test]
fn scalar_cardinality() {
    let (_d, mut e) = base();
    assert_eq!(state(&mut e, "SELECT (SELECT v FROM t)"), "21000");
}

#[test]
fn correlated_exists() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id FROM t WHERE EXISTS (SELECT id FROM u WHERE u.id = t.id)"),
        table(&[&["2"]])
    );
}

#[test]
fn in_subquery() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT id FROM t WHERE id IN (SELECT id FROM u)"), table(&[&["2"]]));
}

#[test]
fn in_subquery_null() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id FROM t WHERE v IN (SELECT v FROM t WHERE id = 3)"),
        table(&[])
    );
}

#[test]
fn from_subquery_with_where_and_order() {
    let (_d, mut e) = base();
    assert_eq!(
        q(
            &mut e,
            "SELECT x.id FROM (SELECT id, v FROM t WHERE v IS NOT NULL) AS x WHERE x.v = 20 ORDER BY x.id DESC"
        ),
        table(&[&["4"], &["1"]])
    );
}

#[test]
fn correlated_scalar_subquery_in_projection() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id, (SELECT n FROM u WHERE u.id = t.id) FROM t ORDER BY id"),
        table(&[&["1", "NULL"], &["2", "two"], &["3", "NULL"], &["4", "NULL"]])
    );
}

// ---- Set operations ---------------------------------------------------------

#[test]
fn union() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id FROM t WHERE id = 1 UNION SELECT id FROM t WHERE id = 1"),
        table(&[&["1"]])
    );
}

#[test]
fn union_all() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id FROM t WHERE id = 1 UNION ALL SELECT id FROM t WHERE id = 1"),
        table(&[&["1"], &["1"]])
    );
}

#[test]
fn union_second_branch() {
    let (_d, mut e) = base();
    assert_eq!(
        q_set(&mut e, "SELECT id FROM t WHERE id = 1 UNION SELECT id FROM t WHERE id = 2"),
        table(&[&["1"], &["2"]])
    );
}

#[test]
fn union_duplicate_elimination() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT v FROM t WHERE id = 1 UNION SELECT v FROM t WHERE id = 4"),
        table(&[&["20"]])
    );
}

#[test]
fn except() {
    let (_d, mut e) = base();
    assert_eq!(
        q_set(&mut e, "SELECT id FROM t EXCEPT SELECT id FROM u"),
        table(&[&["1"], &["3"], &["4"]])
    );
}

#[test]
fn intersect() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT id FROM t INTERSECT SELECT id FROM u"), table(&[&["2"]]));
}

#[test]
fn except_all_and_intersect_all_use_multiplicities() {
    let (_d, mut e) = base();
    assert_eq!(
        q_set(&mut e, "SELECT v FROM t EXCEPT ALL SELECT v FROM t WHERE id = 1"),
        sorted(table(&[&["20"], &["10"], &["NULL"]]))
    );
    assert_eq!(
        q_set(&mut e, "SELECT v FROM t EXCEPT SELECT v FROM t WHERE id = 1"),
        sorted(table(&[&["10"], &["NULL"]]))
    );
    assert_eq!(
        q(&mut e, "SELECT s FROM t INTERSECT ALL SELECT s FROM t WHERE id = 1"),
        table(&[&["b"]])
    );
    assert_eq!(
        q(&mut e, "SELECT v FROM t INTERSECT ALL SELECT v FROM t WHERE v = 20"),
        table(&[&["20"], &["20"]])
    );
}

#[test]
fn union_arity_mismatch() {
    let (_d, mut e) = base();
    assert_eq!(state(&mut e, "SELECT id FROM t UNION SELECT id, v FROM t"), "42601");
}

#[test]
fn set_operation_order_by_applies_to_whole_query() {
    let (_d, mut e) = base();
    assert_eq!(
        q(&mut e, "SELECT id FROM t UNION SELECT id FROM u ORDER BY id DESC"),
        table(&[&["5"], &["4"], &["3"], &["2"], &["1"]])
    );
}

// ---- Joins ------------------------------------------------------------------

#[test]
fn inner_join() {
    let (_d, mut e) = base();
    assert_eq!(q(&mut e, "SELECT t.id, u.n FROM t JOIN u ON t.id = u.id"), table(&[&["2", "two"]]));
}

#[test]
fn left_join() {
    let (_d, mut e) = base();
    assert_eq!(
        q_set(&mut e, "SELECT t.id, u.n FROM t LEFT JOIN u ON t.id = u.id"),
        sorted(table(&[&["1", "NULL"], &["2", "two"], &["3", "NULL"], &["4", "NULL"]]))
    );
}

#[test]
fn right_join() {
    let (_d, mut e) = base();
    assert_eq!(
        q_set(&mut e, "SELECT t.id, u.n FROM t RIGHT JOIN u ON t.id = u.id"),
        sorted(table(&[&["2", "two"], &["NULL", "five"]]))
    );
}

#[test]
fn full_join() {
    let (_d, mut e) = base();
    assert_eq!(
        q_set(&mut e, "SELECT t.id, u.n FROM t FULL JOIN u ON t.id = u.id"),
        sorted(table(&[
            &["1", "NULL"],
            &["2", "two"],
            &["3", "NULL"],
            &["4", "NULL"],
            &["NULL", "five"]
        ]))
    );
}

#[test]
fn ambiguous_column_in_join() {
    let (_d, mut e) = base();
    assert_eq!(state(&mut e, "SELECT id FROM t JOIN u ON t.id = u.id"), "42702");
}

// ---- Scalar expressions -----------------------------------------------------

#[test]
fn cast() {
    let (_d, mut e) = engine();
    assert_eq!(single(&mut e, "SELECT CAST('12' AS INTEGER)"), "12");
    assert_eq!(state(&mut e, "SELECT CAST('abc' AS INTEGER)"), "22018");
}

#[test]
fn case() {
    let (_d, mut e) = engine();
    assert_eq!(single(&mut e, "SELECT CASE WHEN 1 = 1 THEN 7 ELSE 9 END"), "7");
    assert_eq!(single(&mut e, "SELECT CASE WHEN 1 = 2 THEN 7 END"), "NULL");
    assert_eq!(single(&mut e, "SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END"), "two");
}

#[test]
fn coalesce() {
    let (_d, mut e) = engine();
    assert_eq!(single(&mut e, "SELECT COALESCE(NULL, 7)"), "7");
    assert_eq!(single(&mut e, "SELECT NULLIF(3, 3)"), "NULL");
    assert_eq!(single(&mut e, "SELECT NULLIF(3, 4)"), "3");
}

#[test]
fn inline_comment() {
    let (_d, mut e) = engine();
    assert_eq!(single(&mut e, "SELECT 1 -- audit comment"), "1");
}

#[test]
fn divide_zero() {
    let (_d, mut e) = engine();
    assert_eq!(state(&mut e, "SELECT 1 / 0"), "22012");
}

#[test]
fn type_mismatch_is_an_error() {
    let (_d, mut e) = engine();
    assert_eq!(state(&mut e, "SELECT 1 + 'a'"), "42804");
}

#[test]
fn exact_numeric_arithmetic() {
    let (_d, mut e) = engine();
    assert_eq!(single(&mut e, "SELECT 7 / 2"), "3");
    assert_eq!(single(&mut e, "SELECT 2.5 * 2"), "5.0");
    assert_eq!(single(&mut e, "SELECT 0.1 + 0.2"), "0.3");
}

// ---- DML --------------------------------------------------------------------

#[test]
fn update_null() {
    let (_d, mut e) = base();
    exec(&mut e, "UPDATE t SET v = NULL WHERE id = 2");
    assert_eq!(q(&mut e, "SELECT v FROM t WHERE id = 2"), table(&[&["NULL"]]));
}

#[test]
fn update_expression() {
    let (_d, mut e) = base();
    exec(&mut e, "UPDATE t SET v = v + 1 WHERE id = 2");
    assert_eq!(q(&mut e, "SELECT v FROM t WHERE id = 2"), table(&[&["11"]]));
}

// ---- API / persistence ------------------------------------------------------

#[test]
fn default_expression_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("defaults.db");
    let path = path.to_str().unwrap();
    {
        let mut e = Engine::new(path);
        exec(
            &mut e,
            "CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER DEFAULT (1 + 2), s TEXT DEFAULT 'it''s')",
        );
    }
    let mut e = Engine::new(path);
    exec(&mut e, "INSERT INTO d (id) VALUES (1)");
    assert_eq!(q(&mut e, "SELECT n, s FROM d"), table(&[&["3", "it's"]]));
}

#[test]
fn database_query_returns_typed_rows() {
    use aerodb::sql::{decimal::Decimal, value::{SqlType, Value}};
    let dir = tempfile::tempdir().unwrap();
    let db = aerodb::db::Database::open(dir.path().join("q.db").to_str().unwrap()).unwrap();
    let tx = db.begin().unwrap();
    db.query(&tx, "CREATE TABLE t (id INTEGER PRIMARY KEY, v DECIMAL(5,2))").unwrap();
    db.query(&tx, "INSERT INTO t VALUES (1, 1.005)").unwrap();
    let r = db.query(&tx, "SELECT v, v * 2 FROM t").unwrap();
    db.commit(tx).unwrap();
    assert_eq!(r.columns[0].ty, SqlType::Decimal { precision: 5, scale: 2 });
    let dec = |s: &str| Value::Decimal(Decimal::parse(s).unwrap());
    assert_eq!(r.rows, vec![vec![dec("1.01"), dec("2.02")]]);
}

#[test]
fn delete_all() {
    let (_d, mut e) = base();
    exec(&mut e, "DELETE FROM t");
    assert_eq!(q(&mut e, "SELECT id FROM t"), table(&[]));
}
