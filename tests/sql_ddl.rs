//! DROP TABLE RESTRICT dependency checks, CREATE/DROP VIEW (executed through
//! the unified query executor), ALTER TABLE ADD COLUMN with defaults and
//! constraints, and catalog round trips of DECIMAL/VARCHAR column types
//! (0.6.0 task 6).
mod common;

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

/// Result column names of a query.
fn header(e: &mut Engine, sql: &str) -> Vec<String> {
    e.query(sql)
        .unwrap_or_else(|m| panic!("`{sql}` failed: {m}"))
        .columns
        .into_iter()
        .map(|c| c.name)
        .collect()
}

fn assert_dependent(e: &DbError) {
    assert!(
        matches!(e, DbError::DependentObjects(_)),
        "expected DependentObjects, got {e:?}"
    );
    assert_eq!(e.sqlstate(), "2BP01");
    assert!(e.to_string().contains("depend"), "{e}");
}

/// The audit table `t` with four rows.
fn audit_t(e: &mut Engine) {
    exec(
        e,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, s TEXT)",
    );
    exec(
        e,
        "INSERT INTO t VALUES (1, 20, 'b'), (2, 10, 'a'), (3, NULL, 'c'), (4, 20, 'b')",
    );
}

// ---- DROP TABLE RESTRICT --------------------------------------------------------

#[test]
fn drop_restrict_dependency() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    exec(
        &mut e,
        "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id))",
    );
    exec(&mut e, "INSERT INTO p VALUES (1)");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1)");
    let v = err(&mut e, "DROP TABLE p RESTRICT");
    assert_dependent(&v);
    assert_eq!(
        v.to_string(),
        "cannot drop p because other objects depend on it"
    );
    // Without RESTRICT the default behaviour is the same.
    assert_dependent(&err(&mut e, "DROP TABLE p"));
    assert_dependent(&err(&mut e, "DROP TABLE IF EXISTS p"));
    assert_eq!(q(&mut e, "SELECT id FROM p"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT id, pid FROM ch"), table(&[&["1", "1"]]));
    // The FK is still enforced.
    assert_eq!(state(&mut e, "INSERT INTO ch VALUES (2, 9)"), "23503");
}

#[test]
fn drop_child_then_parent_works() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    exec(
        &mut e,
        "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
    );
    exec(&mut e, "INSERT INTO p VALUES (1)");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1)");
    exec(&mut e, "DROP TABLE ch RESTRICT");
    exec(&mut e, "DROP TABLE p RESTRICT");
    assert_eq!(state(&mut e, "SELECT id FROM p"), "42P01");
    assert_eq!(state(&mut e, "SELECT id FROM ch"), "42P01");
    // The names can be reused.
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE ch (x TEXT)");
    exec(&mut e, "INSERT INTO ch VALUES ('a')");
    assert_eq!(q(&mut e, "SELECT x FROM ch"), table(&[&["a"]]));
}

#[test]
fn drop_cascade_is_unsupported() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    assert_eq!(state(&mut e, "DROP TABLE p CASCADE"), "0A000");
    exec(&mut e, "CREATE VIEW pv AS SELECT id FROM p");
    assert_eq!(state(&mut e, "DROP VIEW pv CASCADE"), "0A000");
}

#[test]
fn drop_table_referenced_by_view_is_rejected() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "CREATE VIEW tv AS SELECT id FROM t WHERE v = 20");
    assert_dependent(&err(&mut e, "DROP TABLE t"));
    assert_dependent(&err(&mut e, "DROP TABLE t RESTRICT"));
    assert_eq!(q_set(&mut e, "SELECT id FROM tv"), table(&[&["1"], &["4"]]));
    // A view referencing the table only inside a subquery also depends on it.
    exec(&mut e, "CREATE TABLE u (k INTEGER)");
    exec(
        &mut e,
        "CREATE VIEW uv AS SELECT 1 AS one WHERE EXISTS (SELECT k FROM u)",
    );
    assert_dependent(&err(&mut e, "DROP TABLE u"));
    exec(&mut e, "DROP VIEW uv");
    exec(&mut e, "DROP TABLE u");
    exec(&mut e, "DROP VIEW tv");
    exec(&mut e, "DROP TABLE t");
}

#[test]
fn self_referencing_fk_table_drops() {
    let (_dir, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE emp (id INTEGER PRIMARY KEY, boss INTEGER REFERENCES emp(id))",
    );
    exec(&mut e, "INSERT INTO emp VALUES (1, NULL), (2, 1)");
    exec(&mut e, "DROP TABLE emp RESTRICT");
    assert_eq!(state(&mut e, "SELECT id FROM emp"), "42P01");
}

#[test]
fn drop_table_in_transaction_rolls_back() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE d (id INTEGER PRIMARY KEY)");
    exec(&mut e, "INSERT INTO d VALUES (1)");
    exec(&mut e, "BEGIN");
    exec(&mut e, "DROP TABLE d");
    assert_eq!(state(&mut e, "SELECT id FROM d"), "42P01");
    exec(&mut e, "ROLLBACK");
    assert_eq!(q(&mut e, "SELECT id FROM d"), table(&[&["1"]]));
}

// ---- views ------------------------------------------------------------------------

#[test]
fn view_audit_probe() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "CREATE VIEW vv AS SELECT id FROM t WHERE id = 1");
    assert_eq!(q(&mut e, "SELECT id FROM vv"), table(&[&["1"]]));
}

#[test]
fn view_with_column_list() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(
        &mut e,
        "CREATE VIEW named (k, val) AS SELECT id, v FROM t WHERE v IS NOT NULL",
    );
    assert_eq!(header(&mut e, "SELECT * FROM named"), vec!["k", "val"]);
    assert_eq!(
        q_set(&mut e, "SELECT k, val FROM named"),
        table(&[&["1", "20"], &["2", "10"], &["4", "20"]])
    );
    // The underlying names are not exposed.
    assert_eq!(state(&mut e, "SELECT id FROM named"), "42703");
    // Qualified by the view name or an alias.
    assert_eq!(
        q(&mut e, "SELECT named.k FROM named WHERE named.val = 10"),
        table(&[&["2"]])
    );
    assert_eq!(
        q(&mut e, "SELECT n.k FROM named AS n WHERE n.val = 10"),
        table(&[&["2"]])
    );
    // Column list length must match the query arity.
    assert_eq!(
        state(&mut e, "CREATE VIEW bad (a, b, c) AS SELECT id, v FROM t"),
        "42601"
    );
    assert_eq!(
        state(&mut e, "CREATE VIEW bad (a) AS SELECT id, v FROM t"),
        "42601"
    );
    assert_eq!(state(&mut e, "SELECT * FROM bad"), "42P01");
}

#[test]
fn view_with_duplicate_output_names_needs_column_list() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    assert_eq!(
        state(&mut e, "CREATE VIEW dup AS SELECT id, id FROM t"),
        "42601"
    );
    exec(
        &mut e,
        "CREATE VIEW dup (a, b) AS SELECT id, id FROM t WHERE id = 2",
    );
    assert_eq!(q(&mut e, "SELECT a, b FROM dup"), table(&[&["2", "2"]]));
}

#[test]
fn view_must_bind_at_create() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    assert_eq!(
        state(&mut e, "CREATE VIEW bad AS SELECT id FROM missing"),
        "42P01"
    );
    assert_eq!(
        state(&mut e, "CREATE VIEW bad AS SELECT nope FROM t"),
        "42703"
    );
    assert_eq!(state(&mut e, "SELECT * FROM bad"), "42P01");
}

#[test]
fn view_with_joins_aggregates_and_order_by() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(
        &mut e,
        "CREATE TABLE u (id INTEGER PRIMARY KEY, tid INTEGER, w TEXT)",
    );
    exec(
        &mut e,
        "INSERT INTO u VALUES (10, 1, 'x'), (11, 1, 'y'), (12, 2, 'z')",
    );
    exec(
        &mut e,
        "CREATE VIEW agg AS SELECT t.s AS s, COUNT(*) AS n, SUM(t.v) AS total FROM t GROUP BY t.s ORDER BY s",
    );
    assert_eq!(
        q(&mut e, "SELECT s, n, total FROM agg"),
        table(&[&["a", "1", "10"], &["b", "2", "40"], &["c", "1", "NULL"]])
    );
    exec(
        &mut e,
        "CREATE VIEW joined AS SELECT t.id AS tid, u.w AS w FROM t JOIN u ON u.tid = t.id ORDER BY u.id DESC",
    );
    assert_eq!(
        q(&mut e, "SELECT tid, w FROM joined"),
        table(&[&["2", "z"], &["1", "y"], &["1", "x"]])
    );
    // Aggregates over a view.
    assert_eq!(
        q(&mut e, "SELECT COUNT(*) FROM joined WHERE tid = 1"),
        table(&[&["2"]])
    );
}

#[test]
fn view_joined_with_table() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(
        &mut e,
        "CREATE VIEW big AS SELECT id, v FROM t WHERE v >= 20",
    );
    assert_eq!(
        q_set(
            &mut e,
            "SELECT t.id, t.s, big.v FROM t JOIN big ON big.id = t.id"
        ),
        table(&[&["1", "b", "20"], &["4", "b", "20"]])
    );
    assert_eq!(
        q_set(
            &mut e,
            "SELECT b.id FROM t LEFT JOIN big AS b ON b.id = t.id WHERE t.id < 3"
        ),
        table(&[&["1"], &["NULL"]])
    );
    // In subqueries too.
    assert_eq!(
        q_set(&mut e, "SELECT id FROM t WHERE id IN (SELECT id FROM big)"),
        table(&[&["1"], &["4"]])
    );
    assert_eq!(
        q_set(&mut e, "SELECT x.id FROM (SELECT id FROM big) AS x"),
        table(&[&["1"], &["4"]])
    );
}

#[test]
fn view_over_view() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(
        &mut e,
        "CREATE VIEW v1 AS SELECT id, v, s FROM t WHERE v IS NOT NULL",
    );
    exec(
        &mut e,
        "CREATE VIEW v2 (ident, doubled) AS SELECT id, v * 2 FROM v1 WHERE s = 'b'",
    );
    assert_eq!(
        q_set(&mut e, "SELECT ident, doubled FROM v2"),
        table(&[&["1", "40"], &["4", "40"]])
    );
    // v1 is referenced by v2.
    assert_dependent(&err(&mut e, "DROP VIEW v1"));
    assert_dependent(&err(&mut e, "DROP VIEW v1 RESTRICT"));
    assert_dependent(&err(&mut e, "DROP TABLE t"));
    exec(&mut e, "DROP VIEW v2");
    exec(&mut e, "DROP VIEW v1");
    exec(&mut e, "DROP TABLE t");
}

#[test]
fn filter_on_view_column() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(
        &mut e,
        "CREATE VIEW calc (id, plus) AS SELECT id, v + 1 FROM t",
    );
    assert_eq!(
        q_set(&mut e, "SELECT id FROM calc WHERE plus = 21"),
        table(&[&["1"], &["4"]])
    );
    assert_eq!(
        q(&mut e, "SELECT id FROM calc WHERE plus IS NULL"),
        table(&[&["3"]])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT id, plus FROM calc ORDER BY plus DESC NULLS LAST, id LIMIT 2"
        ),
        table(&[&["1", "21"], &["4", "21"]])
    );
}

#[test]
fn view_reflects_later_inserts() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(
        &mut e,
        "CREATE VIEW twenty AS SELECT id FROM t WHERE v = 20",
    );
    assert_eq!(
        q_set(&mut e, "SELECT id FROM twenty"),
        table(&[&["1"], &["4"]])
    );
    exec(&mut e, "INSERT INTO t VALUES (5, 20, 'e')");
    exec(&mut e, "UPDATE t SET v = 20 WHERE id = 2");
    exec(&mut e, "DELETE FROM t WHERE id = 1");
    assert_eq!(
        q_set(&mut e, "SELECT id FROM twenty"),
        table(&[&["2"], &["4"], &["5"]])
    );
}

#[test]
fn reopen_keeps_view() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    exec(
        &mut e,
        "CREATE VIEW \"Quoted View\" (\"Id\", label) AS SELECT id, s || '!' FROM t WHERE id < 3 ORDER BY id",
    );
    exec(
        &mut e,
        "CREATE VIEW atop AS SELECT \"Id\" FROM \"Quoted View\"",
    );
    let mut e = reopen(&dir, e);
    assert_eq!(
        q(&mut e, "SELECT \"Id\", label FROM \"Quoted View\""),
        table(&[&["1", "b!"], &["2", "a!"]])
    );
    assert_eq!(
        q_set(&mut e, "SELECT \"Id\" FROM atop"),
        table(&[&["1"], &["2"]])
    );
    // Tables still load next to the view rows.
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t"), table(&[&["4"]]));
    exec(&mut e, "INSERT INTO t VALUES (9, 1, 'z')");
    assert_dependent(&err(&mut e, "DROP TABLE t"));
    let mut e = reopen(&dir, e);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t"), table(&[&["5"]]));
    assert_dependent(&err(&mut e, "DROP VIEW \"Quoted View\""));
}

#[test]
fn drop_view() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "CREATE VIEW gone AS SELECT id FROM t");
    exec(&mut e, "DROP VIEW gone");
    assert_eq!(state(&mut e, "SELECT id FROM gone"), "42P01");
    assert_eq!(state(&mut e, "DROP VIEW gone"), "42P01");
    exec(&mut e, "DROP VIEW IF EXISTS gone");
    // DROP VIEW does not drop tables and DROP TABLE does not drop views.
    assert_eq!(state(&mut e, "DROP VIEW t"), "42809");
    exec(&mut e, "CREATE VIEW w AS SELECT id FROM t");
    assert_eq!(state(&mut e, "DROP TABLE w"), "42809");
    let mut e = reopen(&dir, e);
    assert_eq!(state(&mut e, "SELECT id FROM gone"), "42P01");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM w"), table(&[&["4"]]));
    // The name can be reused for a table once the view is gone.
    exec(&mut e, "DROP VIEW w");
    exec(&mut e, "CREATE TABLE w (a INTEGER)");
    exec(&mut e, "INSERT INTO w VALUES (1)");
    assert_eq!(q(&mut e, "SELECT a FROM w"), table(&[&["1"]]));
}

#[test]
fn dml_on_view_is_unsupported() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "CREATE VIEW ro AS SELECT id, v FROM t");
    for sql in [
        "INSERT INTO ro VALUES (9, 9)",
        "UPDATE ro SET v = 1",
        "DELETE FROM ro",
        "CREATE INDEX ro_idx ON ro (id)",
        "ALTER TABLE ro ADD COLUMN z INTEGER",
    ] {
        let v = err(&mut e, sql);
        assert_eq!(v.sqlstate(), "0A000", "{sql}: {v}");
    }
    assert!(
        err(&mut e, "INSERT INTO ro VALUES (9, 9)")
            .to_string()
            .contains("cannot modify view")
    );
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t"), table(&[&["4"]]));
}

#[test]
fn create_view_in_transaction_rolls_back() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "BEGIN");
    exec(&mut e, "CREATE VIEW tx_view AS SELECT id FROM t");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM tx_view"), table(&[&["4"]]));
    exec(&mut e, "ROLLBACK");
    assert_eq!(state(&mut e, "SELECT id FROM tx_view"), "42P01");
    // DROP VIEW rolls back too.
    exec(&mut e, "CREATE VIEW keep AS SELECT id FROM t");
    exec(&mut e, "BEGIN");
    exec(&mut e, "DROP VIEW keep");
    assert_eq!(state(&mut e, "SELECT id FROM keep"), "42P01");
    exec(&mut e, "ROLLBACK");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM keep"), table(&[&["4"]]));
    // Committed CREATE VIEW survives reopen; rolled-back one does not.
    exec(&mut e, "BEGIN");
    exec(
        &mut e,
        "CREATE VIEW committed AS SELECT id FROM t WHERE id = 2",
    );
    exec(&mut e, "COMMIT");
    let mut e = reopen(&dir, e);
    assert_eq!(q(&mut e, "SELECT id FROM committed"), table(&[&["2"]]));
    assert_eq!(state(&mut e, "SELECT id FROM tx_view"), "42P01");
}

#[test]
fn ddl_in_failed_transaction_is_refused() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "BEGIN");
    assert_eq!(state(&mut e, "INSERT INTO t VALUES (1, 1, 'dup')"), "23505");
    assert_eq!(state(&mut e, "CREATE VIEW fv AS SELECT id FROM t"), "25P02");
    assert_eq!(state(&mut e, "ALTER TABLE t ADD COLUMN z INTEGER"), "25P02");
    assert_eq!(state(&mut e, "DROP TABLE t"), "25P02");
    exec(&mut e, "ROLLBACK");
    assert_eq!(state(&mut e, "SELECT id FROM fv"), "42P01");
    assert_eq!(header(&mut e, "SELECT * FROM t"), vec!["id", "v", "s"]);
}

#[test]
fn table_and_view_name_clash() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    let v = err(&mut e, "CREATE VIEW t AS SELECT 1 AS one");
    assert!(matches!(v, DbError::DuplicateObject(_)), "{v:?}");
    assert_eq!(v.sqlstate(), "42P07");
    assert!(v.to_string().contains("already exists"), "{v}");
    exec(&mut e, "CREATE VIEW tv AS SELECT id FROM t");
    assert_eq!(state(&mut e, "CREATE TABLE tv (a INTEGER)"), "42P07");
    assert_eq!(state(&mut e, "CREATE VIEW tv AS SELECT v FROM t"), "42P07");
    // The view keeps its definition.
    assert_eq!(header(&mut e, "SELECT * FROM tv"), vec!["id"]);
}

#[test]
fn view_star_keeps_definition_time_columns() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "CREATE VIEW star AS SELECT * FROM t WHERE id <= 2");
    exec(
        &mut e,
        "CREATE VIEW qstar AS SELECT x.* FROM t AS x WHERE x.id = 1",
    );
    exec(&mut e, "ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 5");
    assert_eq!(header(&mut e, "SELECT * FROM star"), vec!["id", "v", "s"]);
    assert_eq!(
        q_set(&mut e, "SELECT * FROM star"),
        table(&[&["1", "20", "b"], &["2", "10", "a"]])
    );
    assert_eq!(header(&mut e, "SELECT * FROM qstar"), vec!["id", "v", "s"]);
    let mut e = reopen(&dir, e);
    assert_eq!(header(&mut e, "SELECT * FROM star"), vec!["id", "v", "s"]);
    assert_eq!(
        header(&mut e, "SELECT * FROM t"),
        vec!["id", "v", "s", "extra"]
    );
}

// ---- ALTER TABLE ADD COLUMN -----------------------------------------------------------

#[test]
fn alter_add() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "ALTER TABLE t ADD COLUMN n INTEGER");
    assert_eq!(
        q(&mut e, "SELECT n FROM t"),
        table(&[&["NULL"], &["NULL"], &["NULL"], &["NULL"]])
    );
    assert_eq!(header(&mut e, "SELECT * FROM t"), vec!["id", "v", "s", "n"]);
    // The COLUMN keyword is optional.
    exec(&mut e, "ALTER TABLE t ADD m TEXT");
    assert_eq!(
        header(&mut e, "SELECT * FROM t"),
        vec!["id", "v", "s", "n", "m"]
    );
    exec(&mut e, "INSERT INTO t VALUES (5, 1, 'e', 7, 'mm')");
    assert_eq!(
        q(&mut e, "SELECT n, m FROM t WHERE id = 5"),
        table(&[&["7", "mm"]])
    );
}

#[test]
fn add_column_with_default() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 7");
    assert_eq!(q(&mut e, "SELECT DISTINCT n FROM t"), table(&[&["7"]]));
    exec(&mut e, "INSERT INTO t (id, v, s) VALUES (5, 1, 'e')");
    assert_eq!(q(&mut e, "SELECT n FROM t WHERE id = 5"), table(&[&["7"]]));
    exec(&mut e, "INSERT INTO t VALUES (6, 1, 'f', DEFAULT)");
    assert_eq!(q(&mut e, "SELECT n FROM t WHERE id = 6"), table(&[&["7"]]));
    assert_eq!(
        q(&mut e, "SELECT COUNT(*) FROM t WHERE n = 7"),
        table(&[&["6"]])
    );
}

#[test]
fn add_decimal_column_rounds_default() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(
        &mut e,
        "ALTER TABLE t ADD COLUMN price DECIMAL(5,2) DEFAULT 1.005",
    );
    assert_eq!(
        q(&mut e, "SELECT DISTINCT price FROM t"),
        table(&[&["1.01"]])
    );
    exec(&mut e, "INSERT INTO t (id) VALUES (5)");
    assert_eq!(
        q(&mut e, "SELECT price FROM t WHERE id = 5"),
        table(&[&["1.01"]])
    );
    assert_eq!(
        state(
            &mut e,
            "ALTER TABLE t ADD COLUMN big DECIMAL(3,2) DEFAULT 123.4"
        ),
        "22003"
    );
    assert_eq!(
        header(&mut e, "SELECT * FROM t"),
        vec!["id", "v", "s", "price"]
    );
}

#[test]
fn add_not_null_without_default_on_nonempty_table_fails() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    let v = err(&mut e, "ALTER TABLE t ADD COLUMN nn INTEGER NOT NULL");
    assert!(matches!(v, DbError::NullViolation(_)), "{v:?}");
    assert_eq!(v.sqlstate(), "23502");
    assert_eq!(header(&mut e, "SELECT * FROM t"), vec!["id", "v", "s"]);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t"), table(&[&["4"]]));
    let mut e = reopen(&dir, e);
    assert_eq!(header(&mut e, "SELECT * FROM t"), vec!["id", "v", "s"]);
    // On an empty table it is fine.
    exec(&mut e, "CREATE TABLE empty (a INTEGER)");
    exec(&mut e, "ALTER TABLE empty ADD COLUMN b INTEGER NOT NULL");
    assert_eq!(state(&mut e, "INSERT INTO empty (a) VALUES (1)"), "22023");
    assert_eq!(state(&mut e, "INSERT INTO empty VALUES (1, NULL)"), "23502");
    exec(&mut e, "INSERT INTO empty VALUES (1, 2)");
}

#[test]
fn add_not_null_with_default() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(
        &mut e,
        "ALTER TABLE t ADD COLUMN z INTEGER NOT NULL DEFAULT 0",
    );
    assert_eq!(q(&mut e, "SELECT DISTINCT z FROM t"), table(&[&["0"]]));
    assert_eq!(state(&mut e, "UPDATE t SET z = NULL WHERE id = 1"), "23502");
    exec(&mut e, "INSERT INTO t (id) VALUES (5)");
    assert_eq!(q(&mut e, "SELECT z FROM t WHERE id = 5"), table(&[&["0"]]));
}

#[test]
fn add_duplicate_column_fails() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    let v = err(&mut e, "ALTER TABLE t ADD COLUMN v TEXT");
    assert_eq!(v.sqlstate(), "42701", "{v}");
    assert_eq!(header(&mut e, "SELECT * FROM t"), vec!["id", "v", "s"]);
    assert_eq!(
        state(&mut e, "ALTER TABLE missing ADD COLUMN x INTEGER"),
        "42P01"
    );
}

#[test]
fn create_table_duplicate_column_follows_identifier_rule() {
    let (_dir, mut e) = engine();
    assert_eq!(
        state(&mut e, "CREATE TABLE d1 (x INTEGER, X INTEGER)"),
        "42701"
    );
    assert_eq!(
        state(&mut e, "CREATE TABLE d2 (\"x\" INTEGER, x INTEGER)"),
        "42701"
    );
    assert_eq!(
        state(&mut e, "CREATE TABLE d3 (x INTEGER, \"X\" INTEGER)"),
        "42701"
    );
    assert_eq!(
        state(&mut e, "CREATE TABLE d4 (\"x\" INTEGER, \"x\" INTEGER)"),
        "42701"
    );
    exec(&mut e, "CREATE TABLE d5 (\"x\" INTEGER, \"X\" INTEGER)");
    assert_eq!(header(&mut e, "SELECT * FROM d5"), vec!["x", "X"]);
}

#[test]
fn add_column_duplicate_follows_identifier_rule() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE f (x INTEGER)");
    assert_eq!(state(&mut e, "ALTER TABLE f ADD COLUMN X INTEGER"), "42701");
    assert_eq!(
        state(&mut e, "ALTER TABLE f ADD COLUMN \"x\" INTEGER"),
        "42701"
    );
    exec(&mut e, "ALTER TABLE f ADD COLUMN \"X\" INTEGER");
    assert_eq!(header(&mut e, "SELECT * FROM f"), vec!["x", "X"]);
}

#[test]
fn add_column_unsupported_constraints() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    for sql in [
        "ALTER TABLE t ADD COLUMN k INTEGER PRIMARY KEY",
        "ALTER TABLE t ADD COLUMN k INTEGER REFERENCES t(id)",
        "ALTER TABLE t ADD COLUMN k INTEGER NOT NULL AUTO_INCREMENT",
        "ALTER TABLE t DROP COLUMN v",
    ] {
        assert_eq!(state(&mut e, sql), "0A000", "{sql}");
    }
    assert_eq!(header(&mut e, "SELECT * FROM t"), vec!["id", "v", "s"]);
}

#[test]
fn add_column_unique_is_validated() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    // Every row would get the same non-NULL default: duplicate.
    let v = err(
        &mut e,
        "ALTER TABLE t ADD COLUMN u INTEGER UNIQUE DEFAULT 1",
    );
    assert_eq!(v.sqlstate(), "23505", "{v}");
    assert_eq!(header(&mut e, "SELECT * FROM t"), vec!["id", "v", "s"]);
    // NULLs are distinct.
    exec(&mut e, "ALTER TABLE t ADD COLUMN u INTEGER UNIQUE");
    exec(&mut e, "UPDATE t SET u = id");
    let v = err(&mut e, "INSERT INTO t (id, u) VALUES (9, 1)");
    assert!(
        matches!(&v, DbError::UniqueViolation { constraint, .. } if constraint == "t_u_key"),
        "{v:?}"
    );
}

#[test]
fn add_column_check_validated_against_existing_rows() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    let v = err(
        &mut e,
        "ALTER TABLE t ADD COLUMN x INTEGER CHECK (x > 0) DEFAULT 0",
    );
    assert!(
        matches!(&v, DbError::CheckViolation { constraint } if constraint == "t_x_check"),
        "{v:?}"
    );
    assert_eq!(v.sqlstate(), "23514");
    assert_eq!(header(&mut e, "SELECT * FROM t"), vec!["id", "v", "s"]);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t"), table(&[&["4"]]));
    // A passing CHECK is enforced afterwards and survives reopen.
    exec(
        &mut e,
        "ALTER TABLE t ADD COLUMN x INTEGER CHECK (x > 0) DEFAULT 3",
    );
    assert_eq!(state(&mut e, "UPDATE t SET x = 0 WHERE id = 1"), "23514");
    let mut e = reopen(&dir, e);
    assert_eq!(
        state(&mut e, "INSERT INTO t VALUES (9, 1, 'q', -1)"),
        "23514"
    );
    exec(&mut e, "INSERT INTO t VALUES (9, 1, 'q', 1)");
}

#[test]
fn reopen_after_alter_keeps_column_and_values() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    exec(
        &mut e,
        "ALTER TABLE t ADD COLUMN code VARCHAR(5) DEFAULT 'ab'",
    );
    exec(
        &mut e,
        "ALTER TABLE t ADD COLUMN amt DECIMAL(6,2) NOT NULL DEFAULT 2.5",
    );
    exec(&mut e, "UPDATE t SET code = 'xyz' WHERE id = 3");
    let mut e = reopen(&dir, e);
    assert_eq!(
        header(&mut e, "SELECT * FROM t"),
        vec!["id", "v", "s", "code", "amt"]
    );
    assert_eq!(
        q(&mut e, "SELECT id, code, amt FROM t ORDER BY id"),
        table(&[
            &["1", "ab", "2.50"],
            &["2", "ab", "2.50"],
            &["3", "xyz", "2.50"],
            &["4", "ab", "2.50"]
        ])
    );
    assert_eq!(
        state(&mut e, "UPDATE t SET code = 'toolong' WHERE id = 1"),
        "22001"
    );
    exec(&mut e, "INSERT INTO t (id) VALUES (5)");
    assert_eq!(
        q(&mut e, "SELECT code, amt FROM t WHERE id = 5"),
        table(&[&["ab", "2.50"]])
    );
}

#[test]
fn alter_in_transaction_rollback_restores_shape_and_data() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "BEGIN");
    exec(&mut e, "ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 1");
    exec(&mut e, "UPDATE t SET n = 2 WHERE id = 1");
    assert_eq!(q(&mut e, "SELECT n FROM t WHERE id = 1"), table(&[&["2"]]));
    exec(&mut e, "ROLLBACK");
    assert_eq!(header(&mut e, "SELECT * FROM t"), vec!["id", "v", "s"]);
    assert_eq!(
        q_set(&mut e, "SELECT * FROM t"),
        table(&[
            &["1", "20", "b"],
            &["2", "10", "a"],
            &["3", "NULL", "c"],
            &["4", "20", "b"]
        ])
    );
    assert_eq!(state(&mut e, "SELECT n FROM t"), "42703");
    exec(&mut e, "INSERT INTO t VALUES (5, 5, 'e')");
    let mut e = reopen(&dir, e);
    assert_eq!(header(&mut e, "SELECT * FROM t"), vec!["id", "v", "s"]);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t"), table(&[&["5"]]));
    // Committed ALTER inside a transaction persists.
    exec(&mut e, "BEGIN");
    exec(&mut e, "INSERT INTO t VALUES (6, 6, 'f')");
    exec(&mut e, "ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 1");
    exec(&mut e, "COMMIT");
    let mut e = reopen(&dir, e);
    assert_eq!(
        q(&mut e, "SELECT COUNT(*) FROM t WHERE n = 1"),
        table(&[&["6"]])
    );
}

#[test]
fn update_and_order_by_new_column() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "ALTER TABLE t ADD COLUMN rank INTEGER");
    exec(&mut e, "UPDATE t SET rank = 10 - id");
    exec(&mut e, "UPDATE t SET rank = rank * 2 WHERE s = 'b'");
    assert_eq!(
        q(&mut e, "SELECT id, rank FROM t ORDER BY rank"),
        table(&[&["3", "7"], &["2", "8"], &["4", "12"], &["1", "18"]])
    );
    assert_eq!(q(&mut e, "SELECT SUM(rank) FROM t"), table(&[&["45"]]));
    exec(&mut e, "DELETE FROM t WHERE rank > 10");
    assert_eq!(q_set(&mut e, "SELECT id FROM t"), table(&[&["2"], &["3"]]));
}

#[test]
fn add_column_to_table_with_index_and_fk() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY, name TEXT)");
    exec(
        &mut e,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
    );
    exec(&mut e, "CREATE INDEX p_name ON p (name)");
    exec(&mut e, "INSERT INTO p VALUES (1, 'a'), (2, 'b')");
    exec(&mut e, "INSERT INTO c VALUES (1, 1)");
    exec(&mut e, "ALTER TABLE p ADD COLUMN extra TEXT DEFAULT 'e'");
    assert_eq!(
        q(&mut e, "SELECT id, extra FROM p WHERE name = 'a'"),
        table(&[&["1", "e"]])
    );
    assert_eq!(state(&mut e, "DELETE FROM p WHERE id = 1"), "23503");
    exec(&mut e, "DELETE FROM p WHERE id = 2");
    assert_eq!(q(&mut e, "SELECT id FROM p"), table(&[&["1"]]));
    assert_eq!(state(&mut e, "INSERT INTO c VALUES (2, 7)"), "23503");
}

/// A transaction whose snapshot predates a committed ADD COLUMN still sees
/// the old (short) row versions; reads pad them with NULL instead of failing.
#[test]
fn older_snapshot_reads_short_rows_padded_with_null() {
    use aerodb::db::Database;
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(db_path(&dir).to_str().unwrap()).unwrap();
    let setup = db.begin().unwrap();
    db.query(&setup, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.query(&setup, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    db.commit(setup).unwrap();

    let old = db.begin().unwrap();
    let alter = db.begin().unwrap();
    db.query(&alter, "ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 5")
        .unwrap();
    db.commit(alter).unwrap();

    let render = |r: aerodb::execution::query::QueryResult| -> Vec<Vec<String>> {
        sorted(
            r.rows
                .iter()
                .map(|row| row.iter().map(|v| v.to_string()).collect())
                .collect(),
        )
    };
    // The schema is not snapshot-versioned: the old transaction sees the new
    // column, its row versions predate it and read as NULL.
    let seen = render(db.query(&old, "SELECT id, v, n FROM t").unwrap());
    assert_eq!(seen, table(&[&["1", "10", "NULL"], &["2", "20", "NULL"]]));
    let filtered = render(db.query(&old, "SELECT id FROM t WHERE n IS NULL").unwrap());
    assert_eq!(filtered, table(&[&["1"], &["2"]]));
    db.abort(old).unwrap();

    let fresh = db.begin().unwrap();
    let seen = render(db.query(&fresh, "SELECT id, v, n FROM t").unwrap());
    assert_eq!(seen, table(&[&["1", "10", "5"], &["2", "20", "5"]]));
    db.commit(fresh).unwrap();
}

/// Page-1 catalog keys are not derived from the number of relations, so a
/// drop followed by creates never reuses a live key (tables and views).
#[test]
fn catalog_keys_are_not_reused_after_drop() {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE a (x INTEGER)");
    exec(&mut e, "CREATE TABLE b (x INTEGER)");
    exec(&mut e, "CREATE VIEW bv AS SELECT x FROM b");
    exec(&mut e, "DROP TABLE a");
    exec(&mut e, "CREATE TABLE c (x INTEGER)");
    exec(&mut e, "CREATE VIEW cv AS SELECT x FROM c");
    exec(&mut e, "INSERT INTO b VALUES (2)");
    exec(&mut e, "INSERT INTO c VALUES (3)");
    let mut e = reopen(&dir, e);
    assert_eq!(q(&mut e, "SELECT x FROM bv"), table(&[&["2"]]));
    assert_eq!(q(&mut e, "SELECT x FROM cv"), table(&[&["3"]]));
    assert_eq!(state(&mut e, "SELECT x FROM a"), "42P01");
}

/// Enough tables that their page-1 rows split the catalog root; view rows
/// interleaved with table rows survive root updates and reopen.
#[test]
fn many_tables_and_views_share_the_catalog_page() {
    let (dir, mut e) = engine();
    for i in 0..40 {
        exec(
            &mut e,
            &format!("CREATE TABLE t{i} (id INTEGER PRIMARY KEY, label TEXT, amount DECIMAL(8,2))"),
        );
        exec(
            &mut e,
            &format!("CREATE VIEW v{i} AS SELECT id, label FROM t{i} WHERE amount > 0"),
        );
        exec(
            &mut e,
            &format!("INSERT INTO t{i} VALUES ({i}, 'row{i}', 1.5)"),
        );
    }
    let mut e = reopen(&dir, e);
    for i in [0, 17, 39] {
        assert_eq!(
            q(&mut e, &format!("SELECT id, label FROM v{i}")),
            table(&[&[&i.to_string(), &format!("row{i}")]])
        );
    }
    exec(&mut e, "ALTER TABLE t5 ADD COLUMN extra INTEGER DEFAULT 1");
    let mut e = reopen(&dir, e);
    assert_eq!(q(&mut e, "SELECT extra FROM t5"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT label FROM v5"), table(&[&["row5"]]));
    // Catalog growth inside a rolled-back transaction leaves no trace.
    exec(&mut e, "BEGIN");
    for i in 0..30 {
        exec(
            &mut e,
            &format!("CREATE VIEW extra_view_number_{i} AS SELECT id FROM t{i}"),
        );
    }
    exec(&mut e, "ROLLBACK");
    assert_eq!(state(&mut e, "SELECT id FROM extra_view_number_3"), "42P01");
    assert_eq!(q(&mut e, "SELECT label FROM v39"), table(&[&["row39"]]));
    let mut e = reopen(&dir, e);
    assert_eq!(state(&mut e, "SELECT id FROM extra_view_number_3"), "42P01");
    assert_eq!(q(&mut e, "SELECT id FROM v0"), table(&[&["0"]]));
}

// ---- catalog type round trips -------------------------------------------------------------

#[test]
fn decimal_and_varchar_types_survive_reopen() {
    let (dir, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE ty (id INTEGER PRIMARY KEY, d DECIMAL(7,3), vc VARCHAR(4), cv CHARACTER VARYING(3))",
    );
    exec(&mut e, "INSERT INTO ty VALUES (1, 1.23456, 'abcd', 'xy')");
    let types = |e: &mut Engine| -> Vec<String> {
        e.query("SELECT d, vc, cv FROM ty")
            .unwrap()
            .columns
            .iter()
            .map(|c| c.ty.name())
            .collect()
    };
    let before = types(&mut e);
    assert_eq!(before, vec!["DECIMAL(7,3)", "VARCHAR(4)", "VARCHAR(3)"]);
    let mut e = reopen(&dir, e);
    assert_eq!(types(&mut e), before);
    assert_eq!(
        q(&mut e, "SELECT d, vc, cv FROM ty"),
        table(&[&["1.235", "abcd", "xy"]])
    );
    assert_eq!(
        state(&mut e, "INSERT INTO ty VALUES (2, 1, 'abcde', 'x')"),
        "22001"
    );
    assert_eq!(
        state(&mut e, "INSERT INTO ty VALUES (2, 12345.6, 'a', 'x')"),
        "22003"
    );
    exec(&mut e, "INSERT INTO ty VALUES (2, 2.0005, 'ä', 'xyz')");
    assert_eq!(
        q(&mut e, "SELECT d FROM ty WHERE id = 2"),
        table(&[&["2.001"]])
    );
}

// ---- several catalog writes to one relation inside one transaction -------------------------

/// Run `body` inside BEGIN … `end` (COMMIT or ROLLBACK).
fn in_tx(e: &mut Engine, end: &str, body: &[&str]) {
    exec(e, "BEGIN");
    for sql in body {
        exec(e, sql);
    }
    exec(e, end);
}

/// `n` INSERTs with wide rows: enough to split the table's root page.
fn wide_inserts(table: &str, n: usize) -> Vec<String> {
    (1..=n)
        .map(|i| {
            format!(
                "INSERT INTO {table} (id, pad) VALUES ({i}, '{}')",
                "x".repeat(200)
            )
        })
        .collect()
}

#[test]
fn create_and_drop_view_in_one_committed_transaction() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    in_tx(
        &mut e,
        "COMMIT",
        &["CREATE VIEW v AS SELECT id FROM t", "DROP VIEW v"],
    );
    assert_eq!(state(&mut e, "SELECT id FROM v"), "42P01");
    // A later rollback (catalog reload) must not resurrect it either.
    in_tx(&mut e, "ROLLBACK", &["INSERT INTO t VALUES (9, 9, 'z')"]);
    assert_eq!(state(&mut e, "SELECT id FROM v"), "42P01");
    let mut e = reopen(&dir, e);
    assert_eq!(state(&mut e, "SELECT id FROM v"), "42P01");
    // The name is free again.
    exec(&mut e, "CREATE VIEW v AS SELECT s FROM t WHERE id = 2");
    assert_eq!(q(&mut e, "SELECT s FROM v"), table(&[&["a"]]));
    // CREATE, DROP, CREATE again in one transaction keeps the last definition.
    in_tx(
        &mut e,
        "COMMIT",
        &[
            "CREATE VIEW w AS SELECT id FROM t",
            "DROP VIEW w",
            "CREATE VIEW w AS SELECT v FROM t WHERE id = 1",
        ],
    );
    let mut e = reopen(&dir, e);
    assert_eq!(q(&mut e, "SELECT * FROM w"), table(&[&["20"]]));
}

#[test]
fn create_and_drop_view_in_one_rolled_back_transaction() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    in_tx(
        &mut e,
        "ROLLBACK",
        &["CREATE VIEW v AS SELECT id FROM t", "DROP VIEW v"],
    );
    assert_eq!(state(&mut e, "SELECT id FROM v"), "42P01");
    let mut e = reopen(&dir, e);
    assert_eq!(state(&mut e, "SELECT id FROM v"), "42P01");
    exec(&mut e, "CREATE VIEW v AS SELECT id FROM t WHERE id = 3");
    assert_eq!(q(&mut e, "SELECT id FROM v"), table(&[&["3"]]));
}

#[test]
fn create_table_then_alter_in_one_transaction() {
    let (dir, mut e) = engine();
    in_tx(
        &mut e,
        "COMMIT",
        &[
            "CREATE TABLE a (id INTEGER PRIMARY KEY, s TEXT)",
            "INSERT INTO a VALUES (1, 'x'), (2, 'y')",
            "ALTER TABLE a ADD COLUMN c INTEGER DEFAULT 7",
            "INSERT INTO a (id, s) VALUES (3, 'z')",
        ],
    );
    let expected = table(&[&["1", "x", "7"], &["2", "y", "7"], &["3", "z", "7"]]);
    assert_eq!(q_set(&mut e, "SELECT * FROM a"), expected);
    let mut e = reopen(&dir, e);
    assert_eq!(header(&mut e, "SELECT * FROM a"), vec!["id", "s", "c"]);
    assert_eq!(q_set(&mut e, "SELECT * FROM a"), expected);

    in_tx(
        &mut e,
        "ROLLBACK",
        &[
            "CREATE TABLE b (id INTEGER PRIMARY KEY)",
            "INSERT INTO b VALUES (1)",
            "ALTER TABLE b ADD COLUMN c INTEGER DEFAULT 7",
        ],
    );
    assert_eq!(state(&mut e, "SELECT id FROM b"), "42P01");
    let mut e = reopen(&dir, e);
    assert_eq!(state(&mut e, "SELECT id FROM b"), "42P01");
    assert_eq!(q_set(&mut e, "SELECT * FROM a"), expected);
}

#[test]
fn two_alters_in_one_transaction() {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE b (id INTEGER PRIMARY KEY)");
    exec(&mut e, "INSERT INTO b VALUES (1), (2)");
    in_tx(
        &mut e,
        "COMMIT",
        &[
            "ALTER TABLE b ADD COLUMN c INTEGER DEFAULT 1",
            "ALTER TABLE b ADD COLUMN d TEXT DEFAULT 'q'",
        ],
    );
    let expected = table(&[&["1", "1", "q"], &["2", "1", "q"]]);
    assert_eq!(q_set(&mut e, "SELECT * FROM b"), expected);
    let mut e = reopen(&dir, e);
    assert_eq!(header(&mut e, "SELECT * FROM b"), vec!["id", "c", "d"]);
    assert_eq!(q_set(&mut e, "SELECT * FROM b"), expected);

    in_tx(
        &mut e,
        "ROLLBACK",
        &[
            "ALTER TABLE b ADD COLUMN e INTEGER",
            "ALTER TABLE b ADD COLUMN f INTEGER DEFAULT 3",
        ],
    );
    assert_eq!(header(&mut e, "SELECT * FROM b"), vec!["id", "c", "d"]);
    assert_eq!(q_set(&mut e, "SELECT * FROM b"), expected);
    let mut e = reopen(&dir, e);
    assert_eq!(header(&mut e, "SELECT * FROM b"), vec!["id", "c", "d"]);
    assert_eq!(q_set(&mut e, "SELECT * FROM b"), expected);
}

#[test]
fn create_table_root_split_then_alter_in_one_transaction() {
    let (dir, mut e) = engine();
    let mut body = vec!["CREATE TABLE r (id INTEGER PRIMARY KEY, pad TEXT)".to_string()];
    body.extend(wide_inserts("r", 120));
    body.push("ALTER TABLE r ADD COLUMN c INTEGER DEFAULT 5".to_string());
    let body_refs: Vec<&str> = body.iter().map(String::as_str).collect();
    in_tx(&mut e, "COMMIT", &body_refs);
    let check = |e: &mut Engine| {
        assert_eq!(header(e, "SELECT * FROM r"), vec!["id", "pad", "c"]);
        assert_eq!(
            q(e, "SELECT COUNT(*), MIN(c), MAX(c), SUM(id) FROM r"),
            table(&[&["120", "5", "5", "7260"]])
        );
    };
    check(&mut e);
    let mut e = reopen(&dir, e);
    check(&mut e);

    let mut body = vec!["CREATE TABLE r2 (id INTEGER PRIMARY KEY, pad TEXT)".to_string()];
    body.extend(wide_inserts("r2", 120));
    body.push("ALTER TABLE r2 ADD COLUMN c INTEGER DEFAULT 5".to_string());
    let body_refs: Vec<&str> = body.iter().map(String::as_str).collect();
    in_tx(&mut e, "ROLLBACK", &body_refs);
    assert_eq!(state(&mut e, "SELECT id FROM r2"), "42P01");
    let mut e = reopen(&dir, e);
    assert_eq!(state(&mut e, "SELECT id FROM r2"), "42P01");
    check(&mut e);
}

#[test]
fn existing_table_root_split_then_alter_in_one_transaction() {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE r (id INTEGER PRIMARY KEY, pad TEXT)");
    let mut body = wide_inserts("r", 120);
    body.push("ALTER TABLE r ADD COLUMN c INTEGER DEFAULT 5".to_string());
    let body_refs: Vec<&str> = body.iter().map(String::as_str).collect();
    in_tx(&mut e, "ROLLBACK", &body_refs);
    assert_eq!(header(&mut e, "SELECT * FROM r"), vec!["id", "pad"]);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM r"), table(&[&["0"]]));
    in_tx(&mut e, "COMMIT", &body_refs);
    let mut e = reopen(&dir, e);
    assert_eq!(header(&mut e, "SELECT * FROM r"), vec!["id", "pad", "c"]);
    assert_eq!(
        q(&mut e, "SELECT COUNT(*), MIN(c), MAX(c) FROM r"),
        table(&[&["120", "5", "5"]])
    );
}

#[test]
fn rolled_back_root_split_then_committed_inserts_survive_reopen() {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE r (id INTEGER PRIMARY KEY, pad TEXT)");
    let body = wide_inserts("r", 120);
    let body_refs: Vec<&str> = body.iter().map(String::as_str).collect();
    in_tx(&mut e, "ROLLBACK", &body_refs);
    let body: Vec<String> = (1..=300)
        .map(|i| {
            format!(
                "INSERT INTO r (id, pad) VALUES ({}, '{}')",
                i + 1000,
                "y".repeat(200)
            )
        })
        .collect();
    let body_refs: Vec<&str> = body.iter().map(String::as_str).collect();
    in_tx(&mut e, "COMMIT", &body_refs);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM r"), table(&[&["300"]]));
    let mut e = reopen(&dir, e);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM r"), table(&[&["300"]]));
    assert_eq!(
        q(&mut e, "SELECT id FROM r WHERE id = 1150"),
        table(&[&["1150"]])
    );
}

/// INSERTs of wide rows with the given ids; `pad` is `fill` x 200 followed
/// by the id (distinct per row, so an index on it stays small).
fn wide_range(table: &str, ids: impl Iterator<Item = i32>, fill: char) -> Vec<String> {
    ids.map(|i| {
        format!(
            "INSERT INTO {table} (id, pad) VALUES ({i}, '{}{i}')",
            fill.to_string().repeat(200)
        )
    })
    .collect()
}

fn run_tx(e: &mut Engine, end: &str, body: &[String]) {
    let refs: Vec<&str> = body.iter().map(String::as_str).collect();
    in_tx(e, end, &refs);
}

/// An aborted transaction splits leaves under an internal root (no root
/// move); later committed rows land in the leaves it created and must be
/// found again after reopen.
#[test]
fn rolled_back_leaf_splits_then_committed_inserts_survive_reopen() {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE r (id INTEGER PRIMARY KEY, pad TEXT)");
    run_tx(
        &mut e,
        "COMMIT",
        &wide_range("r", (1..=120).map(|i| i * 10), 'a'),
    );
    run_tx(
        &mut e,
        "ROLLBACK",
        &wide_range("r", (1..=300).map(|i| i * 4 + 1), 'b'),
    );
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM r"), table(&[&["120"]]));
    run_tx(
        &mut e,
        "COMMIT",
        &wide_range("r", (1..=300).map(|i| i * 4 + 3), 'c'),
    );
    let check = |e: &mut Engine| {
        assert_eq!(
            q(e, "SELECT COUNT(*), SUM(id) FROM r"),
            table(&[&["420", "254100"]])
        );
        assert_eq!(
            q(e, "SELECT id FROM r WHERE id = 1199"),
            table(&[&["1199"]])
        );
        assert_eq!(
            q(e, "SELECT COUNT(*) FROM r WHERE id = 1197"),
            table(&[&["0"]])
        );
    };
    check(&mut e);
    let mut e = reopen(&dir, e);
    check(&mut e);
}

/// Several rounds of: grow the table (root split / deeper tree) and ALTER in
/// one transaction, then ROLLBACK; read in between; finally commit a round
/// and reopen.
#[test]
fn repeated_root_splits_alter_rollback_and_reopen() {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE r (id INTEGER PRIMARY KEY, pad TEXT)");
    exec(&mut e, "CREATE INDEX r_pad ON r (pad)");
    let mut committed: i64 = 0;
    let mut count = 0;
    for round in 0..4i32 {
        let base = round * 1000;
        let mut body = wide_range("r", (1..=150).map(|i| base + i), 'r');
        body.push("ALTER TABLE r ADD COLUMN c INTEGER DEFAULT 5".to_string());
        body.push("ALTER TABLE r ADD COLUMN d TEXT DEFAULT 'q'".to_string());
        run_tx(&mut e, "ROLLBACK", &body);
        assert_eq!(
            header(&mut e, "SELECT * FROM r"),
            vec!["id", "pad"],
            "round {round}"
        );
        assert_eq!(
            q(&mut e, "SELECT COUNT(*) FROM r"),
            vec![vec![count.to_string()]],
            "round {round}"
        );
        // Commit a smaller batch in a disjoint key range.
        run_tx(
            &mut e,
            "COMMIT",
            &wide_range("r", (1..=40).map(|i| base + 500 + i), 'k'),
        );
        count += 40;
        committed += (1..=40).map(|i| i64::from(base + 500 + i)).sum::<i64>();
        assert_eq!(
            q(&mut e, "SELECT COUNT(*), SUM(id) FROM r"),
            vec![vec![count.to_string(), committed.to_string()]]
        );
    }
    let mut body = wide_range("r", (1..=150).map(|i| 9000 + i), 'z');
    body.push("ALTER TABLE r ADD COLUMN c INTEGER DEFAULT 5".to_string());
    run_tx(&mut e, "COMMIT", &body);
    count += 150;
    committed += (1..=150).map(|i| i64::from(9000 + i)).sum::<i64>();
    let check = |e: &mut Engine| {
        assert_eq!(header(e, "SELECT * FROM r"), vec!["id", "pad", "c"]);
        assert_eq!(
            q(e, "SELECT COUNT(*), SUM(id), MIN(c), MAX(c) FROM r"),
            vec![vec![
                count.to_string(),
                committed.to_string(),
                "5".to_string(),
                "5".to_string()
            ]]
        );
        assert_eq!(
            q(e, "SELECT id FROM r WHERE id = 3540"),
            table(&[&["3540"]])
        );
        assert_eq!(
            q(e, "SELECT COUNT(*) FROM r WHERE id = 3001"),
            table(&[&["0"]])
        );
        let z = "z".repeat(200);
        assert_eq!(
            q(e, &format!("SELECT id FROM r WHERE pad = '{z}9150'")),
            table(&[&["9150"]])
        );
        assert_eq!(
            q(
                e,
                &format!("SELECT id FROM r WHERE pad = '{}3540'", "k".repeat(200))
            ),
            table(&[&["3540"]])
        );
        assert_eq!(
            q(
                e,
                &format!(
                    "SELECT COUNT(*) FROM r WHERE pad = '{}3001'",
                    "r".repeat(200)
                )
            ),
            table(&[&["0"]])
        );
    };
    check(&mut e);
    let mut e = reopen(&dir, e);
    check(&mut e);
}

/// Index entries and sequence rows rewritten twice in one transaction (the
/// rewrite deletes a version the same transaction created).
#[test]
fn index_and_sequence_rows_rewritten_in_one_transaction() {
    let (dir, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE x (id INT NOT NULL AUTO_INCREMENT, s TEXT)",
    );
    exec(&mut e, "CREATE INDEX x_s ON x (s)");
    in_tx(
        &mut e,
        "COMMIT",
        &[
            "INSERT INTO x (s) VALUES ('a')",
            "INSERT INTO x (s) VALUES ('a')",
            "INSERT INTO x (s) VALUES ('a')",
        ],
    );
    let check = |e: &mut Engine| {
        assert_eq!(
            q_set(e, "SELECT id FROM x WHERE s = 'a'"),
            table(&[&["1"], &["2"], &["3"]])
        );
        assert_eq!(
            q_set(e, "SELECT * FROM x"),
            table(&[&["1", "a"], &["2", "a"], &["3", "a"]])
        );
    };
    check(&mut e);
    let mut e = reopen(&dir, e);
    check(&mut e);
    in_tx(
        &mut e,
        "ROLLBACK",
        &[
            "INSERT INTO x (s) VALUES ('a')",
            "INSERT INTO x (s) VALUES ('a')",
        ],
    );
    check(&mut e);
    exec(&mut e, "INSERT INTO x (s) VALUES ('b')");
    assert_eq!(
        q(&mut e, "SELECT COUNT(*) FROM x WHERE s = 'b'"),
        table(&[&["1"]])
    );
    let mut e = reopen(&dir, e);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM x"), table(&[&["4"]]));
}

#[test]
fn root_split_rollback_variants() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE r (id INTEGER PRIMARY KEY, pad TEXT)");
    let body = wide_inserts("r", 120);
    let body_refs: Vec<&str> = body.iter().map(String::as_str).collect();
    in_tx(&mut e, "ROLLBACK", &body_refs);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM r"), table(&[&["0"]]));
    in_tx(&mut e, "COMMIT", &body_refs);
    in_tx(
        &mut e,
        "ROLLBACK",
        &["ALTER TABLE r ADD COLUMN c INTEGER DEFAULT 5"],
    );
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM r"), table(&[&["120"]]));
    exec(&mut e, "CREATE TABLE r3 (id INTEGER PRIMARY KEY, pad TEXT)");
    in_tx(
        &mut e,
        "ROLLBACK",
        &[
            "INSERT INTO r3 VALUES (1, 'a')",
            "ALTER TABLE r3 ADD COLUMN c INTEGER DEFAULT 5",
        ],
    );
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM r3"), table(&[&["0"]]));
    let mut body = wide_inserts("r3", 20);
    body.push("ALTER TABLE r3 ADD COLUMN c INTEGER DEFAULT 5".to_string());
    let body_refs: Vec<&str> = body.iter().map(String::as_str).collect();
    in_tx(&mut e, "ROLLBACK", &body_refs);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM r3"), table(&[&["0"]]));
    exec(&mut e, "CREATE TABLE r4 (id INTEGER PRIMARY KEY, pad TEXT)");
    let mut body = wide_inserts("r4", 120);
    body.push("UPDATE r4 SET id = id + 1000".to_string());
    let body_refs: Vec<&str> = body.iter().map(String::as_str).collect();
    in_tx(&mut e, "ROLLBACK", &body_refs);
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM r4"), table(&[&["0"]]));
}

#[test]
fn create_and_drop_table_in_one_transaction() {
    let (dir, mut e) = engine();
    in_tx(
        &mut e,
        "COMMIT",
        &[
            "CREATE TABLE d (id INTEGER PRIMARY KEY)",
            "INSERT INTO d VALUES (1)",
            "DROP TABLE d",
        ],
    );
    assert_eq!(state(&mut e, "SELECT id FROM d"), "42P01");
    in_tx(&mut e, "ROLLBACK", &["CREATE TABLE other (x INTEGER)"]);
    assert_eq!(state(&mut e, "SELECT id FROM d"), "42P01");
    let mut e = reopen(&dir, e);
    assert_eq!(state(&mut e, "SELECT id FROM d"), "42P01");
    exec(&mut e, "CREATE TABLE d (x TEXT)");
    exec(&mut e, "INSERT INTO d VALUES ('again')");
    assert_eq!(q(&mut e, "SELECT * FROM d"), table(&[&["again"]]));

    in_tx(
        &mut e,
        "ROLLBACK",
        &[
            "CREATE TABLE d2 (id INTEGER PRIMARY KEY)",
            "INSERT INTO d2 VALUES (1)",
            "DROP TABLE d2",
        ],
    );
    assert_eq!(state(&mut e, "SELECT id FROM d2"), "42P01");
    let mut e = reopen(&dir, e);
    assert_eq!(state(&mut e, "SELECT id FROM d2"), "42P01");
    assert_eq!(q(&mut e, "SELECT * FROM d"), table(&[&["again"]]));
}

// ---- wrong object type ----------------------------------------------------------------------

#[test]
fn drop_wrong_object_type() {
    let (_dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "CREATE VIEW tv AS SELECT id FROM t");
    for (sql, msg) in [
        ("DROP TABLE tv", "tv is not a table"),
        ("DROP TABLE IF EXISTS tv", "tv is not a table"),
        ("DROP VIEW t", "t is not a view"),
        ("DROP VIEW IF EXISTS t", "t is not a view"),
    ] {
        let v = err(&mut e, sql);
        assert!(matches!(v, DbError::WrongObjectType(_)), "{sql}: {v:?}");
        assert_eq!(v.sqlstate(), "42809", "{sql}");
        assert_eq!(v.to_string(), msg, "{sql}");
    }
    // Nothing was dropped; missing names keep their old behaviour.
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM tv"), table(&[&["4"]]));
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM t"), table(&[&["4"]]));
    assert_eq!(state(&mut e, "DROP TABLE nope"), "42P01");
    assert_eq!(state(&mut e, "DROP VIEW nope"), "42P01");
    exec(&mut e, "DROP TABLE IF EXISTS nope");
    exec(&mut e, "DROP VIEW IF EXISTS nope");
}

// ---- `*` inside subqueries of a stored view ----------------------------------------------------

#[test]
fn view_star_in_subqueries_keeps_definition_time_columns() {
    let (dir, mut e) = engine();
    audit_t(&mut e);
    exec(&mut e, "CREATE TABLE t2 (k INTEGER)");
    exec(&mut e, "INSERT INTO t2 VALUES (1), (3)");
    exec(
        &mut e,
        "CREATE VIEW v8 AS SELECT id FROM t WHERE id IN (SELECT * FROM t2)",
    );
    exec(
        &mut e,
        "CREATE VIEW v9 AS SELECT id, (SELECT * FROM t2 WHERE k = t.id) AS kk FROM t WHERE id = 3",
    );
    exec(
        &mut e,
        "CREATE VIEW v10 AS SELECT id FROM t WHERE EXISTS (SELECT * FROM (SELECT * FROM t2) AS s WHERE s.k = t.id)",
    );
    exec(
        &mut e,
        "CREATE VIEW v11 AS SELECT id FROM t WHERE id IN (SELECT * FROM (SELECT * FROM t2) AS s)",
    );
    exec(&mut e, "ALTER TABLE t2 ADD COLUMN extra INTEGER");
    let check = |e: &mut Engine| {
        assert_eq!(q_set(e, "SELECT id FROM v8"), table(&[&["1"], &["3"]]));
        assert_eq!(q(e, "SELECT id, kk FROM v9"), table(&[&["3", "3"]]));
        assert_eq!(q_set(e, "SELECT id FROM v10"), table(&[&["1"], &["3"]]));
        assert_eq!(q_set(e, "SELECT id FROM v11"), table(&[&["1"], &["3"]]));
    };
    check(&mut e);
    let mut e = reopen(&dir, e);
    check(&mut e);
}
