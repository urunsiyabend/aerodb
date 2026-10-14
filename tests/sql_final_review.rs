//! Regressions from the 0.6.0 final whole-branch review: secondary index
//! buckets (hash collisions, NULL, dead versions), the identifier rule for
//! relation / index / sequence names, the REPL line handler, AUTO_INCREMENT
//! sequences, and CASE result conversion.
mod common;

use aerodb::catalog::Catalog;
use aerodb::engine::Engine;
use aerodb::storage::row::ColumnValue;
use common::{engine, err, exec, rows, sorted, table};
use std::collections::HashMap;
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

fn reopen(dir: &TempDir, e: Engine) -> Engine {
    drop(e);
    Engine::new(dir.path().join("test.db").to_str().unwrap())
}

/// Two distinct strings whose index hash is equal (found by search, so the
/// test does not depend on a hard-coded hasher output).
fn colliding_texts() -> (String, String) {
    let mut seen: HashMap<i32, String> = HashMap::new();
    for i in 0.. {
        let s = format!("k{i}");
        let h = Catalog::hash_value(&ColumnValue::Text(s.clone()));
        if let Some(prev) = seen.insert(h, s.clone()) {
            return (prev, s);
        }
    }
    unreachable!()
}

// ---- C1: index buckets ------------------------------------------------------------

#[test]
fn create_index_over_null_and_zero() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE ix(id INTEGER PRIMARY KEY, n INTEGER)");
    exec(&mut e, "INSERT INTO ix VALUES (1, NULL), (2, 0)");
    exec(&mut e, "CREATE INDEX ixn ON ix(n)");
    assert_eq!(q(&mut e, "SELECT id FROM ix WHERE n = 0"), table(&[&["2"]]));
    assert_eq!(q(&mut e, "SELECT id FROM ix WHERE n IS NULL"), table(&[&["1"]]));
}

#[test]
fn indexed_null_after_zero_insert_and_update() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE ix(id INTEGER PRIMARY KEY, n INTEGER)");
    exec(&mut e, "CREATE INDEX ixn ON ix(n)");
    exec(&mut e, "INSERT INTO ix VALUES (1, 0)");
    exec(&mut e, "INSERT INTO ix VALUES (2, NULL)");
    exec(&mut e, "INSERT INTO ix VALUES (3, 7)");
    exec(&mut e, "UPDATE ix SET n = NULL WHERE id = 3");
    assert_eq!(q(&mut e, "SELECT id FROM ix WHERE n = 0"), table(&[&["1"]]));
    assert_eq!(q_set(&mut e, "SELECT id FROM ix WHERE n IS NULL"), table(&[&["2"], &["3"]]));
    assert_eq!(q(&mut e, "SELECT id FROM ix WHERE n = 7"), table(&[]));
}

#[test]
fn indexed_double_values_with_equal_hash() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE d(id INTEGER PRIMARY KEY, x DOUBLE)");
    exec(&mut e, "CREATE INDEX dx ON d(x)");
    exec(&mut e, "INSERT INTO d VALUES (1, 1.5)");
    exec(&mut e, "INSERT INTO d VALUES (2, 1.7)");
    exec(&mut e, "INSERT INTO d VALUES (3, 1.5)");
    assert_eq!(q_set(&mut e, "SELECT id FROM d WHERE x = 1.5"), table(&[&["1"], &["3"]]));
    assert_eq!(q(&mut e, "SELECT id FROM d WHERE x = 1.7"), table(&[&["2"]]));
    assert_eq!(q(&mut e, "SELECT id FROM d WHERE x = 1.6"), table(&[]));
    exec(&mut e, "UPDATE d SET x = 1.7 WHERE id = 3");
    assert_eq!(q_set(&mut e, "SELECT id FROM d WHERE x = 1.7"), table(&[&["2"], &["3"]]));
    assert_eq!(q(&mut e, "SELECT id FROM d WHERE x = 1.5"), table(&[&["1"]]));
}

#[test]
fn indexed_text_values_with_colliding_hash() {
    let (a, b) = colliding_texts();
    assert_ne!(a, b);
    assert_eq!(
        Catalog::hash_value(&ColumnValue::Text(a.clone())),
        Catalog::hash_value(&ColumnValue::Text(b.clone()))
    );
    // Index built over existing rows and maintained by later writes.
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE t(id INTEGER PRIMARY KEY, s TEXT)");
    exec(&mut e, &format!("INSERT INTO t VALUES (1, '{a}'), (2, '{b}')"));
    exec(&mut e, "CREATE INDEX ts ON t(s)");
    exec(&mut e, &format!("INSERT INTO t VALUES (3, '{b}')"));
    exec(&mut e, &format!("INSERT INTO t VALUES (4, '{a}')"));
    assert_eq!(q_set(&mut e, &format!("SELECT id FROM t WHERE s = '{a}'")), table(&[&["1"], &["4"]]));
    assert_eq!(q_set(&mut e, &format!("SELECT id FROM t WHERE s = '{b}'")), table(&[&["2"], &["3"]]));
}

#[test]
fn create_index_after_updates_and_rollback_and_reopen() {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE f(id INTEGER PRIMARY KEY, d DOUBLE, n INTEGER)");
    exec(&mut e, "INSERT INTO f VALUES (1, 1.5, 0), (2, 2.5, 5)");
    // Committed update: the old version (1.5) stays physically present.
    exec(&mut e, "UPDATE f SET d = 1.7 WHERE id = 1");
    // Rolled-back update: an aborted version (NULL) stays physically present.
    exec(&mut e, "BEGIN");
    exec(&mut e, "UPDATE f SET n = NULL WHERE id = 1");
    exec(&mut e, "ROLLBACK");
    exec(&mut e, "CREATE INDEX fd ON f(d)");
    exec(&mut e, "CREATE INDEX fn ON f(n)");
    assert_eq!(q(&mut e, "SELECT id FROM f WHERE d = 1.7"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT id FROM f WHERE d = 1.5"), table(&[]));
    assert_eq!(q(&mut e, "SELECT id FROM f WHERE n = 0"), table(&[&["1"]]));
    // Indexes are not persisted: after reopen they can be created again.
    let mut e = reopen(&dir, e);
    exec(&mut e, "CREATE INDEX fd ON f(d)");
    exec(&mut e, "CREATE INDEX fn ON f(n)");
    assert_eq!(q(&mut e, "SELECT id FROM f WHERE d = 1.7"), table(&[&["1"]]));
    assert_eq!(q(&mut e, "SELECT id FROM f WHERE n = 0"), table(&[&["1"]]));
    exec(&mut e, "INSERT INTO f VALUES (3, 1.5, NULL)");
    assert_eq!(q(&mut e, "SELECT id FROM f WHERE d = 1.5"), table(&[&["3"]]));
}

// ---- I1: identifier rule for relation / index / sequence names ------------------

#[test]
fn create_table_and_view_names_collide_case_insensitively() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE o(id INTEGER)");
    assert_eq!(state(&mut e, "CREATE TABLE O(id INTEGER)"), "42P07");
    exec(&mut e, "CREATE TABLE IF NOT EXISTS O(id INTEGER)");
    assert_eq!(state(&mut e, "CREATE VIEW O AS SELECT 1 AS x"), "42P07");
    exec(&mut e, "INSERT INTO o VALUES (1)");
    assert_eq!(q(&mut e, "SELECT id FROM O"), table(&[&["1"]]));
    exec(&mut e, "CREATE VIEW oo AS SELECT id FROM o");
    assert_eq!(state(&mut e, "CREATE TABLE OO(id INTEGER)"), "42P07");
    assert_eq!(state(&mut e, "CREATE VIEW Oo AS SELECT 2 AS y"), "42P07");
    assert_eq!(q(&mut e, "SELECT id FROM oo"), table(&[&["1"]]));
}

#[test]
fn quoted_names_differing_in_case_are_distinct_and_unquoted_use_is_ambiguous() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE \"Qt\"(id INTEGER)");
    exec(&mut e, "CREATE TABLE \"qt\"(id INTEGER)");
    assert_eq!(state(&mut e, "CREATE TABLE \"Qt\"(id INTEGER)"), "42P07");
    exec(&mut e, "INSERT INTO \"qt\" VALUES (2)");
    assert_eq!(q(&mut e, "SELECT id FROM \"qt\""), table(&[&["2"]]));
    let amb = err(&mut e, "SELECT id FROM qt");
    assert_eq!(amb.sqlstate(), "42702", "{amb}");
    assert_eq!(state(&mut e, "DROP TABLE qt"), "42702");
    exec(&mut e, "DROP TABLE \"qt\"");
    assert_eq!(q(&mut e, "SELECT id FROM qt"), table(&[]));
}

#[test]
fn drop_table_and_view_use_the_identifier_rule() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE VIEW Vx AS SELECT 1 AS a");
    exec(&mut e, "DROP VIEW vx");
    assert_eq!(state(&mut e, "SELECT a FROM Vx"), "42P01");
    exec(&mut e, "CREATE TABLE Tc(id INTEGER)");
    assert_eq!(state(&mut e, "DROP VIEW tc"), "42809");
    exec(&mut e, "CREATE VIEW Vt AS SELECT id FROM tc");
    assert_eq!(state(&mut e, "DROP TABLE vt"), "42809");
    assert_eq!(state(&mut e, "DROP TABLE TC"), "2BP01");
    exec(&mut e, "DROP VIEW VT");
    exec(&mut e, "DROP TABLE TC");
    assert_eq!(state(&mut e, "SELECT id FROM tc"), "42P01");
    exec(&mut e, "DROP TABLE IF EXISTS tc");
    exec(&mut e, "DROP VIEW IF EXISTS vt");
    assert_eq!(state(&mut e, "DROP TABLE tc"), "42P01");
    // Parent referenced with another spelling is still a dependent.
    exec(&mut e, "CREATE TABLE Par(id INTEGER PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE ch(id INTEGER PRIMARY KEY, p INTEGER REFERENCES PAR(id))");
    assert_eq!(state(&mut e, "DROP TABLE par"), "2BP01");
}

#[test]
fn index_names_follow_the_identifier_rule() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE o(id INTEGER, v INTEGER)");
    exec(&mut e, "CREATE INDEX ix ON o(v)");
    let dup = err(&mut e, "CREATE INDEX IX ON o(v)");
    assert_eq!(dup.sqlstate(), "42P07", "{dup}");
    exec(&mut e, "DROP INDEX Ix");
    let missing = err(&mut e, "DROP INDEX IX");
    assert_eq!(missing.sqlstate(), "42704");
    let msg = missing.to_string();
    assert_eq!(msg.matches("not found").count(), 1, "{msg}");
    exec(&mut e, "CREATE INDEX IX ON o(v)");
}

#[test]
fn sequence_names_follow_the_identifier_rule() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE SEQUENCE s1");
    assert_eq!(state(&mut e, "CREATE SEQUENCE S1"), "42P07");
}

// ---- AUTO_INCREMENT sequences ----------------------------------------------------------

#[test]
fn drop_table_drops_its_auto_increment_sequence() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE ai(id INTEGER NOT NULL AUTO_INCREMENT, v TEXT)");
    exec(&mut e, "INSERT INTO ai (v) VALUES ('a')");
    exec(&mut e, "INSERT INTO ai (v) VALUES ('b')");
    exec(&mut e, "DROP TABLE ai");
    exec(&mut e, "CREATE TABLE ai(id INTEGER NOT NULL AUTO_INCREMENT, v TEXT)");
    exec(&mut e, "INSERT INTO ai (v) VALUES ('c')");
    assert_eq!(q(&mut e, "SELECT id, v FROM ai"), table(&[&["1", "c"]]));
}

#[test]
fn auto_increment_sequence_names_do_not_collide() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE a_b(c INTEGER NOT NULL AUTO_INCREMENT, v TEXT)");
    exec(&mut e, "CREATE TABLE a(b_c INTEGER NOT NULL AUTO_INCREMENT, v TEXT)");
    exec(&mut e, "INSERT INTO a_b (v) VALUES ('x')");
    exec(&mut e, "INSERT INTO a_b (v) VALUES ('y')");
    exec(&mut e, "INSERT INTO a (v) VALUES ('z')");
    assert_eq!(q(&mut e, "SELECT b_c FROM a"), table(&[&["1"]]));
    assert_eq!(q_set(&mut e, "SELECT c FROM a_b"), table(&[&["1"], &["2"]]));
}

#[test]
fn legacy_auto_increment_sequence_is_still_used_and_dropped() {
    use aerodb::execution::handle_statement;
    use aerodb::sql::parser::parse_sql;
    use aerodb::storage::pager::Pager;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    let mut catalog = Catalog::open(Pager::new(path.to_str().unwrap()).unwrap()).unwrap();
    // A 0.5.1 catalog: AUTO_INCREMENT column with its `{table}_{column}`
    // sequence, already advanced to 41.
    catalog
        .create_table_with_fks(
            "leg",
            vec![
                ("id".into(), aerodb::storage::row::ColumnType::Integer, true, None, true),
                ("v".into(), aerodb::storage::row::ColumnType::Text, false, None, false),
            ],
            Vec::new(),
            None,
        )
        .unwrap();
    catalog.create_sequence("leg_id", 1, 1).unwrap();
    catalog.update_sequence_current("leg_id", 41).unwrap();
    let mut run = |sql: &str| handle_statement(&mut catalog, parse_sql(sql).unwrap());
    run("INSERT INTO leg (v) VALUES ('x')").unwrap();
    run("DROP TABLE leg").unwrap();
    drop(run);
    assert!(catalog.next_sequence_value("leg_id").is_err(), "legacy sequence dropped");
    let mut e = Engine::open(path.to_str().unwrap()).unwrap();
    exec(&mut e, "CREATE TABLE leg(id INTEGER NOT NULL AUTO_INCREMENT, v TEXT)");
    exec(&mut e, "INSERT INTO leg (v) VALUES ('y')");
    assert_eq!(q(&mut e, "SELECT id FROM leg"), table(&[&["1"]]));
}

#[test]
fn legacy_auto_increment_sequence_values_continue() {
    use aerodb::execution::handle_statement;
    use aerodb::sql::parser::parse_sql;
    use aerodb::storage::pager::Pager;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    let mut catalog = Catalog::open(Pager::new(path.to_str().unwrap()).unwrap()).unwrap();
    catalog
        .create_table_with_fks(
            "leg",
            vec![
                ("id".into(), aerodb::storage::row::ColumnType::Integer, true, None, true),
                ("v".into(), aerodb::storage::row::ColumnType::Text, false, None, false),
            ],
            Vec::new(),
            None,
        )
        .unwrap();
    catalog.create_sequence("leg_id", 1, 1).unwrap();
    catalog.update_sequence_current("leg_id", 41).unwrap();
    handle_statement(&mut catalog, parse_sql("INSERT INTO leg (v) VALUES ('x')").unwrap()).unwrap();
    let r = aerodb::execution::query::execute_query(
        &mut catalog,
        &match parse_sql("SELECT id FROM leg").unwrap() {
            aerodb::sql::ast::Statement::Query(q) => *q,
            _ => unreachable!(),
        },
        None,
    )
    .unwrap();
    assert_eq!(r.rows[0][0].to_string(), "42");
}

// ---- CASE result conversion ------------------------------------------------------------

#[test]
fn case_results_take_the_unified_type() {
    use aerodb::sql::value::Value;
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE e(id INTEGER PRIMARY KEY, sal DECIMAL(8,2), n INTEGER, x DOUBLE)");
    exec(&mut e, "INSERT INTO e VALUES (1, 100, 1, 0.5), (2, 200, 2, 0.5), (3, 50, 3, 0.5), (4, 75, 4, 0.5)");
    fn kinds(e: &mut Engine, sql: &str) -> Vec<(String, &'static str)> {
        let r = e.query(sql).unwrap_or_else(|m| panic!("`{sql}`: {m}"));
        r.rows
            .iter()
            .map(|row| {
                let kind = match row[0] {
                    Value::Int(_) => "int",
                    Value::Decimal(_) => "decimal",
                    Value::Double(_) => "double",
                    _ => "other",
                };
                (row[0].to_string(), kind)
            })
            .collect()
    }
    fn dec(v: &[&str]) -> Vec<(String, &'static str)> {
        v.iter().map(|s| (s.to_string(), "decimal")).collect()
    }
    // Integer alternatives become DECIMAL (scale 0, as in PostgreSQL).
    assert_eq!(
        kinds(&mut e, "SELECT CASE WHEN id > 2 THEN sal ELSE 1 END FROM e ORDER BY id"),
        dec(&["1", "1", "50.00", "75.00"])
    );
    assert_eq!(
        kinds(&mut e, "SELECT CASE WHEN id > 2 THEN n ELSE sal END FROM e ORDER BY id"),
        dec(&["100.00", "200.00", "3", "4"])
    );
    assert_eq!(
        kinds(&mut e, "SELECT CASE WHEN id > 2 THEN 1 ELSE sal END FROM e ORDER BY id"),
        dec(&["100.00", "200.00", "1", "1"])
    );
    // DOUBLE wins over INTEGER and DECIMAL.
    let mixed = kinds(
        &mut e,
        "SELECT CASE WHEN id = 1 THEN n WHEN id = 2 THEN sal ELSE x END FROM e ORDER BY id",
    );
    assert!(mixed.iter().all(|(_, k)| *k == "double"), "{mixed:?}");
    // COALESCE results follow the same rule.
    assert_eq!(kinds(&mut e, "SELECT COALESCE(n, sal) FROM e WHERE id = 1"), dec(&["1"]));
    // Integer-only CASE is unchanged.
    let ints = kinds(&mut e, "SELECT CASE WHEN id > 2 THEN n ELSE 0 END FROM e ORDER BY id");
    assert_eq!(
        ints,
        vec![("0".into(), "int"), ("0".into(), "int"), ("3".into(), "int"), ("4".into(), "int")]
    );
}
