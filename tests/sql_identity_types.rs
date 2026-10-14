//! Logical row identity (physical rowid vs. SQL keys), logical PRIMARY KEY /
//! FOREIGN KEY checks, identifier resolution in DML/DDL targets, and exact
//! CHAR/VARCHAR/DECIMAL assignment end to end (0.6.0 task 4).
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

fn is_duplicate(e: &DbError) -> bool {
    matches!(e, DbError::DuplicateKey(_))
}

fn is_fk_violation(e: &DbError) -> bool {
    matches!(e, DbError::ForeignKeyViolation(_))
}

// ---- row identity / PRIMARY KEY ---------------------------------------------

#[test]
fn pk_text() {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE d (id CHAR(3) PRIMARY KEY)");
    exec(&mut e, "INSERT INTO d VALUES ('a')");
    assert_eq!(q(&mut e, "SELECT id FROM d"), table(&[&["a  "]]));

    let dup = err(&mut e, "INSERT INTO d VALUES ('a')");
    assert!(is_duplicate(&dup), "{dup:?}");
    // PAD SPACE: 'a ' is the same CHAR key as 'a'.
    let dup = err(&mut e, "INSERT INTO d VALUES ('a ')");
    assert!(is_duplicate(&dup), "{dup:?}");

    exec(&mut e, "INSERT INTO d VALUES ('c')");
    exec(&mut e, "UPDATE d SET id = 'b' WHERE id = 'a'");
    assert_eq!(q_set(&mut e, "SELECT id FROM d"), table(&[&["b  "], &["c  "]]));
    let dup = err(&mut e, "UPDATE d SET id = 'b' WHERE id = 'c'");
    assert!(is_duplicate(&dup), "{dup:?}");

    exec(&mut e, "DELETE FROM d WHERE id = 'b'");
    assert_eq!(q(&mut e, "SELECT id FROM d"), table(&[&["c  "]]));

    let mut e = reopen(&dir, e);
    assert_eq!(q(&mut e, "SELECT id FROM d"), table(&[&["c  "]]));
    exec(&mut e, "INSERT INTO d VALUES ('a')");
    assert!(is_duplicate(&err(&mut e, "INSERT INTO d VALUES ('c')")));
    assert_eq!(q_set(&mut e, "SELECT id FROM d"), table(&[&["a  "], &["c  "]]));
}

#[test]
fn composite_pk_distinct() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE d (a INTEGER, b INTEGER, PRIMARY KEY (a,b))");
    exec(&mut e, "INSERT INTO d VALUES (1, 2), (1, 3)");
    assert_eq!(q_set(&mut e, "SELECT a, b FROM d"), table(&[&["1", "2"], &["1", "3"]]));

    match err(&mut e, "INSERT INTO d VALUES (1, 2)") {
        DbError::DuplicateKey(ref k) if k == "(1, 2)" => {}
        other => panic!("expected DuplicateKey((1, 2)), got {other:?}"),
    }
    assert_eq!(state(&mut e, "INSERT INTO d VALUES (1, 2)"), "23505");

    // Duplicate within one multi-row INSERT.
    let dup = err(&mut e, "INSERT INTO d VALUES (2, 1), (2, 1)");
    assert!(is_duplicate(&dup), "{dup:?}");
    // Duplicate created by UPDATE.
    let dup = err(&mut e, "UPDATE d SET b = 2 WHERE b = 3");
    assert!(is_duplicate(&dup), "{dup:?}");
    // Two rows updated onto the same key in one statement.
    exec(&mut e, "INSERT INTO d VALUES (5, 1), (6, 1)");
    let dup = err(&mut e, "UPDATE d SET a = 7 WHERE b = 1");
    assert!(is_duplicate(&dup), "{dup:?}");

    assert_eq!(
        q_set(&mut e, "SELECT a, b FROM d"),
        table(&[&["1", "2"], &["1", "3"], &["5", "1"], &["6", "1"]])
    );
}

#[test]
fn non_first_column_primary_key() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER, n INTEGER PRIMARY KEY)");
    exec(&mut e, "INSERT INTO p VALUES (1, 100)");
    let dup = err(&mut e, "INSERT INTO p VALUES (2, 100)");
    assert!(is_duplicate(&dup), "{dup:?}");
    exec(&mut e, "INSERT INTO p VALUES (1, 200)");
    assert_eq!(q_set(&mut e, "SELECT id, n FROM p"), table(&[&["1", "100"], &["1", "200"]]));
}

#[test]
fn table_without_primary_key_accepts_duplicate_rows() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE n (id INTEGER, s TEXT)");
    exec(&mut e, "INSERT INTO n VALUES (1, 'x'), (1, 'x')");
    exec(&mut e, "INSERT INTO n VALUES (1, 'x')");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM n"), table(&[&["3"]]));
    exec(&mut e, "UPDATE n SET s = 'y'");
    assert_eq!(q(&mut e, "SELECT s FROM n"), table(&[&["y"], &["y"], &["y"]]));
    exec(&mut e, "DELETE FROM n");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM n"), table(&[&["0"]]));
}

#[test]
fn rowid_allocation_survives_reopen() {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE n (s TEXT, v INTEGER)");
    exec(&mut e, "INSERT INTO n VALUES ('a', 1), ('b', 2)");
    let mut e = reopen(&dir, e);
    exec(&mut e, "INSERT INTO n VALUES ('c', 3)");
    exec(&mut e, "INSERT INTO n VALUES ('a', 1)");
    assert_eq!(
        q_set(&mut e, "SELECT s, v FROM n"),
        table(&[&["a", "1"], &["a", "1"], &["b", "2"], &["c", "3"]])
    );
    // Deleted rows' ids are not reused either.
    exec(&mut e, "DELETE FROM n WHERE s = 'c'");
    let mut e = reopen(&dir, e);
    exec(&mut e, "INSERT INTO n VALUES ('d', 4)");
    assert_eq!(q(&mut e, "SELECT COUNT(*) FROM n"), table(&[&["4"]]));
}

#[test]
fn update_shifting_integer_keys() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec(&mut e, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    exec(&mut e, "UPDATE t SET id = id + 1");
    assert_eq!(
        q_set(&mut e, "SELECT id, v FROM t"),
        table(&[&["2", "a"], &["3", "b"], &["4", "c"]])
    );
}

// ---- FOREIGN KEY -------------------------------------------------------------

fn composite_parent(e: &mut Engine, on_delete: &str) {
    exec(e, "CREATE TABLE p (a INTEGER, b INTEGER, PRIMARY KEY (a,b))");
    exec(e, "INSERT INTO p VALUES (1, 2), (3, 4)");
    exec(
        e,
        &format!(
            "CREATE TABLE ch (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, \
             FOREIGN KEY (a,b) REFERENCES p(a,b){on_delete})"
        ),
    );
}

#[test]
fn fk_composite_invalid() {
    let (_dir, mut e) = engine();
    composite_parent(&mut e, "");
    let v = err(&mut e, "INSERT INTO ch VALUES (1, 1, 999)");
    assert!(is_fk_violation(&v), "{v:?}");
    assert_eq!(v.sqlstate(), "23503");
    // Partial match on the first column only is not enough.
    let v = err(&mut e, "INSERT INTO ch VALUES (1, 1, 4)");
    assert!(is_fk_violation(&v), "{v:?}");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1, 2)");
    // MATCH SIMPLE: any NULL column exempts the row.
    exec(&mut e, "INSERT INTO ch VALUES (2, NULL, 5)");
    assert_eq!(
        q_set(&mut e, "SELECT id, a, b FROM ch"),
        table(&[&["1", "1", "2"], &["2", "NULL", "5"]])
    );
    // UPDATE of the child is checked the same way.
    let v = err(&mut e, "UPDATE ch SET b = 999 WHERE id = 1");
    assert!(is_fk_violation(&v), "{v:?}");
    exec(&mut e, "UPDATE ch SET a = 3, b = 4 WHERE id = 1");
}

#[test]
fn fk_nullable() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id))");
    exec(&mut e, "INSERT INTO p VALUES (1)");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1)");
    exec(&mut e, "INSERT INTO ch VALUES (2, NULL)");
    assert_eq!(q_set(&mut e, "SELECT pid FROM ch"), table(&[&["1"], &["NULL"]]));
    exec(&mut e, "UPDATE ch SET pid = NULL WHERE id = 1");
    // Parent row is no longer referenced once the child is NULL.
    exec(&mut e, "DELETE FROM p WHERE id = 1");
}

#[test]
fn fk_nonphysical_parent_key() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER, n INTEGER PRIMARY KEY)");
    exec(&mut e, "INSERT INTO p VALUES (1, 100)");
    exec(&mut e, "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(n))");
    exec(&mut e, "INSERT INTO ch VALUES (1, 100)");
    assert_eq!(q(&mut e, "SELECT pid FROM ch"), table(&[&["100"]]));
    let v = err(&mut e, "INSERT INTO ch VALUES (2, 999)");
    assert!(is_fk_violation(&v), "{v:?}");
    // id = 1 is not the referenced key: it must not satisfy the reference.
    let v = err(&mut e, "INSERT INTO ch VALUES (3, 1)");
    assert!(is_fk_violation(&v), "{v:?}");
}

#[test]
fn delete_parent_with_composite_child_is_rejected() {
    let (_dir, mut e) = engine();
    composite_parent(&mut e, "");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1, 2)");
    let v = err(&mut e, "DELETE FROM p WHERE a = 1");
    assert!(is_fk_violation(&v), "{v:?}");
    // (3,4) is not referenced (no child matches on all columns).
    exec(&mut e, "INSERT INTO ch VALUES (2, 3, NULL)");
    exec(&mut e, "DELETE FROM p WHERE a = 3");
    assert_eq!(q(&mut e, "SELECT a, b FROM p"), table(&[&["1", "2"]]));
}

#[test]
fn restrict_and_no_action_reject_referenced_key_update() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY, s TEXT)");
    exec(&mut e, "CREATE TABLE c1 (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id) ON UPDATE RESTRICT)");
    exec(&mut e, "CREATE TABLE c2 (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id) ON UPDATE CASCADE)");
    exec(&mut e, "INSERT INTO p VALUES (1, 'x'), (2, 'y')");
    exec(&mut e, "INSERT INTO c1 VALUES (1, 1)");
    exec(&mut e, "INSERT INTO c2 VALUES (1, 2)");
    let v = err(&mut e, "UPDATE p SET id = 10 WHERE id = 1");
    assert!(is_fk_violation(&v), "{v:?}");
    // Updates that keep the referenced key are fine, also under ON UPDATE CASCADE.
    exec(&mut e, "UPDATE p SET s = 'z'");
    assert_eq!(q_set(&mut e, "SELECT id, s FROM p"), table(&[&["1", "z"], &["2", "z"]]));
}

#[test]
fn on_delete_cascade_with_composite_key() {
    let (_dir, mut e) = engine();
    composite_parent(&mut e, " ON DELETE CASCADE");
    exec(&mut e, "CREATE TABLE gc (id INTEGER PRIMARY KEY, cid INTEGER, FOREIGN KEY (cid) REFERENCES ch(id) ON DELETE CASCADE)");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1, 2), (2, 1, 2), (3, 3, 4), (4, 1, NULL)");
    exec(&mut e, "INSERT INTO gc VALUES (10, 1), (11, 3)");
    exec(&mut e, "DELETE FROM p WHERE a = 1");
    assert_eq!(q_set(&mut e, "SELECT id FROM ch"), table(&[&["3"], &["4"]]));
    assert_eq!(q(&mut e, "SELECT id FROM gc"), table(&[&["11"]]));
    assert_eq!(q(&mut e, "SELECT a, b FROM p"), table(&[&["3", "4"]]));
}

#[test]
fn text_fk_matches_char_pad_space() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (code CHAR(3) PRIMARY KEY)");
    exec(&mut e, "INSERT INTO p VALUES ('a')");
    exec(&mut e, "CREATE TABLE ct (id INTEGER PRIMARY KEY, code TEXT, FOREIGN KEY (code) REFERENCES p(code))");
    exec(&mut e, "CREATE TABLE cc (id INTEGER PRIMARY KEY, code CHAR(5), FOREIGN KEY (code) REFERENCES p(code))");
    exec(&mut e, "INSERT INTO ct VALUES (1, 'a')");
    exec(&mut e, "INSERT INTO cc VALUES (1, 'a')");
    assert!(is_fk_violation(&err(&mut e, "INSERT INTO ct VALUES (2, 'b')")));
    assert!(is_fk_violation(&err(&mut e, "DELETE FROM p")));
}

#[test]
fn fk_must_reference_primary_key() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY, other INTEGER)");
    let v = err(&mut e, "CREATE TABLE c (id INTEGER, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(other))");
    assert!(matches!(v, DbError::InvalidForeignKey(_)), "{v:?}");
    assert_eq!(v.sqlstate(), "42830");
    exec(&mut e, "CREATE TABLE p2 (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");
    // A subset of a composite key is not a key either.
    assert_eq!(state(&mut e, "CREATE TABLE c (id INTEGER, x INTEGER, FOREIGN KEY (x) REFERENCES p2(a))"), "42830");
    // A failed CREATE TABLE leaves no table behind.
    assert_eq!(state(&mut e, "SELECT * FROM c"), "42P01");
}

#[test]
fn fk_arity_mismatch() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p2 (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");
    let v = err(&mut e, "CREATE TABLE c (id INTEGER, x INTEGER, FOREIGN KEY (x) REFERENCES p2(a, b))");
    assert!(matches!(v, DbError::InvalidForeignKey(_)), "{v:?}");
    assert_eq!(v.sqlstate(), "42830");
}

#[test]
fn fk_type_mismatch() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    assert_eq!(state(&mut e, "CREATE TABLE c (id INTEGER, pid TEXT, FOREIGN KEY (pid) REFERENCES p(id))"), "42804");
    exec(&mut e, "CREATE TABLE c (id INTEGER, pid DECIMAL(5,0), FOREIGN KEY (pid) REFERENCES p(id))");
    exec(&mut e, "INSERT INTO p VALUES (7)");
    exec(&mut e, "INSERT INTO c VALUES (1, 7)");
    assert!(is_fk_violation(&err(&mut e, "INSERT INTO c VALUES (2, 8)")));
}

#[test]
fn self_referencing_fk() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE emp (id INTEGER PRIMARY KEY, boss INTEGER, FOREIGN KEY (boss) REFERENCES emp(id))");
    exec(&mut e, "INSERT INTO emp VALUES (1, NULL)");
    exec(&mut e, "INSERT INTO emp VALUES (2, 1)");
    exec(&mut e, "INSERT INTO emp VALUES (3, 3)");
    assert!(is_fk_violation(&err(&mut e, "INSERT INTO emp VALUES (4, 9)")));
    assert!(is_fk_violation(&err(&mut e, "DELETE FROM emp WHERE id = 1")));
    // Deleting the referencing row together with its parent is fine.
    exec(&mut e, "DELETE FROM emp WHERE id <= 2");
    assert_eq!(q(&mut e, "SELECT id FROM emp"), table(&[&["3"]]));
}

#[test]
fn same_tx_child_insert_then_parent_delete_is_rejected() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE p (id INTEGER PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id))");
    exec(&mut e, "INSERT INTO p VALUES (1)");
    exec(&mut e, "BEGIN");
    exec(&mut e, "INSERT INTO ch VALUES (1, 1)");
    let v = err(&mut e, "DELETE FROM p WHERE id = 1");
    assert!(is_fk_violation(&v), "{v:?}");
    exec(&mut e, "ROLLBACK");
    assert_eq!(q(&mut e, "SELECT id FROM p"), table(&[&["1"]]));
    assert!(q(&mut e, "SELECT id FROM ch").is_empty());
}

#[test]
fn commit_recheck_rejects_child_of_concurrently_deleted_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(db_path(&dir).to_str().unwrap()).unwrap();
    let setup = db.begin().unwrap();
    for sql in [
        "CREATE TABLE p (a INTEGER, b INTEGER, PRIMARY KEY (a, b))",
        "CREATE TABLE ch (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES p(a, b))",
        "INSERT INTO p VALUES (1, 2)",
    ] {
        db.query(&setup, sql).unwrap();
    }
    db.commit(setup).unwrap();

    let t1 = db.begin().unwrap();
    let t2 = db.begin().unwrap();
    db.query(&t1, "INSERT INTO ch VALUES (1, 1, 2)").unwrap();
    // t2 cannot see t1's uncommitted child, so its statement-time check passes.
    db.query(&t2, "DELETE FROM p WHERE a = 1").unwrap();
    db.commit(t2).unwrap();
    match db.commit(t1) {
        Err(DbError::ForeignKeyViolation(_)) => {}
        other => panic!("expected ForeignKeyViolation at COMMIT, got {other:?}"),
    }

    let check = db.begin().unwrap();
    assert!(db.query(&check, "SELECT id FROM ch").unwrap().rows.is_empty());
    assert!(db.query(&check, "SELECT a FROM p").unwrap().rows.is_empty());
    db.commit(check).unwrap();
}

#[test]
fn commit_recheck_rejects_concurrent_duplicate_text_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(db_path(&dir).to_str().unwrap()).unwrap();
    let setup = db.begin().unwrap();
    db.query(&setup, "CREATE TABLE d (id CHAR(3) PRIMARY KEY, v INTEGER)").unwrap();
    db.commit(setup).unwrap();

    let t1 = db.begin().unwrap();
    let t2 = db.begin().unwrap();
    db.query(&t1, "INSERT INTO d VALUES ('k', 1)").unwrap();
    db.query(&t2, "INSERT INTO d VALUES ('k', 2)").unwrap();
    db.commit(t1).unwrap();
    match db.commit(t2) {
        Err(DbError::DuplicateKey(_)) => {}
        other => panic!("expected DuplicateKey at COMMIT, got {other:?}"),
    }
    let check = db.begin().unwrap();
    let r = db.query(&check, "SELECT v FROM d").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string(), "1");
    db.commit(check).unwrap();
}

// ---- exact types --------------------------------------------------------------

#[test]
fn decimal_rounding_on_insert_and_update() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE m (id INTEGER PRIMARY KEY, n DECIMAL(10,2))");
    exec(&mut e, "INSERT INTO m VALUES (1, 1.005)");
    assert_eq!(q(&mut e, "SELECT n FROM m"), table(&[&["1.01"]]));
    exec(&mut e, "UPDATE m SET n = 2.675");
    assert_eq!(q(&mut e, "SELECT n FROM m"), table(&[&["2.68"]]));
    exec(&mut e, "UPDATE m SET n = -2.675");
    assert_eq!(q(&mut e, "SELECT n FROM m"), table(&[&["-2.68"]]));
    exec(&mut e, "INSERT INTO m VALUES (2, '3.335')");
    assert_eq!(q(&mut e, "SELECT n FROM m WHERE id = 2"), table(&[&["3.34"]]));
}

#[test]
fn decimal_overflow_is_22003() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE m (id INTEGER PRIMARY KEY, n DECIMAL(10,2))");
    let v = err(&mut e, "INSERT INTO m VALUES (1, 999999999.995)");
    assert!(matches!(v, DbError::NumericOverflow(_)), "{v:?}");
    exec(&mut e, "INSERT INTO m VALUES (1, 99999999.99)");
    let v = err(&mut e, "UPDATE m SET n = 999999999.995");
    assert!(matches!(v, DbError::NumericOverflow(_)), "{v:?}");
    assert_eq!(state(&mut e, "UPDATE m SET n = n * 100"), "22003");
    assert_eq!(q(&mut e, "SELECT n FROM m"), table(&[&["99999999.99"]]));
}

#[test]
fn decimal_expression_assignment_is_exact() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE m (id INTEGER PRIMARY KEY, n DECIMAL(5,2))");
    exec(&mut e, "INSERT INTO m VALUES (1, 0.1 + 0.2)");
    assert_eq!(q(&mut e, "SELECT n FROM m"), table(&[&["0.30"]]));
    assert_eq!(q(&mut e, "SELECT id FROM m WHERE n = 0.3"), table(&[&["1"]]));
    exec(&mut e, "UPDATE m SET n = n + 0.1 + 0.2");
    assert_eq!(q(&mut e, "SELECT n FROM m"), table(&[&["0.60"]]));
}

#[test]
fn varchar_length_counts_characters() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE s (id INTEGER PRIMARY KEY, v VARCHAR(3))");
    let v = err(&mut e, "INSERT INTO s VALUES (1, 'abcd')");
    assert!(matches!(v, DbError::StringTooLong(_)), "{v:?}");
    assert_eq!(v.sqlstate(), "22001");
    exec(&mut e, "INSERT INTO s VALUES (1, 'äöü')");
    assert_eq!(q(&mut e, "SELECT v FROM s"), table(&[&["äöü"]]));
    assert_eq!(state(&mut e, "UPDATE s SET v = 'abcd'"), "22001");
    exec(&mut e, "UPDATE s SET v = 'xyz'");
    assert_eq!(q(&mut e, "SELECT v FROM s"), table(&[&["xyz"]]));
}

#[test]
fn char_multibyte_is_padded_by_characters() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE s (id INTEGER PRIMARY KEY, c CHAR(3))");
    exec(&mut e, "INSERT INTO s VALUES (1, 'äö')");
    assert_eq!(q(&mut e, "SELECT c FROM s"), table(&[&["äö "]]));
    assert_eq!(state(&mut e, "INSERT INTO s VALUES (2, 'äöüx')"), "22001");
    assert_eq!(state(&mut e, "UPDATE s SET c = 'äöüx'"), "22001");
    exec(&mut e, "UPDATE s SET c = 'ü'");
    assert_eq!(q(&mut e, "SELECT c FROM s"), table(&[&["ü  "]]));
}

#[test]
fn decimal_survives_reopen() {
    let (dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE m (id INTEGER PRIMARY KEY, n DECIMAL(10,2))");
    exec(&mut e, "INSERT INTO m VALUES (1, 12345678.9), (2, -0.05)");
    let mut e = reopen(&dir, e);
    assert_eq!(
        q_set(&mut e, "SELECT id, n FROM m"),
        table(&[&["1", "12345678.90"], &["2", "-0.05"]])
    );
    assert_eq!(q(&mut e, "SELECT SUM(n) FROM m"), table(&[&["12345678.85"]]));
}

#[test]
fn assignment_conversion_error_classes() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE k (id INTEGER PRIMARY KEY, i INTEGER, sm SMALLINT, d DATE)");
    exec(&mut e, "INSERT INTO k VALUES (1, 1, 1, '2024-01-01')");
    assert_eq!(state(&mut e, "INSERT INTO k VALUES (2, 'x', 1, NULL)"), "22018");
    assert_eq!(state(&mut e, "INSERT INTO k VALUES (2, 3000000000, 1, NULL)"), "22003");
    assert_eq!(state(&mut e, "INSERT INTO k VALUES (2, 1, 70000, NULL)"), "22003");
    assert_eq!(state(&mut e, "UPDATE k SET sm = 70000"), "22003");
    assert_eq!(state(&mut e, "UPDATE k SET i = 'x'"), "22018");
    let v = err(&mut e, "INSERT INTO k VALUES (2, 1, 1, '2024-13-45')");
    assert!(v.to_string().contains("not a valid DATE"), "{v}");
}

// ---- identifier resolution in DML/DDL targets -----------------------------------

#[test]
fn dml_targets_resolve_identifiers_like_column_references() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)");
    exec(&mut e, "INSERT INTO PEOPLE (ID, Name) VALUES (1, 'a')");
    exec(&mut e, "INSERT INTO \"people\" (\"id\", \"name\") VALUES (2, 'b')");
    exec(&mut e, "UPDATE People SET NAME = 'c' WHERE id = 2");
    assert_eq!(q_set(&mut e, "SELECT id, name FROM PEOPLE"), table(&[&["1", "a"], &["2", "c"]]));
    assert_eq!(q(&mut e, "SELECT People.name FROM people WHERE id = 1"), table(&[&["a"]]));
    exec(&mut e, "CREATE INDEX people_name ON PEOPLE (NAME)");
    assert_eq!(q(&mut e, "SELECT id FROM people WHERE name = 'c'"), table(&[&["2"]]));
    exec(&mut e, "DELETE FROM PeOpLe WHERE id = 1");
    assert_eq!(q(&mut e, "SELECT id FROM people"), table(&[&["2"]]));

    // Quoted identifiers match exactly.
    assert_eq!(state(&mut e, "INSERT INTO \"PEOPLE\" VALUES (3, 'x')"), "42P01");
    assert_eq!(state(&mut e, "SELECT id FROM \"People\""), "42P01");
    assert_eq!(state(&mut e, "UPDATE \"PEOPLE\" SET name = 'x'"), "42P01");
    assert_eq!(state(&mut e, "DELETE FROM \"PEOPLE\""), "42P01");
    assert_eq!(state(&mut e, "INSERT INTO people (\"ID\") VALUES (3)"), "42703");
    assert_eq!(state(&mut e, "UPDATE people SET \"Name\" = 'x'"), "42703");
    assert_eq!(state(&mut e, "CREATE INDEX bad ON people (\"NAME\")"), "42703");
}

#[test]
fn duplicate_targets_use_the_identifier_rule() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)");
    assert_eq!(state(&mut e, "INSERT INTO people (id, ID) VALUES (1, 2)"), "42601");
    assert_eq!(state(&mut e, "INSERT INTO people (name, \"name\") VALUES ('a', 'b')"), "42601");
    assert_eq!(state(&mut e, "UPDATE people SET name = 'a', NAME = 'b'"), "42601");
}

#[test]
fn constraint_column_names_resolve_identifiers() {
    let (_dir, mut e) = engine();
    exec(&mut e, "CREATE TABLE Parent (Id INTEGER, PRIMARY KEY (ID))");
    exec(&mut e, "CREATE TABLE child (id INTEGER PRIMARY KEY, Pid INTEGER, FOREIGN KEY (PID) REFERENCES PARENT(id))");
    exec(&mut e, "INSERT INTO parent VALUES (1)");
    exec(&mut e, "INSERT INTO child VALUES (1, 1)");
    assert!(is_fk_violation(&err(&mut e, "INSERT INTO child VALUES (2, 2)")));
    assert!(is_duplicate(&err(&mut e, "INSERT INTO parent VALUES (1)")));
    assert_eq!(
        state(&mut e, "CREATE TABLE c2 (id INTEGER, pid INTEGER, FOREIGN KEY (\"PID\") REFERENCES parent(id))"),
        "42703"
    );
    assert_eq!(
        state(&mut e, "CREATE TABLE c2 (id INTEGER, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent(\"ID\"))"),
        "42703"
    );
    assert_eq!(
        state(&mut e, "CREATE TABLE c2 (id INTEGER, pid INTEGER, FOREIGN KEY (pid) REFERENCES \"PARENT\"(id))"),
        "42P01"
    );
}
