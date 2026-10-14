//! information_schema virtual views (F021 subset), the SQLSTATE surface
//! (E171) and commit-time error classes (0.6.0 task 7b).
mod common;

use aerodb::db::Database;
use aerodb::engine::Engine;
use aerodb::error::DbError;
use common::{engine, err, exec, rows, table};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn q(e: &mut Engine, sql: &str) -> Vec<Vec<String>> {
    rows(e, sql).unwrap_or_else(|m| panic!("`{sql}` failed: {m}"))
}

fn state(e: &mut Engine, sql: &str) -> &'static str {
    err(e, sql).sqlstate()
}

fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("test.db")
}

fn reopen(dir: &TempDir, e: Engine) -> Engine {
    drop(e);
    Engine::new(db_path(dir).to_str().unwrap())
}

fn stmt(sql: &str) -> aerodb::sql::ast::Statement {
    aerodb::sql::parser::parse_sql(sql).unwrap()
}

/// The audit probe: one base table `t`.
#[test]
fn audit_probe_lists_the_base_table() {
    let (_d, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    assert_eq!(
        q(&mut e, "SELECT table_name FROM information_schema.tables"),
        table(&[&["t"]])
    );
}

#[test]
fn tables_lists_base_tables_and_views_in_name_order() {
    let (_d, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE b (x INTEGER)");
    exec(&mut e, "CREATE VIEW v AS SELECT id FROM t WHERE id > 1");
    let expected = table(&[
        &["aerodb", "public", "b", "BASE TABLE"],
        &["aerodb", "public", "t", "BASE TABLE"],
        &["aerodb", "public", "v", "VIEW"],
    ]);
    assert_eq!(
        q(&mut e, "SELECT * FROM information_schema.tables"),
        expected
    );
    // Schema and view names follow the identifier rule.
    assert_eq!(
        q(&mut e, "SELECT * FROM INFORMATION_SCHEMA.TABLES"),
        expected
    );
    assert_eq!(
        q(&mut e, "SELECT * FROM \"information_schema\".\"tables\""),
        expected
    );
    assert_eq!(
        state(&mut e, "SELECT * FROM \"INFORMATION_SCHEMA\".tables"),
        "42P01"
    );
    // Column names are lowercase.
    let cols: Vec<String> = e
        .query("SELECT * FROM information_schema.tables")
        .unwrap()
        .columns
        .into_iter()
        .map(|c| c.name)
        .collect();
    assert_eq!(
        cols,
        vec!["table_catalog", "table_schema", "table_name", "table_type"]
    );
    // A view's definition.
    assert_eq!(
        q(
            &mut e,
            "SELECT table_name, view_definition FROM information_schema.views"
        ),
        table(&[&["v", "SELECT id FROM t WHERE (id > 1)"]])
    );
}

#[test]
fn columns_describe_types_defaults_and_nullability() {
    let (_d, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE m (id INTEGER PRIMARY KEY, price DECIMAL(10,2), label VARCHAR(20) NOT NULL DEFAULT 'x', code CHAR(3))",
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT * FROM information_schema.columns WHERE table_name = 'm'"
        ),
        table(&[
            &[
                "aerodb", "public", "m", "id", "1", "NULL", "NO", "INTEGER", "NULL", "32", "0"
            ],
            &[
                "aerodb", "public", "m", "price", "2", "NULL", "YES", "DECIMAL", "NULL", "10", "2"
            ],
            &[
                "aerodb",
                "public",
                "m",
                "label",
                "3",
                "'x'",
                "NO",
                "CHARACTER VARYING",
                "20",
                "NULL",
                "NULL"
            ],
            &[
                "aerodb",
                "public",
                "m",
                "code",
                "4",
                "NULL",
                "YES",
                "CHARACTER",
                "3",
                "NULL",
                "NULL"
            ],
        ])
    );
    let r = e.query("SELECT * FROM information_schema.columns").unwrap();
    let names: Vec<(String, String)> = r
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.ty.name()))
        .collect();
    assert_eq!(
        names,
        [
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("column_name", "TEXT"),
            ("ordinal_position", "INTEGER"),
            ("column_default", "TEXT"),
            ("is_nullable", "TEXT"),
            ("data_type", "TEXT"),
            ("character_maximum_length", "INTEGER"),
            ("numeric_precision", "INTEGER"),
            ("numeric_scale", "INTEGER"),
        ]
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect::<Vec<_>>()
    );

    exec(
        &mut e,
        "CREATE TABLE ty (s SMALLINT, mi MEDIUMINT, d DOUBLE, tx TEXT, b BOOLEAN, dt DATE, tm TIME, ts TIMESTAMP, \
         dtt DATETIME, y YEAR, n INTEGER NOT NULL DEFAULT 7)",
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, \
             column_default FROM information_schema.columns WHERE table_name = 'ty' ORDER BY ordinal_position"
        ),
        table(&[
            &["s", "SMALLINT", "NULL", "16", "0", "YES", "NULL"],
            &["mi", "MEDIUMINT", "NULL", "24", "0", "YES", "NULL"],
            &["d", "DOUBLE PRECISION", "NULL", "53", "NULL", "YES", "NULL"],
            &["tx", "TEXT", "NULL", "NULL", "NULL", "YES", "NULL"],
            &["b", "BOOLEAN", "NULL", "NULL", "NULL", "YES", "NULL"],
            &["dt", "DATE", "NULL", "NULL", "NULL", "YES", "NULL"],
            &["tm", "TIME", "NULL", "NULL", "NULL", "YES", "NULL"],
            &["ts", "TIMESTAMP", "NULL", "NULL", "NULL", "YES", "NULL"],
            &["dtt", "DATETIME", "NULL", "NULL", "NULL", "YES", "NULL"],
            &["y", "YEAR", "NULL", "NULL", "NULL", "YES", "NULL"],
            &["n", "INTEGER", "NULL", "32", "0", "NO", "7"],
        ])
    );
}

#[test]
fn view_columns_use_the_view_result_types() {
    let (_d, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE m (id INTEGER PRIMARY KEY, price DECIMAL(10,2), label VARCHAR(20))",
    );
    exec(
        &mut e,
        "CREATE VIEW mv AS SELECT id, price * 2 AS p2, UPPER(label) AS u, label, NULL AS z FROM m",
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT column_name, ordinal_position, is_nullable, data_type, character_maximum_length, \
             numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = 'mv'"
        ),
        table(&[
            &["id", "1", "YES", "INTEGER", "NULL", "32", "0"],
            &["p2", "2", "YES", "DECIMAL", "NULL", "10", "2"],
            &["u", "3", "YES", "TEXT", "NULL", "NULL", "NULL"],
            &[
                "label",
                "4",
                "YES",
                "CHARACTER VARYING",
                "20",
                "NULL",
                "NULL"
            ],
            &["z", "5", "YES", "TEXT", "NULL", "NULL", "NULL"],
        ])
    );
}

fn constraint_schema(e: &mut Engine) {
    exec(
        e,
        "CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
    );
    exec(
        e,
        "CREATE TABLE c (id INTEGER, pid INTEGER REFERENCES p(id) ON DELETE CASCADE, code TEXT, \
         qty INTEGER CHECK (qty > 0), \
         CONSTRAINT c_pk PRIMARY KEY (id), \
         CONSTRAINT c_code_fk FOREIGN KEY (code) REFERENCES p(code) ON UPDATE RESTRICT, \
         CONSTRAINT qty_small CHECK (qty < 100 OR code LIKE 'big%'))",
    );
    exec(
        e,
        "CREATE TABLE cp (a INTEGER, b TEXT, c INTEGER, PRIMARY KEY (c, a), UNIQUE (b, c))",
    );
}

const EXPECTED_CONSTRAINTS: &[&[&str]] = &[
    &["aerodb", "public", "c_pk", "public", "c", "PRIMARY KEY"],
    &[
        "aerodb",
        "public",
        "c_pid_fkey",
        "public",
        "c",
        "FOREIGN KEY",
    ],
    &[
        "aerodb",
        "public",
        "c_code_fk",
        "public",
        "c",
        "FOREIGN KEY",
    ],
    &["aerodb", "public", "c_qty_check", "public", "c", "CHECK"],
    &["aerodb", "public", "qty_small", "public", "c", "CHECK"],
    &["aerodb", "public", "cp_pkey", "public", "cp", "PRIMARY KEY"],
    &["aerodb", "public", "cp_b_c_key", "public", "cp", "UNIQUE"],
    &["aerodb", "public", "p_pkey", "public", "p", "PRIMARY KEY"],
    &["aerodb", "public", "p_code_key", "public", "p", "UNIQUE"],
];

const EXPECTED_KEY_USAGE: &[&[&str]] = &[
    &["c_pk", "public", "c", "id", "1"],
    &["c_pid_fkey", "public", "c", "pid", "1"],
    &["c_code_fk", "public", "c", "code", "1"],
    &["cp_pkey", "public", "cp", "c", "1"],
    &["cp_pkey", "public", "cp", "a", "2"],
    &["cp_b_c_key", "public", "cp", "b", "1"],
    &["cp_b_c_key", "public", "cp", "c", "2"],
    &["p_pkey", "public", "p", "id", "1"],
    &["p_code_key", "public", "p", "code", "1"],
];

fn assert_constraint_views(e: &mut Engine) {
    assert_eq!(
        q(e, "SELECT * FROM information_schema.table_constraints"),
        table(EXPECTED_CONSTRAINTS)
    );
    assert_eq!(
        q(e, "SELECT * FROM information_schema.key_column_usage"),
        table(EXPECTED_KEY_USAGE)
    );
    assert_eq!(
        q(
            e,
            "SELECT * FROM information_schema.referential_constraints"
        ),
        table(&[
            &["c_pid_fkey", "p_pkey", "NO ACTION", "CASCADE"],
            &["c_code_fk", "p_code_key", "RESTRICT", "NO ACTION"],
        ])
    );
    assert_eq!(
        q(e, "SELECT * FROM information_schema.check_constraints"),
        table(&[
            &["c_qty_check", "(qty > 0)"],
            &["qty_small", "((qty < 100) OR (code LIKE 'big%'))"]
        ])
    );
}

#[test]
fn constraint_views_list_pk_unique_fk_and_check() {
    let (_d, mut e) = engine();
    constraint_schema(&mut e);
    assert_constraint_views(&mut e);
    // NOT NULL is not a listed constraint.
    exec(&mut e, "CREATE TABLE nn (x INTEGER NOT NULL)");
    assert_eq!(
        q(
            &mut e,
            "SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_name = 'nn'"
        ),
        table(&[&["0"]])
    );
}

#[test]
fn information_schema_views_filter_sort_join_and_aggregate() {
    let (_d, mut e) = engine();
    constraint_schema(&mut e);
    exec(&mut e, "CREATE VIEW pv AS SELECT id, code FROM p");
    assert_eq!(
        q(
            &mut e,
            "SELECT c.table_name, c.column_name FROM information_schema.columns c \
             JOIN information_schema.tables t ON t.table_name = c.table_name \
             WHERE t.table_type = 'VIEW' ORDER BY c.ordinal_position DESC"
        ),
        table(&[&["pv", "code"], &["pv", "id"]])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT tc.table_name, k.column_name, k.ordinal_position \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.key_column_usage k ON k.constraint_name = tc.constraint_name \
             WHERE tc.constraint_type = 'PRIMARY KEY' ORDER BY tc.table_name DESC, k.ordinal_position"
        ),
        table(&[
            &["p", "id", "1"],
            &["cp", "c", "1"],
            &["cp", "a", "2"],
            &["c", "id", "1"]
        ])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT table_name, COUNT(*) FROM information_schema.columns GROUP BY table_name \
             HAVING COUNT(*) > 2 ORDER BY table_name"
        ),
        table(&[&["c", "4"], &["cp", "3"]])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT table_name FROM information_schema.tables WHERE table_name IN \
             (SELECT table_name FROM information_schema.columns WHERE column_name = 'code') ORDER BY 1"
        ),
        table(&[&["c"], &["p"], &["pv"]])
    );
    // Qualified column references use the view name or its alias.
    assert_eq!(
        q(
            &mut e,
            "SELECT tables.table_name FROM information_schema.tables WHERE tables.table_type = 'VIEW'"
        ),
        table(&[&["pv"]])
    );
}

#[test]
fn database_api_reads_information_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(db_path(&dir).to_str().unwrap()).unwrap();
    db.autocommit(stmt("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
        .unwrap();
    let tx = db.begin().unwrap();
    db.execute(&tx, stmt("CREATE TABLE u (x INTEGER)")).unwrap();
    // Uncommitted DDL of the own transaction is visible.
    let r = db
        .query(&tx, "SELECT table_name FROM information_schema.tables")
        .unwrap();
    let names: Vec<String> = r.rows.iter().map(|row| row[0].to_string()).collect();
    assert_eq!(names, vec!["t", "u"]);
    db.abort(tx).unwrap();
}

#[test]
fn metadata_follows_alter_and_drop() {
    let (_d, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE gone (x INTEGER UNIQUE)");
    exec(
        &mut e,
        "ALTER TABLE t ADD COLUMN note VARCHAR(5) DEFAULT 'n' CHECK (note <> 'bad')",
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT column_name, ordinal_position, column_default, data_type FROM information_schema.columns \
             WHERE table_name = 't'"
        ),
        table(&[
            &["id", "1", "NULL", "INTEGER"],
            &["note", "2", "'n'", "CHARACTER VARYING"]
        ])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 't'"
        ),
        table(&[&["t_pkey", "PRIMARY KEY"], &["t_note_check", "CHECK"]])
    );
    exec(&mut e, "DROP TABLE gone");
    assert_eq!(
        q(&mut e, "SELECT table_name FROM information_schema.tables"),
        table(&[&["t"]])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT COUNT(*) FROM information_schema.key_column_usage WHERE table_name = 'gone'"
        ),
        table(&[&["0"]])
    );
    // A rolled-back CREATE TABLE leaves no metadata.
    exec(&mut e, "BEGIN");
    exec(&mut e, "CREATE TABLE tmp (x INTEGER)");
    assert_eq!(
        q(&mut e, "SELECT COUNT(*) FROM information_schema.tables"),
        table(&[&["2"]])
    );
    exec(&mut e, "ROLLBACK");
    assert_eq!(
        q(&mut e, "SELECT table_name FROM information_schema.tables"),
        table(&[&["t"]])
    );
}

#[test]
fn metadata_and_constraint_names_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::new(db_path(&dir).to_str().unwrap());
    constraint_schema(&mut e);
    exec(
        &mut e,
        "CREATE VIEW meta AS SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'",
    );
    let tables_before = q(&mut e, "SELECT * FROM information_schema.tables");
    let columns_before = q(&mut e, "SELECT * FROM information_schema.columns");
    let mut e = reopen(&dir, e);
    assert_constraint_views(&mut e);
    assert_eq!(
        q(&mut e, "SELECT * FROM information_schema.tables"),
        tables_before
    );
    assert_eq!(
        q(&mut e, "SELECT * FROM information_schema.columns"),
        columns_before
    );
    // A view over information_schema persists and still runs.
    assert_eq!(
        q(&mut e, "SELECT * FROM meta"),
        table(&[&["c"], &["cp"], &["p"]])
    );
    assert_eq!(
        q(
            &mut e,
            "SELECT view_definition FROM information_schema.views WHERE table_name = 'meta'"
        ),
        table(&[&[
            "SELECT table_name FROM information_schema.tables WHERE (table_type = 'BASE TABLE')"
        ]])
    );
}

#[test]
fn information_schema_is_read_only() {
    let (_d, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    for sql in [
        "INSERT INTO information_schema.tables VALUES ('a', 'b', 'c', 'd')",
        "UPDATE information_schema.tables SET table_name = 'x'",
        "DELETE FROM information_schema.columns",
        "DROP TABLE information_schema.tables",
        "DROP VIEW information_schema.views",
        "CREATE TABLE information_schema.x (id INTEGER)",
        "CREATE VIEW information_schema.x AS SELECT 1",
        "CREATE INDEX i ON information_schema.tables (table_name)",
        "ALTER TABLE information_schema.tables ADD COLUMN x INTEGER",
        "CREATE TABLE r (id INTEGER REFERENCES information_schema.tables (table_name))",
    ] {
        let e2 = err(&mut e, sql);
        assert_eq!(e2.sqlstate(), "0A000", "{sql}: {e2}");
        assert!(
            e2.to_string().contains("information_schema is read-only"),
            "{sql}: {e2}"
        );
    }
    assert_eq!(
        q(&mut e, "SELECT table_name FROM information_schema.tables"),
        table(&[&["t"]])
    );
}

#[test]
fn other_schemas_and_unknown_views_are_not_found() {
    let (_d, mut e) = engine();
    exec(&mut e, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    assert_eq!(state(&mut e, "SELECT * FROM other_schema.t"), "42P01");
    assert_eq!(state(&mut e, "SELECT * FROM public.t"), "42P01");
    assert_eq!(
        state(&mut e, "SELECT * FROM information_schema.nope"),
        "42P01"
    );
    assert_eq!(
        state(
            &mut e,
            "SELECT * FROM t JOIN other_schema.t u ON u.id = t.id"
        ),
        "42P01"
    );
    assert_eq!(
        state(&mut e, "INSERT INTO other_schema.t VALUES (1)"),
        "42P01"
    );
    assert_eq!(state(&mut e, "DELETE FROM other_schema.t"), "42P01");
    // An unqualified name never reaches information_schema.
    assert_eq!(state(&mut e, "SELECT * FROM tables"), "42P01");
    exec(&mut e, "CREATE TABLE tables (x INTEGER)");
    assert_eq!(q(&mut e, "SELECT * FROM tables"), Vec::<Vec<String>>::new());
    exec(
        &mut e,
        "CREATE VIEW meta AS SELECT table_name FROM information_schema.tables",
    );
    // A view over information_schema is not a dependent of the user table `tables`.
    exec(&mut e, "DROP TABLE tables");
    assert_eq!(q(&mut e, "SELECT * FROM meta"), table(&[&["meta"], &["t"]]));
}

fn fixture_copy() -> (TempDir, PathBuf) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/aerodb-0.5.1");
    let dir = tempfile::tempdir().unwrap();
    for f in ["fixture.db", "fixture.db.wal", "fixture.db.clog"] {
        fs::copy(src.join(f), dir.path().join(f)).unwrap();
    }
    let path = dir.path().join("fixture.db");
    (dir, path)
}

#[test]
fn legacy_0_5_1_tables_get_deterministic_constraint_names() {
    let (_dir, path) = fixture_copy();
    let mut e = Engine::new(path.to_str().unwrap());
    let expected = table(&[
        &["orders", "orders_pkey", "PRIMARY KEY"],
        &["orders", "orders_user_id_fkey", "FOREIGN KEY"],
        &["users", "users_pkey", "PRIMARY KEY"],
    ]);
    let sql = "SELECT table_name, constraint_name, constraint_type FROM information_schema.table_constraints";
    assert_eq!(q(&mut e, sql), expected);
    assert_eq!(
        q(
            &mut e,
            "SELECT * FROM information_schema.referential_constraints"
        ),
        table(&[&[
            "orders_user_id_fkey",
            "users_pkey",
            "NO ACTION",
            "NO ACTION"
        ]])
    );
    drop(e);
    let mut e = Engine::new(path.to_str().unwrap());
    assert_eq!(q(&mut e, sql), expected);
}

// ---------------------------------------------------------------------------
// SQLSTATE surface
// ---------------------------------------------------------------------------

#[test]
fn commit_time_foreign_key_violation_is_23503() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(db_path(&dir).to_str().unwrap()).unwrap();
    db.autocommit(stmt("CREATE TABLE p (id INTEGER PRIMARY KEY)"))
        .unwrap();
    db.autocommit(stmt(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
    ))
    .unwrap();
    db.autocommit(stmt("INSERT INTO p VALUES (1)")).unwrap();
    let child = db.begin().unwrap();
    db.execute(&child, stmt("INSERT INTO c VALUES (1, 1)"))
        .unwrap();
    let parent = db.begin().unwrap();
    db.execute(&parent, stmt("DELETE FROM p WHERE id = 1"))
        .unwrap();
    db.commit(parent).unwrap();
    // The reference is valid inside `child`'s snapshot; the violation is
    // only detected when it commits, and keeps its class (not Io / 58030).
    let e = db
        .commit(child)
        .expect_err("stale reference must not commit");
    assert!(matches!(e, DbError::ForeignKeyViolation(_)), "{e:?}");
    assert_eq!(e.sqlstate(), "23503");
}

#[test]
fn engine_errors_carry_their_sqlstate() {
    let (_d, mut e) = engine();
    exec(
        &mut e,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, s VARCHAR(2) UNIQUE, n INTEGER NOT NULL DEFAULT 0 CHECK (n >= 0))",
    );
    exec(
        &mut e,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, tid INTEGER REFERENCES t(id))",
    );
    exec(&mut e, "INSERT INTO t VALUES (1, 'a', 1)");
    let cases: &[(&str, &str)] = &[
        ("SELEC 1", "42601"),
        ("SELECT * FROM nope", "42P01"),
        ("SELECT nope FROM t", "42703"),
        ("SELECT FOO(1)", "42883"),
        ("WITH x AS (SELECT 1) SELECT 1", "0A000"),
        ("INSERT INTO t VALUES (1, 'b', 1)", "23505"),
        ("INSERT INTO t VALUES (2, 'a', 1)", "23505"),
        ("INSERT INTO t (id, s, n) VALUES (3, 'c', NULL)", "23502"),
        ("INSERT INTO t VALUES (4, 'd', -1)", "23514"),
        ("INSERT INTO t VALUES (5, 'toolong', 1)", "22001"),
        ("INSERT INTO c VALUES (1, 99)", "23503"),
        ("SELECT 1 / 0", "22012"),
        ("SELECT CAST('x' AS INTEGER)", "22018"),
        ("SELECT 1 + 'a'", "42804"),
        ("SELECT (SELECT id FROM t UNION SELECT 2)", "21000"),
        ("SELECT id FROM t, c", "42702"),
        ("SELECT s, COUNT(*) FROM t", "42803"),
        ("CREATE TABLE t (x INTEGER)", "42P07"),
        ("CREATE TABLE d (x INTEGER, x INTEGER)", "42701"),
        ("DROP TABLE t", "2BP01"),
        ("CREATE TABLE f (x INTEGER REFERENCES c(tid))", "42830"),
        (
            "CREATE TABLE k (a INTEGER, CONSTRAINT z UNIQUE (a), CONSTRAINT z CHECK (a > 0))",
            "42710",
        ),
        ("DROP VIEW t", "42809"),
    ];
    for (sql, code) in cases {
        let got = err(&mut e, sql);
        assert_eq!(got.sqlstate(), *code, "{sql}: {got:?}");
    }
}
