//! Parse-level behaviour of the fail-closed SQL front end.
use aerodb::sql::ast::{
    Action, Expr, SelectItem, SetExpr, SetOperator, Statement, TableConstraint,
};
use aerodb::sql::parser::{parse_script, parse_sql};
use aerodb::storage::row::ColumnType;

fn sqlstate(sql: &str) -> &'static str {
    match parse_sql(sql) {
        Ok(stmt) => panic!("expected `{sql}` to be rejected, got {stmt:?}"),
        Err(e) => e.sqlstate(),
    }
}

fn query_body(sql: &str) -> SetExpr {
    match parse_sql(sql).unwrap_or_else(|e| panic!("`{sql}`: {e}")) {
        Statement::Query(q) => q.body,
        other => panic!("expected query, got {other:?}"),
    }
}

#[test]
fn bad_sql_tail() {
    assert_eq!(sqlstate("SELECT id FROM t WHERE id = 1 NONSENSE"), "42601");
}

#[test]
fn rejects_tail_in_subquery() {
    // `FROM u NONSENSE` on its own is a valid correlation name (table alias
    // without AS) in SQL, so the garbage tail is placed where no alias can go.
    assert_eq!(
        sqlstate("SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE id = 1 NONSENSE)"),
        "42601"
    );
    assert_eq!(
        sqlstate("SELECT id FROM t WHERE id IN (SELECT id FROM u NONSENSE GARBAGE)"),
        "42601"
    );
}

#[test]
fn rejects_tail_after_create_table() {
    assert_eq!(sqlstate("CREATE TABLE x (a INT) garbage"), "42601");
}

#[test]
fn rejects_tail_after_semicolon() {
    assert_eq!(sqlstate("SELECT 1; SELECT 2"), "42601");
    assert!(parse_sql("SELECT 1;").is_ok());
}

#[test]
fn rejects_unknown_function() {
    assert_eq!(sqlstate("SELECT JSON_VALUE('{}', '$.a')"), "42883");
}

#[test]
fn window_is_unsupported() {
    assert_eq!(
        sqlstate("SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM t"),
        "0A000"
    );
}

#[test]
fn cte_is_unsupported() {
    assert_eq!(
        sqlstate("WITH q AS (SELECT id FROM t) SELECT id FROM q"),
        "0A000"
    );
}

#[test]
fn cursor_is_unsupported() {
    assert_eq!(sqlstate("DECLARE cur CURSOR FOR SELECT id FROM t"), "0A000");
}

#[test]
fn savepoint_is_unsupported() {
    assert_eq!(sqlstate("SAVEPOINT s"), "0A000");
    assert_eq!(sqlstate("ROLLBACK TO SAVEPOINT s"), "0A000");
    assert_eq!(sqlstate("RELEASE SAVEPOINT s"), "0A000");
}

#[test]
fn set_transaction_is_unsupported() {
    assert_eq!(
        sqlstate("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
        "0A000"
    );
    assert_eq!(sqlstate("SET TRANSACTION READ ONLY"), "0A000");
}

#[test]
fn union_parses_to_setop() {
    match query_body("SELECT 1 UNION SELECT 2") {
        SetExpr::SetOp {
            op: SetOperator::Union,
            all: false,
            left,
            right,
        } => {
            assert!(matches!(*left, SetExpr::Select(_)));
            assert!(matches!(*right, SetExpr::Select(_)));
        }
        other => panic!("expected UNION, got {other:?}"),
    }
    assert!(matches!(
        query_body("SELECT 1 UNION ALL SELECT 2"),
        SetExpr::SetOp {
            op: SetOperator::Union,
            all: true,
            ..
        }
    ));
    assert!(matches!(
        query_body("SELECT 1 UNION DISTINCT SELECT 2"),
        SetExpr::SetOp {
            op: SetOperator::Union,
            all: false,
            ..
        }
    ));
}

#[test]
fn intersect_binds_tighter() {
    match query_body("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3") {
        SetExpr::SetOp {
            op: SetOperator::Union,
            left,
            right,
            ..
        } => {
            assert!(matches!(*left, SetExpr::Select(_)));
            assert!(matches!(
                *right,
                SetExpr::SetOp {
                    op: SetOperator::Intersect,
                    ..
                }
            ));
        }
        other => panic!("expected UNION at the root, got {other:?}"),
    }
    // Left associativity for operators of equal precedence.
    match query_body("SELECT 1 EXCEPT SELECT 2 UNION SELECT 3") {
        SetExpr::SetOp {
            op: SetOperator::Union,
            left,
            ..
        } => {
            assert!(matches!(
                *left,
                SetExpr::SetOp {
                    op: SetOperator::Except,
                    ..
                }
            ));
        }
        other => panic!("expected UNION at the root, got {other:?}"),
    }
}

#[test]
fn inline_comment() {
    assert!(parse_sql("SELECT 1 -- c").is_ok());
}

#[test]
fn block_comment() {
    assert!(parse_sql("SELECT /* comment */ 1").is_ok());
    assert_eq!(sqlstate("SELECT 1 /* never closed"), "42601");
}

#[test]
fn comment_only_script_is_empty() {
    assert!(
        parse_script("-- audit comment SELECT 99")
            .unwrap()
            .is_empty()
    );
    assert!(parse_script("  ").unwrap().is_empty());
    assert_eq!(parse_script("SELECT 1; SELECT 2;").unwrap().len(), 2);
}

#[test]
fn select_distinct_is_not_a_column() {
    match query_body("SELECT DISTINCT v FROM t") {
        SetExpr::Select(sel) => {
            assert!(sel.distinct);
            assert_eq!(sel.projection.len(), 1);
            match &sel.projection[0] {
                SelectItem::Expr {
                    expr: Expr::Column { table: None, name },
                    alias: None,
                } => {
                    assert_eq!(name, "v")
                }
                other => panic!("unexpected item {other:?}"),
            }
        }
        other => panic!("expected select, got {other:?}"),
    }
}

#[test]
fn decimal_and_varchar_column_types() {
    match parse_sql("CREATE TABLE d (n DECIMAL(10,2), s VARCHAR(5), c CHAR(3))").unwrap() {
        Statement::CreateTable { columns, .. } => {
            assert_eq!(
                columns[0].col_type,
                ColumnType::Decimal {
                    precision: 10,
                    scale: 2
                }
            );
            assert_eq!(columns[1].col_type, ColumnType::Varchar(5));
            assert_eq!(columns[2].col_type, ColumnType::Char(3));
        }
        other => panic!("expected create table, got {other:?}"),
    }
}

#[test]
fn fk_actions_parse() {
    let sql = "CREATE TABLE c (id INTEGER, pid INTEGER, \
               FOREIGN KEY (pid) REFERENCES p(id) ON DELETE RESTRICT ON UPDATE CASCADE)";
    match parse_sql(sql).unwrap() {
        Statement::CreateTable { fks, .. } => {
            assert_eq!(fks.len(), 1);
            assert_eq!(fks[0].on_delete, Some(Action::Restrict));
            assert_eq!(fks[0].on_update, Some(Action::Cascade));
        }
        other => panic!("expected create table, got {other:?}"),
    }
    let sql = "CREATE TABLE c (id INTEGER, pid INTEGER, \
               FOREIGN KEY (pid) REFERENCES p (id) ON DELETE NO ACTION)";
    match parse_sql(sql).unwrap() {
        Statement::CreateTable { fks, .. } => {
            assert_eq!(fks[0].on_delete, Some(Action::NoAction));
            assert_eq!(fks[0].on_update, None);
        }
        other => panic!("expected create table, got {other:?}"),
    }
}

#[test]
fn fk_set_null_unsupported() {
    let base = "CREATE TABLE c (id INTEGER, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id) ";
    assert_eq!(sqlstate(&format!("{base}ON DELETE SET NULL)")), "0A000");
    assert_eq!(sqlstate(&format!("{base}ON UPDATE SET DEFAULT)")), "0A000");
}

#[test]
fn drop_table_cascade_unsupported() {
    assert_eq!(sqlstate("DROP TABLE t CASCADE"), "0A000");
    assert!(matches!(
        parse_sql("DROP TABLE t RESTRICT").unwrap(),
        Statement::DropTable {
            if_exists: false,
            ..
        }
    ));
}

#[test]
fn unterminated_string() {
    assert_eq!(sqlstate("SELECT 'abc"), "42601");
}

#[test]
fn column_constraints_are_parsed_not_ignored() {
    // UNIQUE / CHECK / CONSTRAINT name / inline REFERENCES are supported since
    // task 5; each one must show up in the AST (never silently dropped).
    let constraints = |sql: &str| match parse_sql(sql).unwrap_or_else(|e| panic!("`{sql}`: {e}")) {
        Statement::CreateTable {
            constraints, fks, ..
        } => (constraints, fks),
        other => panic!("expected create table, got {other:?}"),
    };
    let (c, _) = constraints("CREATE TABLE d (n INTEGER UNIQUE)");
    assert!(
        matches!(&c[..], [TableConstraint::Unique { name: None, columns }] if columns[0].value == "n")
    );
    let (c, _) = constraints("CREATE TABLE d (n INTEGER CHECK (n > 0))");
    assert!(
        matches!(&c[..], [TableConstraint::Check { name: None, column: Some(col), .. }] if col == "n")
    );
    let (c, _) = constraints("CREATE TABLE d (n INTEGER, CONSTRAINT k UNIQUE (n))");
    assert!(matches!(&c[..], [TableConstraint::Unique { name: Some(k), .. }] if k == "k"));
    let (c, _) =
        constraints("CREATE TABLE d (n INTEGER CONSTRAINT pos CHECK (n > 0), CHECK (n < 9))");
    assert!(matches!(&c[..], [
        TableConstraint::Check { name: Some(p), column: Some(_), .. },
        TableConstraint::Check { name: None, column: None, .. },
    ] if p == "pos"));
    let (_, fks) = constraints("CREATE TABLE d (n INTEGER REFERENCES p(id) ON UPDATE CASCADE)");
    assert_eq!(fks.len(), 1);
    assert_eq!(fks[0].on_update, Some(Action::Cascade));
    assert_eq!(sqlstate("CREATE TABLE d (n INTEGER BOGUS)"), "42601");
    assert_eq!(
        sqlstate("CREATE TABLE d (n INTEGER, CONSTRAINT k)"),
        "42601"
    );
    assert_eq!(
        sqlstate("CREATE TABLE d (n INTEGER REFERENCES p(id) ON DELETE SET NULL)"),
        "0A000"
    );
}

#[test]
fn nested_aggregate_is_rejected() {
    assert!(parse_sql("SELECT SUM(COUNT(*)) FROM t").is_err());
}

#[test]
fn natural_fetch_only_similar_are_reserved() {
    // Recognised but unsupported constructs fail with 0A000 instead of the
    // keyword being swallowed as a table/column alias.
    assert_eq!(sqlstate("SELECT * FROM t NATURAL JOIN u"), "0A000");
    assert_eq!(
        sqlstate("SELECT id FROM t FETCH FIRST 1 ROWS ONLY"),
        "0A000"
    );
    assert_eq!(
        sqlstate("SELECT id FROM t WHERE s SIMILAR TO 'a%'"),
        "0A000"
    );
    assert_eq!(
        sqlstate("SELECT id FROM t WHERE s NOT SIMILAR TO 'a%'"),
        "0A000"
    );
    // Reserved words are not usable as bare aliases or identifiers.
    assert_eq!(sqlstate("SELECT id only FROM t"), "42601");
    assert_eq!(sqlstate("SELECT id AS fetch FROM t"), "42601");
    assert_eq!(sqlstate("SELECT natural FROM t"), "42601");
    assert_eq!(sqlstate("SELECT id similar FROM t"), "42601");
}

#[test]
fn duplicate_insert_and_update_targets_are_rejected() {
    assert_eq!(sqlstate("INSERT INTO u (id, id) VALUES (7, 8)"), "42601");
    assert_eq!(sqlstate("UPDATE t SET v = 1, v = 2"), "42601");
}

#[test]
fn column_references_record_quoting() {
    match query_body("SELECT \"ID\", id, \"T\".x FROM t") {
        SetExpr::Select(sel) => {
            let quoted: Vec<(bool, Option<bool>)> = sel
                .projection
                .iter()
                .map(|item| match item {
                    SelectItem::Expr {
                        expr: Expr::Column { table, name },
                        ..
                    } => (name.quoted, table.as_ref().map(|t| t.quoted)),
                    other => panic!("unexpected item {other:?}"),
                })
                .collect();
            assert_eq!(
                quoted,
                vec![(true, None), (false, None), (false, Some(true))]
            );
        }
        other => panic!("expected select, got {other:?}"),
    }
}
