use aerodb::sql::{ast::{BinaryOp, Expr, SelectItem, SetExpr, Statement, TableRef}, parser::parse_statement, value::Value};

#[test]
fn insert_quoted_literal_with_spaces() {
    let stmt = parse_statement("INSERT INTO t VALUES ('hello world')").unwrap();
    match stmt {
        Statement::Insert { table_name, rows, .. } => {
            assert_eq!(table_name, "t");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            match &rows[0][0] {
                Expr::Literal(Value::Text(value)) => assert_eq!(value, "hello world"),
                other => panic!("Expected literal, got {:?}", other),
            }
        }
        other => panic!("Expected insert statement, got {:?}", other),
    }
}

#[test]
fn select_quoted_identifier_with_spaces() {
    let stmt = parse_statement("SELECT \"my column\" FROM t").unwrap();
    match stmt {
        Statement::Query(q) => {
            let SetExpr::Select(sel) = q.body else { panic!("expected select") };
            match &sel.projection[0] {
                SelectItem::Expr { expr: Expr::Column { table: None, name }, .. } => assert_eq!(name, "my column"),
                other => panic!("Expected column, got {:?}", other),
            }
            match &sel.from[0] {
                TableRef::Named { name, .. } => assert_eq!(name, "t"),
                other => panic!("Expected table ref, got {:?}", other),
            }
        }
        other => panic!("Expected select statement, got {:?}", other),
    }
}

#[test]
fn where_clause_quoted_literal_with_spaces() {
    let stmt = parse_statement("SELECT name FROM t WHERE name = 'Alice Smith'").unwrap();
    match stmt {
        Statement::Query(q) => {
            let SetExpr::Select(sel) = q.body else { panic!("expected select") };
            let expr = sel.selection.expect("Expected where predicate");
            match expr {
                Expr::Binary { op: BinaryOp::Eq, left, right } => {
                    assert_eq!(*left, Expr::Column { table: None, name: "name".into() });
                    assert_eq!(*right, Expr::Literal(Value::Text("Alice Smith".into())));
                }
                other => panic!("Expected equals expression, got {:?}", other),
            }
        }
        other => panic!("Expected select statement, got {:?}", other),
    }
}
