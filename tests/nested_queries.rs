use aerodb::sql::{parser::parse_statement, ast::{Statement, TableRef, SelectExpr}};

#[test]
fn parse_from_subquery() {
    let stmt = parse_statement("SELECT * FROM (SELECT id FROM t1) AS sub").unwrap();
    if let Statement::Select { from, .. } = stmt {
        assert!(matches!(from[0], TableRef::Subquery { .. }));
    } else { panic!("expected select"); }
}

// TODO: support subquery parsing in SELECT list
