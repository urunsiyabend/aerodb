use aerodb::sql::parser::parse_statement;
use aerodb::sql::ast::Statement;

#[test]
fn parse_from_subquery() {
    let stmt = parse_statement("SELECT * FROM (SELECT id FROM users) AS sub").unwrap();
    match stmt {
        Statement::Select { from_table, from_subquery: Some(boxed), .. } => {
            assert_eq!(from_table, "sub");
            if let Statement::Select { from_table: inner_table, .. } = *boxed {
                assert_eq!(inner_table, "users");
            } else {
                panic!("expected inner select");
            }
        }
        _ => panic!("expected select"),
    }
}
