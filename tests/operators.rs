use aerodb::sql::{ast::{Expr, Statement}, parser::parse_statement};
use aerodb::storage::row::ColumnValue;
use std::collections::HashMap;

#[test]
fn parse_not_equals_angle_brackets() {
    let stmt = parse_statement("SELECT id FROM users WHERE id <> 5").unwrap();
    if let Statement::Select { where_predicate: Some(pred), .. } = stmt {
        match pred {
            Expr::NotEquals { left, right } => {
                assert_eq!(left, "id");
                assert_eq!(right, "5");
            }
            _ => panic!("expected NotEquals"),
        }
    } else { panic!("expected select") }
}

#[test]
fn parse_between_expression() {
    let stmt = parse_statement("SELECT id FROM users WHERE id BETWEEN 1 AND 3").unwrap();
    if let Statement::Select { where_predicate: Some(pred), .. } = stmt {
        match pred {
            Expr::Between { expr, low, high } => {
                assert_eq!(expr, "id");
                assert_eq!(low, "1");
                assert_eq!(high, "3");
            }
            _ => panic!("expected Between"),
        }
    } else { panic!("expected select") }
}

#[test]
fn evaluate_between_true() {
    let expr = Expr::Between { expr: "5".into(), low: "1".into(), high: "10".into() };
    assert_eq!(
        aerodb::sql::ast::evaluate_expression(&expr, &HashMap::new()),
        ColumnValue::Boolean(true)
    );
}

#[test]
fn evaluate_addition_nonzero() {
    let expr = Expr::Add { left: "2".into(), right: "3".into() };
    assert_eq!(
        aerodb::sql::ast::evaluate_expression(&expr, &HashMap::new()),
        ColumnValue::Integer(5)
    );
    let expr2 = Expr::Add { left: "2".into(), right: "-2".into() };
    assert_eq!(
        aerodb::sql::ast::evaluate_expression(&expr2, &HashMap::new()),
        ColumnValue::Integer(0)
    );
}

#[test]
fn evaluate_multiplication_value() {
    let expr = Expr::Multiply { left: "4".into(), right: "5".into() };
    assert_eq!(
        aerodb::sql::ast::evaluate_expression(&expr, &HashMap::new()),
        ColumnValue::Integer(20)
    );
}

#[test]
fn evaluate_division_value() {
    let expr = Expr::Divide { left: "10".into(), right: "2".into() };
    assert_eq!(
        aerodb::sql::ast::evaluate_expression(&expr, &HashMap::new()),
        ColumnValue::Integer(5)
    );
}

#[test]
fn evaluate_modulo_value() {
    let expr = Expr::Modulo { left: "10".into(), right: "3".into() };
    assert_eq!(
        aerodb::sql::ast::evaluate_expression(&expr, &HashMap::new()),
        ColumnValue::Integer(1)
    );
}

#[test]
fn evaluate_bitwise_and_value() {
    let expr = Expr::BitwiseAnd { left: "6".into(), right: "3".into() };
    assert_eq!(
        aerodb::sql::ast::evaluate_expression(&expr, &HashMap::new()),
        ColumnValue::Integer(6 & 3)
    );
}

#[test]
fn evaluate_bitwise_or_value() {
    let expr = Expr::BitwiseOr { left: "4".into(), right: "1".into() };
    assert_eq!(
        aerodb::sql::ast::evaluate_expression(&expr, &HashMap::new()),
        ColumnValue::Integer(5)
    );
}

#[test]
fn evaluate_bitwise_xor_value() {
    let expr = Expr::BitwiseXor { left: "6".into(), right: "3".into() };
    assert_eq!(
        aerodb::sql::ast::evaluate_expression(&expr, &HashMap::new()),
        ColumnValue::Integer(5)
    );
}
