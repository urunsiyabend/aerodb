use aerodb::execution::eval::evaluate_constant;
use aerodb::sql::{ast::{BinaryOp, Expr, SetExpr, Statement}, parser::{parse_expression_sql, parse_statement}, value::Value};

fn where_of(sql: &str) -> Expr {
    match parse_statement(sql).unwrap() {
        Statement::Query(q) => match q.body {
            SetExpr::Select(sel) => sel.selection.expect("expected a WHERE predicate"),
            other => panic!("expected select, got {other:?}"),
        },
        other => panic!("expected select, got {other:?}"),
    }
}

fn eval(sql: &str) -> Value {
    evaluate_constant(&parse_expression_sql(sql).unwrap()).unwrap()
}

fn col(name: &str) -> Box<Expr> {
    Box::new(Expr::Column { table: None, name: name.into() })
}

fn int(i: i64) -> Box<Expr> {
    Box::new(Expr::Literal(Value::Int(i)))
}

#[test]
fn parse_not_equals_angle_brackets() {
    match where_of("SELECT id FROM users WHERE id <> 5") {
        Expr::Binary { op: BinaryOp::NotEq, left, right } => {
            assert_eq!(left, col("id"));
            assert_eq!(right, int(5));
        }
        _ => panic!("expected NotEquals"),
    }
}

#[test]
fn parse_between_expression() {
    match where_of("SELECT id FROM users WHERE id BETWEEN 1 AND 3") {
        Expr::Between { expr, low, high, negated: false } => {
            assert_eq!(expr, col("id"));
            assert_eq!(low, int(1));
            assert_eq!(high, int(3));
        }
        _ => panic!("expected Between"),
    }
}

#[test]
fn evaluate_between_true() {
    assert_eq!(eval("5 BETWEEN 1 AND 10"), Value::Bool(true));
}

#[test]
fn evaluate_addition_nonzero() {
    assert_eq!(eval("2 + 3"), Value::Int(5));
    assert_eq!(eval("2 + -2"), Value::Int(0));
}

#[test]
fn evaluate_multiplication_value() {
    assert_eq!(eval("4 * 5"), Value::Int(20));
}

#[test]
fn evaluate_division_value() {
    assert_eq!(eval("10 / 2"), Value::Int(5));
}

#[test]
fn evaluate_modulo_value() {
    assert_eq!(eval("10 % 3"), Value::Int(1));
}

#[test]
fn evaluate_bitwise_and_value() {
    assert_eq!(eval("6 & 3"), Value::Int(6 & 3));
}

#[test]
fn evaluate_bitwise_or_value() {
    assert_eq!(eval("4 | 1"), Value::Int(5));
}

#[test]
fn evaluate_bitwise_xor_value() {
    assert_eq!(eval("6 ^ 3"), Value::Int(5));
}
