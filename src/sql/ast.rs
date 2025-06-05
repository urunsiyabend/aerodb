// src/sql/ast.rs

#[derive(Debug)]
pub enum Expr {
    Equals { left: String, right: String },
    NotEquals { left: String, right: String },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}

#[derive(Debug)]
pub enum Statement {
    CreateTable {
        table_name: String,
        columns: Vec<String>,
    },
    Insert {
        table_name: String,
        values: Vec<String>, // all literal values as strings
    },
    Select {
        table_name: String,
        selection: Option<Expr>,
    },
    Exit,
}

use std::collections::HashMap;

/// Evaluate an expression against a map of column values. If an operand
/// matches a column name, the corresponding value is used; otherwise the
/// operand itself is treated as a literal string.
pub fn evaluate_expression(expr: &Expr, values: &HashMap<String, String>) -> bool {
    fn get_value<'a>(token: &'a str, values: &'a HashMap<String, String>) -> &'a str {
        values.get(token).map(String::as_str).unwrap_or(token)
    }

    match expr {
        Expr::Equals { left, right } => get_value(left, values) == get_value(right, values),
        Expr::NotEquals { left, right } => get_value(left, values) != get_value(right, values),
        Expr::And(a, b) => evaluate_expression(a, values) && evaluate_expression(b, values),
        Expr::Or(a, b) => evaluate_expression(a, values) || evaluate_expression(b, values),
    }
}
