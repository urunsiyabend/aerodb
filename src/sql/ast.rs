// src/sql/ast.rs
use crate::storage::row::ColumnType;

#[derive(Debug, Clone)]
pub enum Expr {
    Equals { left: String, right: String },
    NotEquals { left: String, right: String },
    Add { left: String, right: String },
    Subtract { left: String, right: String },
    Multiply { left: String, right: String },
    Divide { left: String, right: String },
    Modulo { left: String, right: String },
    BitwiseAnd { left: String, right: String },
    BitwiseOr { left: String, right: String },
    BitwiseXor { left: String, right: String },
    Between { expr: String, low: String, high: String },
    GreaterThan { left: String, right: String },
    GreaterOrEquals { left: String, right: String },
    LessThan { left: String, right: String },
    LessOrEquals { left: String, right: String },
    InSubquery { left: String, query: Box<Statement> },
    ExistsSubquery { query: Box<Statement> },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Subquery(Box<Statement>),
    Literal(String),
    DefaultValue,
    FunctionCall { name: String, args: Vec<Expr> },
}

#[derive(Debug)]
pub struct OrderBy {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    NoAction,
    Cascade,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub parent_table: String,
    pub parent_columns: Vec<String>,
    pub on_delete: Option<Action>,
    pub on_update: Option<Action>,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub table: String,
    pub alias: Option<String>,
    pub left_table: String,
    pub left_column: String,
    pub right_column: String,
}

#[derive(Debug, Clone)]
pub enum TableRef {
    Named { name: String, alias: Option<String> },
    Subquery { query: Box<Statement>, alias: String },
}

#[derive(Debug, Clone)]
pub enum AggFunc {
    Min,
    Max,
    Count,
    Sum,
    Avg,
}

/// SQL literal used in DEFAULT clauses and elsewhere.
pub type Literal = String;

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub not_null: bool,
    pub default_value: Option<Expr>,
    pub auto_increment: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone)]
pub struct CreateSequence {
    pub name: String,
    pub start: i64,
    pub increment: i64,
}

impl AggFunc {
    pub fn as_str(&self) -> &'static str {
        match self {
            AggFunc::Min => "MIN",
            AggFunc::Max => "MAX",
            AggFunc::Count => "COUNT",
            AggFunc::Sum => "SUM",
            AggFunc::Avg => "AVG",
        }
    }
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    All,
    Column(String),
    Aggregate { func: AggFunc, column: Option<String> },
    Expr(Box<Expr>),
    Subquery(Box<Statement>),
    Literal(String),
}

#[derive(Debug, Clone)]
pub struct SelectExpr {
    pub expr: SelectItem,
    pub alias: Option<String>,
}
pub type Predicate = Expr;

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
        fks: Vec<ForeignKey>,
        primary_key: Option<Vec<String>>,
        if_not_exists: bool,
    },
    CreateIndex {
        index_name: String,
        table_name: String,
        column_name: String,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>, // None for unqualified
        values: Vec<Expr>,
    },
    Select {
        columns: Vec<SelectExpr>,
        from: Vec<TableRef>,
        joins: Vec<JoinClause>,
        where_predicate: Option<Predicate>,
        group_by: Option<Vec<String>>,
        having: Option<Predicate>,
    },
    Delete {
        table_name: String,
        selection: Option<Expr>,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, String)>,
        selection: Option<Expr>,
    },
    CreateSequence(CreateSequence),
    BeginTransaction { name: Option<String> },
    Commit,
    Rollback,
    Exit,
}

use std::collections::HashMap;

/// Evaluate an expression against a map of column values. If an operand
/// matches a column name, the corresponding value is used; otherwise the
/// operand itself is treated as a literal string.
use crate::storage::row::ColumnValue;

pub fn evaluate_expression(expr: &Expr, values: &HashMap<String, String>) -> ColumnValue {
    fn get_value<'a>(token: &'a str, values: &'a HashMap<String, String>) -> &'a str {
        values.get(token).map(String::as_str).unwrap_or(token)
    }

    match expr {
        Expr::Equals { left, right } => ColumnValue::Boolean(get_value(left, values) == get_value(right, values)),
        Expr::NotEquals { left, right } => ColumnValue::Boolean(get_value(left, values) != get_value(right, values)),
        Expr::Add { left, right } => {
            let l = get_value(left, values).parse::<f64>().unwrap_or(0.0);
            let r = get_value(right, values).parse::<f64>().unwrap_or(0.0);
            ColumnValue::Double(l + r)
        }
        Expr::Subtract { left, right } => {
            let l = get_value(left, values).parse::<f64>().unwrap_or(0.0);
            let r = get_value(right, values).parse::<f64>().unwrap_or(0.0);
            ColumnValue::Double(l - r)
        }
        Expr::Multiply { left, right } => {
            let l = get_value(left, values).parse::<f64>().unwrap_or(0.0);
            let r = get_value(right, values).parse::<f64>().unwrap_or(0.0);
            ColumnValue::Double(l * r)
        }
        Expr::Divide { left, right } => {
            let l = get_value(left, values).parse::<f64>().unwrap_or(0.0);
            let r = get_value(right, values).parse::<f64>().unwrap_or(1.0);
            if r == 0.0 { ColumnValue::Double(0.0) } else { ColumnValue::Double(l / r) }
        }
        Expr::Modulo { left, right } => {
            let l = get_value(left, values).parse::<f64>().unwrap_or(0.0);
            let r = get_value(right, values).parse::<f64>().unwrap_or(1.0);
            if r == 0.0 { ColumnValue::Double(0.0) } else { ColumnValue::Double(l % r) }
        }
        Expr::BitwiseAnd { left, right } => {
            let l = get_value(left, values).parse::<i32>().unwrap_or(0);
            let r = get_value(right, values).parse::<i32>().unwrap_or(0);
            ColumnValue::Integer(l & r)
        }
        Expr::BitwiseOr { left, right } => {
            let l = get_value(left, values).parse::<i32>().unwrap_or(0);
            let r = get_value(right, values).parse::<i32>().unwrap_or(0);
            ColumnValue::Integer(l | r)
        }
        Expr::BitwiseXor { left, right } => {
            let l = get_value(left, values).parse::<i32>().unwrap_or(0);
            let r = get_value(right, values).parse::<i32>().unwrap_or(0);
            ColumnValue::Integer(l ^ r)
        }
        Expr::Between { expr: v, low, high } => {
            let val = get_value(v, values).parse::<f64>().unwrap_or(0.0);
            let l = get_value(low, values).parse::<f64>().unwrap_or(0.0);
            let h = get_value(high, values).parse::<f64>().unwrap_or(0.0);
            ColumnValue::Boolean(val >= l && val <= h)
        }
        Expr::GreaterThan { left, right } => {
            let l = get_value(left, values).parse::<f64>().unwrap_or(0.0);
            let r = get_value(right, values).parse::<f64>().unwrap_or(0.0);
            ColumnValue::Boolean(l > r)
        }
        Expr::GreaterOrEquals { left, right } => {
            let l = get_value(left, values).parse::<f64>().unwrap_or(0.0);
            let r = get_value(right, values).parse::<f64>().unwrap_or(0.0);
            ColumnValue::Boolean(l >= r)
        }
        Expr::LessThan { left, right } => {
            let l = get_value(left, values).parse::<f64>().unwrap_or(0.0);
            let r = get_value(right, values).parse::<f64>().unwrap_or(0.0);
            ColumnValue::Boolean(l < r)
        }
        Expr::LessOrEquals { left, right } => {
            let l = get_value(left, values).parse::<f64>().unwrap_or(0.0);
            let r = get_value(right, values).parse::<f64>().unwrap_or(0.0);
            ColumnValue::Boolean(l <= r)
        }
        Expr::FunctionCall { name, args } => {
            let arg_vals: Vec<ColumnValue> = args.iter().map(|a| evaluate_expression(a, values)).collect();
            match crate::sql::functions::FunctionEvaluator::evaluate_function(name, &arg_vals) {
                Ok(v) => v,
                Err(_) => ColumnValue::Null,
            }
        }
        Expr::InSubquery { .. } | Expr::ExistsSubquery { .. } => ColumnValue::Boolean(false),
        Expr::And(a, b) => {
            match (evaluate_expression(a, values), evaluate_expression(b, values)) {
                (ColumnValue::Boolean(l), ColumnValue::Boolean(r)) => ColumnValue::Boolean(l && r),
                _ => ColumnValue::Boolean(false),
            }
        }
        Expr::Or(a, b) => {
            match (evaluate_expression(a, values), evaluate_expression(b, values)) {
                (ColumnValue::Boolean(l), ColumnValue::Boolean(r)) => ColumnValue::Boolean(l || r),
                _ => ColumnValue::Boolean(false),
            }
        }
        Expr::Subquery(_) | Expr::Literal(_) | Expr::FunctionCall { .. } | Expr::DefaultValue => ColumnValue::Boolean(false),
    }
}

pub fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Literal(s) => s.clone(),
        Expr::DefaultValue => "DEFAULT".into(),
        Expr::FunctionCall { name, args } => {
            if args.is_empty() {
                format!("{}{}", name, if name.ends_with("()") { "" } else { "()" })
            } else {
                let inner: Vec<String> = args.iter().map(expr_to_string).collect();
                format!("{}({})", name, inner.join(", "))
            }
        }
        _ => String::new(),
    }
}

pub fn parse_default_expr(s: &str) -> Expr {
    let upper = s.to_ascii_uppercase();
    if upper == "CURRENT_TIMESTAMP" || upper == "CURRENT_TIMESTAMP()" {
        Expr::FunctionCall { name: "CURRENT_TIMESTAMP".into(), args: Vec::new() }
    } else if upper == "GETDATE()" {
        Expr::FunctionCall { name: "GETDATE".into(), args: Vec::new() }
    } else if upper == "GETUTCDATE()" {
        Expr::FunctionCall { name: "GETUTCDATE".into(), args: Vec::new() }
    } else {
        Expr::Literal(s.to_string())
    }
}
