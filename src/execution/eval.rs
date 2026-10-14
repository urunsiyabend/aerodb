//! Typed expression evaluator with SQL three-valued logic.
//!
//! Expressions are evaluated against an [`Env`]: the columns in scope, the
//! current row's values and an optional outer environment (the correlation
//! chain for subqueries). Every failure is an error; there are no silent
//! fallbacks to strings or zero.

use std::cmp::Ordering;

use crate::catalog::Catalog;
use crate::error::{DbError, DbResult};
use crate::execution::query::{QueryResult, run_query};
use crate::sql::ast::{BinaryOp, Expr, Query, UnaryOp};
use crate::sql::functions::call_scalar;
use crate::sql::value::{SqlType, Value};

/// One column visible to expressions: `qualifier` is the table alias (or
/// table name) it belongs to, if any.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopeColumn {
    pub qualifier: Option<String>,
    pub name: String,
    pub ty: SqlType,
}

/// Ordered list of columns an expression may reference.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Scope {
    pub columns: Vec<ScopeColumn>,
}

impl Scope {
    pub fn new() -> Self {
        Scope::default()
    }

    pub fn push(&mut self, qualifier: Option<String>, name: String, ty: SqlType) {
        self.columns.push(ScopeColumn { qualifier, name, ty });
    }

    pub fn extend(&mut self, other: &Scope) {
        self.columns.extend(other.columns.iter().cloned());
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Resolve `[table.]name` in this scope. An exact (case-sensitive) match
    /// wins; otherwise a unique case-insensitive match is accepted. More than
    /// one match is an ambiguity error; no match yields `Ok(None)`.
    pub fn resolve(&self, table: Option<&str>, name: &str) -> DbResult<Option<usize>> {
        let find = |exact: bool| -> Vec<usize> {
            let eq = |a: &str, b: &str| if exact { a == b } else { a.eq_ignore_ascii_case(b) };
            self.columns
                .iter()
                .enumerate()
                .filter(|(_, c)| {
                    eq(&c.name, name)
                        && match table {
                            None => true,
                            Some(t) => c.qualifier.as_deref().is_some_and(|q| eq(q, t)),
                        }
                })
                .map(|(i, _)| i)
                .collect()
        };
        let mut hits = find(true);
        if hits.is_empty() {
            hits = find(false);
        }
        match hits.len() {
            0 => Ok(None),
            1 => Ok(Some(hits[0])),
            _ => Err(DbError::AmbiguousColumn(display_column(table, name))),
        }
    }
}

pub fn display_column(table: Option<&str>, name: &str) -> String {
    match table {
        Some(t) => format!("{}.{}", t, name),
        None => name.to_string(),
    }
}

/// Evaluation environment: scope + current row (+ precomputed aggregates for
/// grouped queries) + optional outer environment.
#[derive(Clone, Copy)]
pub struct Env<'a> {
    pub scope: &'a Scope,
    pub row: &'a [Value],
    pub aggregates: &'a [(Expr, Value)],
    pub outer: Option<&'a Env<'a>>,
}

impl<'a> Env<'a> {
    pub fn new(scope: &'a Scope, row: &'a [Value], outer: Option<&'a Env<'a>>) -> Self {
        Env { scope, row, aggregates: &[], outer }
    }

    /// Value of `[table.]name`, searching the correlation chain outwards.
    pub fn lookup(&self, table: Option<&str>, name: &str) -> DbResult<Value> {
        let mut env = Some(self);
        while let Some(e) = env {
            if let Some(i) = e.scope.resolve(table, name)? {
                return Ok(e.row[i].clone());
            }
            env = e.outer;
        }
        Err(DbError::ColumnNotFound(display_column(table, name)))
    }
}

/// What the evaluator may consult besides the environment.
#[derive(Clone, Copy)]
pub struct Ctx<'c> {
    /// `None` when evaluating constant expressions (no subqueries possible).
    pub catalog: Option<&'c Catalog>,
}

impl<'c> Ctx<'c> {
    pub fn with_catalog(catalog: &'c Catalog) -> Self {
        Ctx { catalog: Some(catalog) }
    }

    fn subquery(&self, query: &Query, env: &Env) -> DbResult<QueryResult> {
        match self.catalog {
            Some(cat) => run_query(cat, query, Some(env)),
            None => Err(DbError::FeatureNotSupported(
                "subqueries in constant expressions".into(),
            )),
        }
    }
}

/// Evaluate an expression that references no columns (DEFAULT values, tests).
pub fn evaluate_constant(expr: &Expr) -> DbResult<Value> {
    let scope = Scope::new();
    let env = Env::new(&scope, &[], None);
    eval(expr, &env, &Ctx { catalog: None })
}

/// SQL truth value of a BOOLEAN (or NULL) value.
pub fn truth(v: &Value) -> DbResult<Option<bool>> {
    Ok(v.truthiness()?)
}

fn bool_value(b: Option<bool>) -> Value {
    match b {
        Some(b) => Value::Bool(b),
        None => Value::Null,
    }
}

fn and3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn or3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn not3(a: Option<bool>) -> Option<bool> {
    a.map(|b| !b)
}

/// `a = b` under three-valued logic.
fn eq3(a: &Value, b: &Value) -> DbResult<Option<bool>> {
    Ok(a.compare(b)?.map(|o| o == Ordering::Equal))
}

fn compare_op(op: BinaryOp, a: &Value, b: &Value) -> DbResult<Value> {
    let ord = a.compare(b)?;
    Ok(bool_value(ord.map(|o| match op {
        BinaryOp::Eq => o == Ordering::Equal,
        BinaryOp::NotEq => o != Ordering::Equal,
        BinaryOp::Lt => o == Ordering::Less,
        BinaryOp::LtEq => o != Ordering::Greater,
        BinaryOp::Gt => o == Ordering::Greater,
        _ => o != Ordering::Less, // GtEq
    })))
}

fn bitwise(op: BinaryOp, a: &Value, b: &Value) -> DbResult<Value> {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int(x), Value::Int(y)) => Ok(Value::Int(match op {
            BinaryOp::BitAnd => x & y,
            BinaryOp::BitOr => x | y,
            _ => x ^ y,
        })),
        _ => Err(DbError::TypeMismatch(format!(
            "operator {} is not defined for {} and {}",
            op.symbol(),
            a.type_name(),
            b.type_name()
        ))),
    }
}

/// Text of a LIKE operand; CHAR values compare without their pad spaces.
fn like_text(v: &Value) -> DbResult<&str> {
    match v {
        Value::Text(s) => Ok(s),
        Value::Char(s) => Ok(s.trim_end_matches(' ')),
        other => Err(DbError::TypeMismatch(format!(
            "LIKE is not defined for {}",
            other.type_name()
        ))),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum LikeTok {
    Any,
    One,
    Lit(char),
}

fn compile_like(pattern: &str, escape: Option<char>) -> DbResult<Vec<LikeTok>> {
    let mut out = Vec::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if Some(c) == escape {
            match chars.next() {
                Some(n) => out.push(LikeTok::Lit(n)),
                None => {
                    return Err(DbError::InvalidValue(
                        "LIKE pattern must not end with the escape character".into(),
                    ));
                }
            }
        } else if c == '%' {
            out.push(LikeTok::Any);
        } else if c == '_' {
            out.push(LikeTok::One);
        } else {
            out.push(LikeTok::Lit(c));
        }
    }
    Ok(out)
}

fn like_match(text: &[char], pat: &[LikeTok]) -> bool {
    // Iterative wildcard matching with single-star backtracking.
    let (mut t, mut p) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        match pat.get(p) {
            Some(LikeTok::One) => {
                t += 1;
                p += 1;
            }
            Some(LikeTok::Lit(c)) if *c == text[t] => {
                t += 1;
                p += 1;
            }
            Some(LikeTok::Any) => {
                star = Some((p, t));
                p += 1;
            }
            _ => match star {
                Some((sp, st)) => {
                    p = sp + 1;
                    t = st + 1;
                    star = Some((sp, st + 1));
                }
                None => return false,
            },
        }
    }
    pat[p..].iter().all(|x| *x == LikeTok::Any)
}

fn single_column(result: &QueryResult) -> DbResult<()> {
    if result.columns.len() != 1 {
        return Err(DbError::Syntax("subquery has too many columns".into()));
    }
    Ok(())
}

/// Evaluate a predicate; only TRUE keeps a row (FALSE and UNKNOWN reject).
pub fn eval_predicate(expr: &Expr, env: &Env, cx: &Ctx) -> DbResult<bool> {
    Ok(truth(&eval(expr, env, cx)?)? == Some(true))
}

pub fn eval(expr: &Expr, env: &Env, cx: &Ctx) -> DbResult<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column { table, name } => env.lookup(table.as_deref(), name),
        Expr::Unary { op, expr } => {
            let v = eval(expr, env, cx)?;
            match op {
                UnaryOp::Neg => Ok(v.neg()?),
                UnaryOp::Plus => match v {
                    Value::Null | Value::Int(_) | Value::Decimal(_) | Value::Double(_) => Ok(v),
                    other => Err(DbError::TypeMismatch(format!(
                        "unary + is not defined for {}",
                        other.type_name()
                    ))),
                },
                UnaryOp::Not => Ok(bool_value(not3(truth(&v)?))),
            }
        }
        Expr::Binary { op, left, right } => match op {
            BinaryOp::And => {
                let l = truth(&eval(left, env, cx)?)?;
                if l == Some(false) {
                    return Ok(Value::Bool(false));
                }
                let r = truth(&eval(right, env, cx)?)?;
                Ok(bool_value(and3(l, r)))
            }
            BinaryOp::Or => {
                let l = truth(&eval(left, env, cx)?)?;
                if l == Some(true) {
                    return Ok(Value::Bool(true));
                }
                let r = truth(&eval(right, env, cx)?)?;
                Ok(bool_value(or3(l, r)))
            }
            _ => {
                let l = eval(left, env, cx)?;
                let r = eval(right, env, cx)?;
                match op {
                    op if op.is_comparison() => compare_op(*op, &l, &r),
                    BinaryOp::Add => Ok(l.add(&r)?),
                    BinaryOp::Sub => Ok(l.sub(&r)?),
                    BinaryOp::Mul => Ok(l.mul(&r)?),
                    BinaryOp::Div => Ok(l.div(&r)?),
                    BinaryOp::Mod => Ok(l.rem(&r)?),
                    BinaryOp::Concat => Ok(l.concat(&r)?),
                    _ => bitwise(*op, &l, &r),
                }
            }
        },
        Expr::IsNull { expr, negated } => {
            let v = eval(expr, env, cx)?;
            Ok(Value::Bool(v.is_null() != *negated))
        }
        Expr::IsTruth { expr, value, negated } => {
            let t = truth(&eval(expr, env, cx)?)?;
            Ok(Value::Bool((t == *value) != *negated))
        }
        Expr::InList { expr, list, negated } => {
            let v = eval(expr, env, cx)?;
            let mut acc = Some(false);
            for item in list {
                let iv = eval(item, env, cx)?;
                acc = or3(acc, eq3(&v, &iv)?);
                if acc == Some(true) {
                    break;
                }
            }
            Ok(bool_value(if *negated { not3(acc) } else { acc }))
        }
        Expr::InSubquery { expr, query, negated } => {
            let v = eval(expr, env, cx)?;
            let result = cx.subquery(query, env)?;
            single_column(&result)?;
            let mut acc = Some(false);
            for row in &result.rows {
                acc = or3(acc, eq3(&v, &row[0])?);
                if acc == Some(true) {
                    break;
                }
            }
            Ok(bool_value(if *negated { not3(acc) } else { acc }))
        }
        Expr::Exists { query, negated } => {
            let result = cx.subquery(query, env)?;
            Ok(Value::Bool(result.rows.is_empty() == *negated))
        }
        Expr::Between { expr, low, high, negated } => {
            let v = eval(expr, env, cx)?;
            let lo = eval(low, env, cx)?;
            let hi = eval(high, env, cx)?;
            let ge = truth(&compare_op(BinaryOp::GtEq, &v, &lo)?)?;
            let le = truth(&compare_op(BinaryOp::LtEq, &v, &hi)?)?;
            let r = and3(ge, le);
            Ok(bool_value(if *negated { not3(r) } else { r }))
        }
        Expr::Like { expr, pattern, escape, negated } => {
            let v = eval(expr, env, cx)?;
            let p = eval(pattern, env, cx)?;
            let esc = match escape {
                Some(e) => Some(eval(e, env, cx)?),
                None => None,
            };
            if v.is_null() || p.is_null() || esc.as_ref().is_some_and(Value::is_null) {
                return Ok(Value::Null);
            }
            let esc_char = match &esc {
                None => None,
                Some(e) => {
                    let s = like_text(e)?;
                    let mut cs = s.chars();
                    match (cs.next(), cs.next()) {
                        (Some(c), None) => Some(c),
                        _ => {
                            return Err(DbError::InvalidValue(
                                "LIKE ESCAPE must be a single character".into(),
                            ));
                        }
                    }
                }
            };
            let text: Vec<char> = like_text(&v)?.chars().collect();
            let pat = compile_like(like_text(&p)?, esc_char)?;
            Ok(Value::Bool(like_match(&text, &pat) != *negated))
        }
        Expr::Case { operand, whens, else_result } => {
            let op_val = match operand {
                Some(o) => Some(eval(o, env, cx)?),
                None => None,
            };
            for (cond, result) in whens {
                let hit = match &op_val {
                    Some(ov) => eq3(ov, &eval(cond, env, cx)?)? == Some(true),
                    None => eval_predicate(cond, env, cx)?,
                };
                if hit {
                    return eval(result, env, cx);
                }
            }
            match else_result {
                Some(e) => eval(e, env, cx),
                None => Ok(Value::Null),
            }
        }
        Expr::Cast { expr, ty } => Ok(eval(expr, env, cx)?.cast_to(ty)?),
        Expr::Function { name, args } => match name.as_str() {
            "COALESCE" => {
                for a in args {
                    let v = eval(a, env, cx)?;
                    if !v.is_null() {
                        return Ok(v);
                    }
                }
                Ok(Value::Null)
            }
            "NULLIF" => {
                if args.len() != 2 {
                    return Err(DbError::UndefinedFunction(format!("NULLIF with {} argument(s)", args.len())));
                }
                let a = eval(&args[0], env, cx)?;
                let b = eval(&args[1], env, cx)?;
                if eq3(&a, &b)? == Some(true) { Ok(Value::Null) } else { Ok(a) }
            }
            _ => {
                let vals = args.iter().map(|a| eval(a, env, cx)).collect::<DbResult<Vec<_>>>()?;
                call_scalar(name, &vals)
            }
        },
        Expr::Aggregate { .. } => env
            .aggregates
            .iter()
            .find(|(e, _)| e == expr)
            .map(|(_, v)| v.clone())
            .ok_or_else(|| {
                DbError::Syntax(format!(
                    "aggregate {} is not allowed in this context",
                    expr.to_sql()
                ))
            }),
        Expr::ScalarSubquery(query) => {
            let result = cx.subquery(query, env)?;
            single_column(&result)?;
            match result.rows.len() {
                0 => Ok(Value::Null),
                1 => Ok(result.rows[0][0].clone()),
                _ => Err(DbError::CardinalityViolation(
                    "more than one row returned by a subquery used as an expression".into(),
                )),
            }
        }
        Expr::Default => Err(DbError::Syntax("DEFAULT is not allowed in this context".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::parse_expression_sql;

    fn c(sql: &str) -> DbResult<Value> {
        evaluate_constant(&parse_expression_sql(sql).unwrap())
    }

    #[test]
    fn three_valued_logic() {
        assert_eq!(c("NULL AND FALSE").unwrap(), Value::Bool(false));
        assert_eq!(c("NULL OR TRUE").unwrap(), Value::Bool(true));
        assert_eq!(c("NULL AND TRUE").unwrap(), Value::Null);
        assert_eq!(c("NOT NULL").unwrap(), Value::Null);
        assert_eq!(c("NULL = NULL").unwrap(), Value::Null);
        assert_eq!(c("1 NOT IN (2, NULL)").unwrap(), Value::Null);
        assert_eq!(c("1 IN (1, NULL)").unwrap(), Value::Bool(true));
        assert_eq!(c("NULL IS UNKNOWN").unwrap(), Value::Bool(true));
    }

    #[test]
    fn errors_are_not_swallowed() {
        assert!(matches!(c("1 / 0"), Err(DbError::DivisionByZero)));
        assert!(matches!(c("1 = 'a'"), Err(DbError::TypeMismatch(_))));
        assert!(matches!(c("1 AND TRUE"), Err(DbError::TypeMismatch(_))));
        assert!(matches!(c("x + 1"), Err(DbError::ColumnNotFound(_))));
    }

    #[test]
    fn like_patterns() {
        assert_eq!(c("'abc' LIKE 'a%c'").unwrap(), Value::Bool(true));
        assert_eq!(c("'abc' LIKE '%b'").unwrap(), Value::Bool(false));
        assert_eq!(c("'a_c' LIKE 'a#_c' ESCAPE '#'").unwrap(), Value::Bool(true));
        assert_eq!(c("'abc' LIKE 'a#_c' ESCAPE '#'").unwrap(), Value::Bool(false));
        assert_eq!(c("'aXbXc' LIKE '%X%X%'").unwrap(), Value::Bool(true));
        assert!(matches!(c("1 LIKE '1'"), Err(DbError::TypeMismatch(_))));
    }

    #[test]
    fn functions_and_case() {
        assert_eq!(c("COALESCE(NULL, NULL, 3)").unwrap(), Value::Int(3));
        assert_eq!(c("NULLIF(1, 1)").unwrap(), Value::Null);
        assert_eq!(c("UPPER('ab')").unwrap(), Value::Text("AB".into()));
        assert_eq!(c("CHAR_LENGTH('héllo')").unwrap(), Value::Int(5));
        assert_eq!(c("ABS(-3)").unwrap(), Value::Int(3));
        assert_eq!(c("CASE 1 WHEN NULL THEN 1 ELSE 2 END").unwrap(), Value::Int(2));
        assert_eq!(c("6 & 3").unwrap(), Value::Int(2));
        assert_eq!(c("4 | 1").unwrap(), Value::Int(5));
        assert_eq!(c("6 ^ 3").unwrap(), Value::Int(5));
    }

    #[test]
    fn scope_resolution() {
        let mut s = Scope::new();
        s.push(Some("t".into()), "id".into(), SqlType::Integer);
        s.push(Some("u".into()), "id".into(), SqlType::Integer);
        s.push(Some("u".into()), "Name".into(), SqlType::Text);
        assert!(matches!(s.resolve(None, "id"), Err(DbError::AmbiguousColumn(_))));
        assert_eq!(s.resolve(Some("u"), "id").unwrap(), Some(1));
        assert_eq!(s.resolve(None, "name").unwrap(), Some(2));
        assert_eq!(s.resolve(None, "zzz").unwrap(), None);
    }
}
