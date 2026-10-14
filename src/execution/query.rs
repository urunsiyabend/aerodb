//! The single SELECT executor. Top-level queries, every subquery kind,
//! FROM-subqueries and set-operation branches all run through
//! [`execute_query`] / [`run_query`].

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::catalog::Catalog;
use crate::error::{DbError, DbResult};
use crate::execution::eval::{Ctx, Env, Scope, display_column, eval, eval_predicate};
use crate::execution::runtime::read_snapshot;
use crate::sql::ast::{
    AggFunc, BinaryOp, Expr, JoinType, OrderByItem, Query, Select, SelectItem, SetExpr, SetOperator,
    TableRef, UnaryOp,
};
use crate::sql::decimal::Decimal;
use crate::sql::value::{SqlType, Value};
use crate::storage::btree::BTree;

#[derive(Debug, Clone, PartialEq)]
pub struct ResultColumn {
    pub name: String,
    pub ty: SqlType,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueryResult {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<Value>>,
}

/// Execute `query`, optionally correlated with an outer environment.
pub fn execute_query(catalog: &mut Catalog, query: &Query, outer: Option<&Env>) -> DbResult<QueryResult> {
    run_query(catalog, query, outer)
}

/// Shared-borrow form of [`execute_query`] used by the evaluator for subqueries.
pub fn run_query(catalog: &Catalog, query: &Query, outer: Option<&Env>) -> DbResult<QueryResult> {
    let mut result = match &query.body {
        SetExpr::Select(sel) => run_select(catalog, sel, &query.order_by, outer)?,
        body => {
            let mut r = run_set_expr(catalog, body, outer)?;
            if !query.order_by.is_empty() {
                let keys = query
                    .order_by
                    .iter()
                    .map(|item| resolve_output_order(item, &output_names(&r.columns), None, None))
                    .collect::<DbResult<Vec<_>>>()?;
                let keyed: Vec<(Vec<Value>, Vec<Value>)> = r
                    .rows
                    .drain(..)
                    .map(|row| {
                        let k = keys.iter().map(|i| row[*i].clone()).collect();
                        (row, k)
                    })
                    .collect();
                r.rows = sort_rows(keyed, &query.order_by)?;
            }
            r
        }
    };
    let offset = query.offset.unwrap_or(0) as usize;
    if offset > 0 {
        result.rows.drain(..offset.min(result.rows.len()));
    }
    if let Some(limit) = query.limit {
        result.rows.truncate(limit as usize);
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Normalized comparison keys (DISTINCT, GROUP BY, set operations)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyPart {
    Null,
    Bool(bool),
    Num(Decimal),
    Float(u64),
    Text(String),
    Temporal(u8, i64),
}

/// Key under which values that SQL considers "not distinct" coincide: NULLs
/// group together, numerically equal Int/Decimal/Double coincide, CHAR is
/// compared without pad spaces.
pub fn key_of(v: &Value) -> KeyPart {
    match v {
        Value::Null => KeyPart::Null,
        Value::Bool(b) => KeyPart::Bool(*b),
        Value::Int(i) => KeyPart::Num(Decimal::from_i64(*i)),
        Value::Decimal(d) => KeyPart::Num(d.normalize()),
        Value::Double(f) => match Decimal::from_f64(*f) {
            Ok(d) if d.to_f64() == *f => KeyPart::Num(d.normalize()),
            _ => KeyPart::Float(f.to_bits()),
        },
        Value::Text(s) => KeyPart::Text(s.clone()),
        Value::Char(s) => KeyPart::Text(s.trim_end_matches(' ').to_string()),
        Value::Date(d) => KeyPart::Temporal(0, *d as i64),
        Value::DateTime(t) | Value::Timestamp(t) => KeyPart::Temporal(1, *t),
        Value::Time(t) => KeyPart::Temporal(2, *t as i64),
        Value::Year(y) => KeyPart::Temporal(3, *y as i64),
    }
}

pub fn row_key(row: &[Value]) -> Vec<KeyPart> {
    row.iter().map(key_of).collect()
}

// ---------------------------------------------------------------------------
// FROM / JOIN
// ---------------------------------------------------------------------------

fn load_table_ref(catalog: &Catalog, tref: &TableRef, outer: Option<&Env>) -> DbResult<(Scope, Vec<Vec<Value>>)> {
    match tref {
        TableRef::Named { name, alias } => {
            let info = catalog
                .get_table(name)
                .map_err(|_| DbError::TableNotFound(name.clone()))?
                .clone();
            let qualifier = alias.clone().unwrap_or_else(|| name.clone());
            let mut scope = Scope::new();
            for (col, ty) in &info.columns {
                scope.push(Some(qualifier.clone()), col.clone(), SqlType::from_column_type(ty));
            }
            let snapshot = read_snapshot(catalog);
            let tree = BTree::open_root(&catalog.pager, info.root_page)?;
            let rows = tree
                .scan_visible(&snapshot)?
                .into_iter()
                .map(|r| r.data.0.iter().map(Value::from_column_value).collect())
                .collect();
            Ok((scope, rows))
        }
        TableRef::Subquery { query, alias } => {
            let r = run_query(catalog, query, outer)?;
            let mut scope = Scope::new();
            for c in &r.columns {
                scope.push(Some(alias.clone()), c.name.clone(), c.ty);
            }
            Ok((scope, r.rows))
        }
    }
}

fn concat_rows(a: &[Value], b: &[Value]) -> Vec<Value> {
    let mut v = Vec::with_capacity(a.len() + b.len());
    v.extend_from_slice(a);
    v.extend_from_slice(b);
    v
}

fn build_source(catalog: &Catalog, sel: &Select, outer: Option<&Env>) -> DbResult<(Scope, Vec<Vec<Value>>)> {
    if sel.from.is_empty() {
        return Ok((Scope::new(), vec![Vec::new()]));
    }
    let cx = Ctx::with_catalog(catalog);
    let (mut scope, mut rows) = load_table_ref(catalog, &sel.from[0], outer)?;
    for tref in &sel.from[1..] {
        let (rscope, rrows) = load_table_ref(catalog, tref, outer)?;
        scope.extend(&rscope);
        let mut product = Vec::with_capacity(rows.len() * rrows.len());
        for l in &rows {
            for r in &rrows {
                product.push(concat_rows(l, r));
            }
        }
        rows = product;
    }
    for join in &sel.joins {
        let (rscope, rrows) = load_table_ref(catalog, &join.table, outer)?;
        let left_width = scope.len();
        let right_width = rscope.len();
        scope.extend(&rscope);
        let mut matched_right = vec![false; rrows.len()];
        let mut out = Vec::new();
        for l in &rows {
            let mut matched_left = false;
            for (ri, r) in rrows.iter().enumerate() {
                let candidate = concat_rows(l, r);
                let keep = match &join.predicate {
                    None => true,
                    Some(p) => eval_predicate(p, &Env::new(&scope, &candidate, outer), &cx)?,
                };
                if keep {
                    matched_left = true;
                    matched_right[ri] = true;
                    out.push(candidate);
                }
            }
            if !matched_left && matches!(join.join_type, JoinType::Left | JoinType::Full) {
                out.push(concat_rows(l, &vec![Value::Null; right_width]));
            }
        }
        if matches!(join.join_type, JoinType::Right | JoinType::Full) {
            for (ri, r) in rrows.iter().enumerate() {
                if !matched_right[ri] {
                    out.push(concat_rows(&vec![Value::Null; left_width], r));
                }
            }
        }
        rows = out;
    }
    Ok((scope, rows))
}

// ---------------------------------------------------------------------------
// Projection helpers
// ---------------------------------------------------------------------------

/// One output column: either a direct scope slot (wildcard expansion) or an
/// expression.
struct OutItem {
    expr: Expr,
    direct: Option<usize>,
    name: String,
}

fn derived_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::Literal(v) => v.to_string(),
        Expr::Aggregate { .. } => expr.to_sql(),
        Expr::ScalarSubquery(_) => "SUBQUERY".into(),
        Expr::Function { name, .. } => name.clone(),
        _ => "EXPR".into(),
    }
}

fn expand_items(sel: &Select, scope: &Scope) -> DbResult<Vec<OutItem>> {
    let mut items = Vec::new();
    for item in &sel.projection {
        match item {
            SelectItem::Wildcard => {
                for (i, c) in scope.columns.iter().enumerate() {
                    items.push(OutItem {
                        expr: Expr::Column { table: c.qualifier.clone(), name: c.name.clone() },
                        direct: Some(i),
                        name: c.name.clone(),
                    });
                }
            }
            SelectItem::QualifiedWildcard(q) => {
                let before = items.len();
                for (i, c) in scope.columns.iter().enumerate() {
                    if c.qualifier.as_deref() == Some(q.as_str()) {
                        items.push(OutItem {
                            expr: Expr::Column { table: c.qualifier.clone(), name: c.name.clone() },
                            direct: Some(i),
                            name: c.name.clone(),
                        });
                    }
                }
                if items.len() == before {
                    return Err(DbError::TableNotFound(q.clone()));
                }
            }
            SelectItem::Expr { expr, alias } => items.push(OutItem {
                expr: expr.clone(),
                direct: None,
                name: alias.clone().unwrap_or_else(|| derived_name(expr)),
            }),
        }
    }
    Ok(items)
}

/// Replace every column reference resolvable in `scope` by a positional
/// marker so qualified and unqualified spellings compare equal.
fn canonicalize(expr: &Expr, scope: &Scope) -> Expr {
    expr.transform(&mut |e| match &e {
        Expr::Column { table, name } => match scope.resolve(table.as_deref(), name) {
            Ok(Some(i)) => Expr::Column { table: Some(format!("\u{0}{}", i)), name: String::new() },
            _ => e,
        },
        _ => e,
    })
}

fn output_names(cols: &[ResultColumn]) -> Vec<String> {
    cols.iter().map(|c| c.name.clone()).collect()
}

/// Resolve an ORDER BY item to an output column (ordinal, output name, or an
/// expression identical to a select item). Returns `Err` with the standard
/// message when the item must reference the select list but does not.
fn resolve_output_order(
    item: &OrderByItem,
    names: &[String],
    item_exprs: Option<&[Expr]>,
    scope: Option<&Scope>,
) -> DbResult<usize> {
    match try_output_order(item, names, item_exprs, scope)? {
        Some(i) => Ok(i),
        None => Err(DbError::Syntax("ORDER BY expressions must appear in select list".into())),
    }
}

fn try_output_order(
    item: &OrderByItem,
    names: &[String],
    item_exprs: Option<&[Expr]>,
    scope: Option<&Scope>,
) -> DbResult<Option<usize>> {
    match &item.expr {
        Expr::Literal(Value::Int(n)) => {
            if *n >= 1 && (*n as usize) <= names.len() {
                return Ok(Some(*n as usize - 1));
            }
            return Err(DbError::Syntax(format!("ORDER BY position {} is not in select list", n)));
        }
        Expr::Column { table: None, name } => {
            let hits: Vec<usize> = names.iter().enumerate().filter(|(_, n)| *n == name).map(|(i, _)| i).collect();
            if hits.len() == 1 {
                return Ok(Some(hits[0]));
            }
            if hits.len() > 1 {
                return Err(DbError::AmbiguousColumn(name.clone()));
            }
        }
        _ => {}
    }
    if let (Some(exprs), Some(scope)) = (item_exprs, scope) {
        let target = canonicalize(&item.expr, scope);
        if let Some(i) = exprs.iter().position(|e| canonicalize(e, scope) == target) {
            return Ok(Some(i));
        }
    }
    Ok(None)
}

enum SortKey {
    Output(usize),
    Expr(Expr),
}

/// Stable sort by precomputed keys; NULLS LAST for ASC and NULLS FIRST for
/// DESC unless overridden. Comparison errors propagate.
fn sort_rows(mut keyed: Vec<(Vec<Value>, Vec<Value>)>, order: &[OrderByItem]) -> DbResult<Vec<Vec<Value>>> {
    let error: RefCell<Option<DbError>> = RefCell::new(None);
    keyed.sort_by(|(_, a), (_, b)| {
        for (i, item) in order.iter().enumerate() {
            let nulls_first = item.nulls_first.unwrap_or(item.descending);
            let ord = match (a[i].is_null(), b[i].is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if nulls_first { Ordering::Less } else { Ordering::Greater }
                }
                (false, true) => {
                    if nulls_first { Ordering::Greater } else { Ordering::Less }
                }
                (false, false) => match a[i].compare(&b[i]) {
                    Ok(Some(o)) => {
                        if item.descending { o.reverse() } else { o }
                    }
                    Ok(None) => Ordering::Equal,
                    Err(e) => {
                        error.borrow_mut().get_or_insert(e.into());
                        Ordering::Equal
                    }
                },
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
    if let Some(e) = error.into_inner() {
        return Err(e);
    }
    Ok(keyed.into_iter().map(|(row, _)| row).collect())
}

// ---------------------------------------------------------------------------
// Static typing (used when every computed value is NULL)
// ---------------------------------------------------------------------------

fn widen_numeric(a: SqlType, b: SqlType) -> SqlType {
    match (a, b) {
        (SqlType::Null, t) | (t, SqlType::Null) => t,
        (SqlType::Double, _) | (_, SqlType::Double) => SqlType::Double,
        (d @ SqlType::Decimal { .. }, _) | (_, d @ SqlType::Decimal { .. }) => d,
        (t, _) => t,
    }
}

fn static_type(expr: &Expr, scope: &Scope) -> SqlType {
    match expr {
        Expr::Literal(v) => v.sql_type(),
        Expr::Column { table, name } => match scope.resolve(table.as_deref(), name) {
            Ok(Some(i)) => scope.columns[i].ty,
            _ => SqlType::Null,
        },
        Expr::Unary { op: UnaryOp::Not, .. } => SqlType::Boolean,
        Expr::Unary { expr, .. } => static_type(expr, scope),
        Expr::Binary { op, left, right } => match op {
            BinaryOp::Concat => SqlType::Text,
            BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::BitXor => SqlType::Integer,
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                widen_numeric(static_type(left, scope), static_type(right, scope))
            }
            _ => SqlType::Boolean,
        },
        Expr::IsNull { .. }
        | Expr::IsTruth { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. } => SqlType::Boolean,
        Expr::Case { whens, else_result, .. } => whens
            .iter()
            .map(|(_, t)| t)
            .chain(else_result.iter().map(|e| e.as_ref()))
            .map(|e| static_type(e, scope))
            .find(|t| *t != SqlType::Null)
            .unwrap_or(SqlType::Null),
        Expr::Cast { ty, .. } => *ty,
        Expr::Function { name, args } => match name.as_str() {
            "UPPER" | "LOWER" => SqlType::Text,
            "CHAR_LENGTH" | "CHARACTER_LENGTH" => SqlType::Integer,
            "CURRENT_TIMESTAMP" | "GETDATE" | "GETUTCDATE" => SqlType::DateTime,
            _ => args
                .iter()
                .map(|a| static_type(a, scope))
                .find(|t| *t != SqlType::Null)
                .unwrap_or(SqlType::Null),
        },
        Expr::Aggregate { func, arg, .. } => match (func, arg) {
            (AggFunc::Count, _) => SqlType::Integer,
            (_, None) => SqlType::Null,
            (AggFunc::Avg, Some(a)) => match static_type(a, scope) {
                d @ SqlType::Decimal { .. } => d,
                SqlType::Null => SqlType::Null,
                _ => SqlType::Double,
            },
            (_, Some(a)) => static_type(a, scope),
        },
        Expr::ScalarSubquery(_) | Expr::Default => SqlType::Null,
    }
}

fn column_types(items: &[OutItem], scope: &Scope, rows: &[Vec<Value>]) -> Vec<ResultColumn> {
    items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let scoped = match item.direct {
                Some(slot) => Some(scope.columns[slot].ty),
                None => match &item.expr {
                    Expr::Column { table, name } => match scope.resolve(table.as_deref(), name) {
                        Ok(Some(slot)) => Some(scope.columns[slot].ty),
                        _ => None,
                    },
                    _ => None,
                },
            };
            let ty = scoped.unwrap_or_else(|| {
                rows.iter()
                    .map(|r| &r[i])
                    .find(|v| !v.is_null())
                    .map(Value::sql_type)
                    .unwrap_or_else(|| static_type(&item.expr, scope))
            });
            ResultColumn { name: item.name.clone(), ty }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

fn collect_aggregates(expr: &Expr, out: &mut Vec<Expr>) {
    expr.walk(&mut |e| {
        if matches!(e, Expr::Aggregate { .. }) && !out.contains(e) {
            out.push(e.clone());
        }
    });
}

/// Every column reference outside an aggregate must match a GROUP BY
/// expression (or be an outer reference).
fn check_grouped(expr: &Expr, group_canon: &[Expr], scope: &Scope) -> DbResult<()> {
    if group_canon.contains(&canonicalize(expr, scope)) {
        return Ok(());
    }
    match expr {
        Expr::Aggregate { .. } => Ok(()),
        Expr::Column { table, name } => match scope.resolve(table.as_deref(), name)? {
            Some(_) => Err(DbError::GroupByMismatch(display_column(table.as_deref(), name))),
            None => Ok(()),
        },
        _ => {
            for child in expr.children() {
                check_grouped(child, group_canon, scope)?;
            }
            Ok(())
        }
    }
}

fn numeric_sum(vals: &[Value]) -> DbResult<Value> {
    let mut acc = vals[0].clone();
    if !matches!(acc, Value::Int(_) | Value::Decimal(_) | Value::Double(_)) {
        return Err(DbError::TypeMismatch(format!("SUM/AVG is not defined for {}", acc.type_name())));
    }
    for v in &vals[1..] {
        acc = acc.add(v)?;
    }
    Ok(acc)
}

fn compute_aggregate(
    func: AggFunc,
    arg: Option<&Expr>,
    distinct: bool,
    rows: &[&Vec<Value>],
    scope: &Scope,
    outer: Option<&Env>,
    cx: &Ctx,
) -> DbResult<Value> {
    let Some(arg) = arg else {
        return Ok(Value::Int(rows.len() as i64));
    };
    let mut vals = Vec::new();
    for row in rows {
        let v = eval(arg, &Env::new(scope, row, outer), cx)?;
        if !v.is_null() {
            vals.push(v);
        }
    }
    if distinct {
        let mut seen = std::collections::HashSet::new();
        vals.retain(|v| seen.insert(key_of(v)));
    }
    if func == AggFunc::Count {
        return Ok(Value::Int(vals.len() as i64));
    }
    if vals.is_empty() {
        return Ok(Value::Null);
    }
    match func {
        AggFunc::Sum => numeric_sum(&vals),
        AggFunc::Avg => {
            let n = vals.len();
            if vals.iter().any(|v| matches!(v, Value::Double(_)))
                || vals.iter().all(|v| matches!(v, Value::Int(_)))
            {
                let mut sum = 0f64;
                for v in &vals {
                    sum += match v {
                        Value::Int(i) => *i as f64,
                        Value::Decimal(d) => d.to_f64(),
                        Value::Double(f) => *f,
                        other => {
                            return Err(DbError::TypeMismatch(format!(
                                "AVG is not defined for {}",
                                other.type_name()
                            )));
                        }
                    };
                }
                let avg = sum / n as f64;
                if !avg.is_finite() {
                    return Err(DbError::NumericOverflow("AVG result is not finite".into()));
                }
                Ok(Value::Double(avg))
            } else {
                let sum = numeric_sum(&vals)?;
                Ok(sum.div(&Value::Decimal(Decimal::from_i64(n as i64)))?)
            }
        }
        _ => {
            let want = if func == AggFunc::Min { Ordering::Less } else { Ordering::Greater };
            let mut best = vals[0].clone();
            for v in &vals[1..] {
                if v.compare(&best)? == Some(want) {
                    best = v.clone();
                }
            }
            Ok(best)
        }
    }
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

fn run_select(catalog: &Catalog, sel: &Select, order_by: &[OrderByItem], outer: Option<&Env>) -> DbResult<QueryResult> {
    let cx = Ctx::with_catalog(catalog);
    let (scope, mut rows) = build_source(catalog, sel, outer)?;

    if let Some(pred) = &sel.selection {
        if pred.contains_aggregate() {
            return Err(DbError::Syntax("aggregate functions are not allowed in WHERE".into()));
        }
        let mut kept = Vec::with_capacity(rows.len());
        for row in rows {
            if eval_predicate(pred, &Env::new(&scope, &row, outer), &cx)? {
                kept.push(row);
            }
        }
        rows = kept;
    }

    let items = expand_items(sel, &scope)?;
    let names: Vec<String> = items.iter().map(|i| i.name.clone()).collect();
    let item_exprs: Vec<Expr> = items.iter().map(|i| i.expr.clone()).collect();

    let mut sort_keys = Vec::with_capacity(order_by.len());
    for item in order_by {
        let key = if sel.distinct {
            SortKey::Output(resolve_output_order(item, &names, Some(&item_exprs), Some(&scope))?)
        } else {
            match try_output_order(item, &names, None, None)? {
                Some(i) => SortKey::Output(i),
                None => SortKey::Expr(item.expr.clone()),
            }
        };
        sort_keys.push(key);
    }

    let mut aggregates = Vec::new();
    for e in &item_exprs {
        collect_aggregates(e, &mut aggregates);
    }
    if let Some(h) = &sel.having {
        collect_aggregates(h, &mut aggregates);
    }
    for k in &sort_keys {
        if let SortKey::Expr(e) = k {
            collect_aggregates(e, &mut aggregates);
        }
    }
    let grouped = !sel.group_by.is_empty() || !aggregates.is_empty() || sel.having.is_some();

    // (output row, sort key values)
    let mut produced: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();

    let project = |env: &Env, row: &[Value]| -> DbResult<(Vec<Value>, Vec<Value>)> {
        let mut out = Vec::with_capacity(items.len());
        for item in &items {
            out.push(match item.direct {
                Some(slot) => row[slot].clone(),
                None => eval(&item.expr, env, &cx)?,
            });
        }
        let mut keys = Vec::with_capacity(sort_keys.len());
        for k in &sort_keys {
            keys.push(match k {
                SortKey::Output(i) => out[*i].clone(),
                SortKey::Expr(e) => eval(e, env, &cx)?,
            });
        }
        Ok((out, keys))
    };

    if !grouped {
        for row in &rows {
            let env = Env::new(&scope, row, outer);
            produced.push(project(&env, row)?);
        }
    } else {
        for g in &sel.group_by {
            if g.contains_aggregate() {
                return Err(DbError::Syntax("aggregate functions are not allowed in GROUP BY".into()));
            }
        }
        let group_canon: Vec<Expr> = sel.group_by.iter().map(|g| canonicalize(g, &scope)).collect();
        for e in &item_exprs {
            check_grouped(e, &group_canon, &scope)?;
        }
        if let Some(h) = &sel.having {
            check_grouped(h, &group_canon, &scope)?;
        }
        for k in &sort_keys {
            if let SortKey::Expr(e) = k {
                check_grouped(e, &group_canon, &scope)?;
            }
        }
        if aggregates.is_empty() {
            // AeroDB 0.5.1 rule kept: a GROUP BY without any aggregate must not
            // group by expressions that are absent from the select list.
            let item_canon: Vec<Expr> = item_exprs.iter().map(|e| canonicalize(e, &scope)).collect();
            for (g, gc) in sel.group_by.iter().zip(&group_canon) {
                if !item_canon.contains(gc) {
                    let name = match g {
                        Expr::Column { table, name } => display_column(table.as_deref(), name),
                        other => other.to_sql(),
                    };
                    return Err(DbError::GroupByMismatch(name));
                }
            }
        }

        // Group rows by normalized key, preserving first-appearance order.
        let mut groups: Vec<Vec<&Vec<Value>>> = Vec::new();
        let mut index: HashMap<Vec<KeyPart>, usize> = HashMap::new();
        if sel.group_by.is_empty() {
            groups.push(rows.iter().collect());
        } else {
            for row in &rows {
                let env = Env::new(&scope, row, outer);
                let mut key = Vec::with_capacity(sel.group_by.len());
                for g in &sel.group_by {
                    key.push(key_of(&eval(g, &env, &cx)?));
                }
                match index.get(&key) {
                    Some(&gi) => groups[gi].push(row),
                    None => {
                        index.insert(key, groups.len());
                        groups.push(vec![row]);
                    }
                }
            }
        }

        let null_row = vec![Value::Null; scope.len()];
        for group in &groups {
            let mut agg_values = Vec::with_capacity(aggregates.len());
            for a in &aggregates {
                if let Expr::Aggregate { func, arg, distinct } = a {
                    let v = compute_aggregate(*func, arg.as_deref(), *distinct, group, &scope, outer, &cx)?;
                    agg_values.push((a.clone(), v));
                }
            }
            let rep: &[Value] = group.first().map(|r| r.as_slice()).unwrap_or(&null_row);
            let env = Env { scope: &scope, row: rep, aggregates: &agg_values, outer };
            if let Some(h) = &sel.having {
                if !eval_predicate(h, &env, &cx)? {
                    continue;
                }
            }
            produced.push(project(&env, rep)?);
        }
    }

    if sel.distinct {
        let mut seen = std::collections::HashSet::new();
        produced.retain(|(row, _)| seen.insert(row_key(row)));
    }

    let rows_out = if order_by.is_empty() {
        produced.into_iter().map(|(r, _)| r).collect()
    } else {
        sort_rows(produced, order_by)?
    };
    let columns = column_types(&items, &scope, &rows_out);
    Ok(QueryResult { columns, rows: rows_out })
}

// ---------------------------------------------------------------------------
// Set operations
// ---------------------------------------------------------------------------

fn category(t: &SqlType) -> u8 {
    match t {
        SqlType::Null => 0,
        t if t.is_numeric() => 1,
        t if t.is_text() => 2,
        SqlType::Boolean => 3,
        SqlType::Date => 4,
        SqlType::DateTime | SqlType::Timestamp => 5,
        SqlType::Time => 6,
        _ => 7, // Year
    }
}

fn run_set_expr(catalog: &Catalog, body: &SetExpr, outer: Option<&Env>) -> DbResult<QueryResult> {
    match body {
        SetExpr::Select(sel) => run_select(catalog, sel, &[], outer),
        SetExpr::SetOp { op, all, left, right } => {
            let l = run_set_expr(catalog, left, outer)?;
            let r = run_set_expr(catalog, right, outer)?;
            if l.columns.len() != r.columns.len() {
                return Err(DbError::Syntax(format!(
                    "each {} query must have the same number of columns",
                    op.as_str()
                )));
            }
            let mut columns = Vec::with_capacity(l.columns.len());
            for (lc, rc) in l.columns.iter().zip(&r.columns) {
                let (cl, cr) = (category(&lc.ty), category(&rc.ty));
                if cl != 0 && cr != 0 && cl != cr {
                    return Err(DbError::TypeMismatch(format!(
                        "{} types {} and {} cannot be matched",
                        op.as_str(),
                        lc.ty.name(),
                        rc.ty.name()
                    )));
                }
                let ty = if cl == 1 && cr == 1 {
                    if lc.ty == rc.ty { lc.ty } else { widen_numeric(lc.ty, rc.ty) }
                } else if cl == 0 {
                    rc.ty
                } else {
                    lc.ty
                };
                columns.push(ResultColumn { name: lc.name.clone(), ty });
            }
            let rows = combine(*op, *all, l.rows, r.rows);
            Ok(QueryResult { columns, rows })
        }
    }
}

fn combine(op: SetOperator, all: bool, left: Vec<Vec<Value>>, right: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    use std::collections::HashSet;
    match (op, all) {
        (SetOperator::Union, true) => {
            let mut out = left;
            out.extend(right);
            out
        }
        (SetOperator::Union, false) => {
            let mut seen = HashSet::new();
            left.into_iter()
                .chain(right)
                .filter(|row| seen.insert(row_key(row)))
                .collect()
        }
        (_, false) => {
            let right_keys: HashSet<Vec<KeyPart>> = right.iter().map(|r| row_key(r)).collect();
            let want_present = op == SetOperator::Intersect;
            let mut seen = HashSet::new();
            left.into_iter()
                .filter(|row| {
                    let k = row_key(row);
                    right_keys.contains(&k) == want_present && seen.insert(k)
                })
                .collect()
        }
        (_, true) => {
            let mut counts: HashMap<Vec<KeyPart>, usize> = HashMap::new();
            for r in &right {
                *counts.entry(row_key(r)).or_default() += 1;
            }
            let keep_matched = op == SetOperator::Intersect;
            left.into_iter()
                .filter(|row| {
                    let matched = match counts.get_mut(&row_key(row)) {
                        Some(c) if *c > 0 => {
                            *c -= 1;
                            true
                        }
                        _ => false,
                    };
                    matched == keep_matched
                })
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_unify_numeric_kinds_and_char_padding() {
        let d = Value::Decimal(Decimal::parse("1.00").unwrap());
        assert_eq!(key_of(&Value::Int(1)), key_of(&d));
        assert_eq!(key_of(&Value::Double(1.0)), key_of(&d));
        assert_ne!(key_of(&Value::Double(1.5)), key_of(&Value::Int(1)));
        assert_eq!(key_of(&Value::Char("a  ".into())), key_of(&Value::Text("a".into())));
        assert_eq!(key_of(&Value::Null), key_of(&Value::Null));
    }

    #[test]
    fn multiset_set_operations() {
        let rows = |v: &[i64]| v.iter().map(|i| vec![Value::Int(*i)]).collect::<Vec<_>>();
        let ints = |r: Vec<Vec<Value>>| {
            r.into_iter()
                .map(|row| match &row[0] {
                    Value::Int(i) => *i,
                    _ => panic!(),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(ints(combine(SetOperator::Except, true, rows(&[1, 1, 2, 3]), rows(&[1, 3]))), vec![1, 2]);
        assert_eq!(ints(combine(SetOperator::Intersect, true, rows(&[1, 1, 2]), rows(&[1, 1, 1]))), vec![1, 1]);
        assert_eq!(ints(combine(SetOperator::Except, false, rows(&[1, 1, 2]), rows(&[2]))), vec![1]);
        assert_eq!(ints(combine(SetOperator::Union, false, rows(&[1, 1]), rows(&[2, 1]))), vec![1, 2]);
    }
}
