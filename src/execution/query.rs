//! The single SELECT executor. Top-level queries, every subquery kind,
//! FROM-subqueries and set-operation branches all run through
//! [`execute_query`] / [`run_query`].

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::catalog::{Catalog, Relation};
use crate::error::{DbError, DbResult};
use crate::execution::eval::{Ctx, Env, Scope, display_column, eval, eval_predicate};
use crate::execution::runtime::read_snapshot;
use crate::sql::ast::{
    AggFunc, BinaryOp, Expr, Ident, JoinType, OrderByItem, Query, Select, SelectItem, SetExpr,
    SetOperator, TableRef, UnaryOp,
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

/// Execute `query`, optionally correlated with an outer environment. The
/// whole query (subqueries, views and FROM-subqueries included) is first
/// bound statically ([`crate::execution::bind`]), so type-category errors
/// are reported before any row is read; the bound form (character literals
/// standing for date/time values converted) is what runs.
pub fn execute_query(
    catalog: &mut Catalog,
    query: &Query,
    outer: Option<&Env>,
) -> DbResult<QueryResult> {
    let mut bound = query.clone();
    crate::execution::bind::bind_query(catalog, &mut bound, outer)?;
    run_query(catalog, &bound, outer)
}

/// Shared-borrow form of [`execute_query`] used by the evaluator for
/// subqueries and by view / FROM-subquery loading; the enclosing statement
/// has already been bound.
pub fn run_query(catalog: &Catalog, query: &Query, outer: Option<&Env>) -> DbResult<QueryResult> {
    let mut result = match &query.body {
        SetExpr::Select(sel) => run_select(catalog, sel, &query.order_by, outer)?.0,
        body => {
            let (mut r, matchable) = run_set_expr(catalog, body, outer)?;
            if !query.order_by.is_empty() {
                let names = output_names(&r.columns, &matchable);
                let keys = query
                    .order_by
                    .iter()
                    .map(|item| resolve_output_order(item, &names, None, None))
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

fn load_table_ref(
    catalog: &Catalog,
    tref: &TableRef,
    outer: Option<&Env>,
) -> DbResult<(Scope, Vec<Vec<Value>>)> {
    match tref {
        TableRef::Named {
            schema: Some(schema),
            name,
            alias,
        } => {
            let view = crate::execution::information_schema::resolve(schema, name)?;
            let qualifier = alias.clone().unwrap_or_else(|| view.name.to_string());
            let mut scope = Scope::new();
            for (col, ty) in view.columns {
                scope.push(Some(qualifier.clone()), col.to_string(), *ty);
            }
            Ok((
                scope,
                crate::execution::information_schema::rows(catalog, view)?,
            ))
        }
        TableRef::Named {
            schema: None,
            name,
            alias,
        } => match catalog.resolve_relation(name)? {
            Relation::Table(info) => {
                let qualifier = alias.clone().unwrap_or_else(|| info.name.clone());
                let mut scope = Scope::new();
                for (col, ty) in &info.columns {
                    scope.push(
                        Some(qualifier.clone()),
                        col.clone(),
                        SqlType::from_column_type(ty),
                    );
                }
                let snapshot = read_snapshot(catalog);
                let tree = BTree::open_root(&catalog.pager, info.root_page)?;
                let width = info.columns.len();
                let rows = tree
                    .scan_visible(&snapshot)?
                    .into_iter()
                    .map(|r| {
                        // Versions written before an ADD COLUMN are short: pad with NULL.
                        let mut vals: Vec<Value> =
                            r.data.0.iter().map(Value::from_column_value).collect();
                        vals.resize(width, Value::Null);
                        vals
                    })
                    .collect();
                Ok((scope, rows))
            }
            Relation::View(view) => {
                // A view is its stored query, run through this same executor
                // (uncorrelated: a view never sees the referencing query's scope),
                // in its bound form.
                let query = crate::execution::bind::bound_view_query(catalog, &view.query)?;
                let r = run_query(catalog, &query, None)?;
                if r.columns.len() != view.columns.len() {
                    return Err(DbError::ColumnNotFound(format!(
                        "view \"{}\" defines {} columns but its query now returns {}",
                        view.name,
                        view.columns.len(),
                        r.columns.len()
                    )));
                }
                let qualifier = alias.clone().unwrap_or_else(|| view.name.clone());
                let mut scope = Scope::new();
                for (name, col) in view.columns.iter().zip(&r.columns) {
                    scope.push(Some(qualifier.clone()), name.clone(), col.ty);
                }
                Ok((scope, r.rows))
            }
        },
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

fn build_source(
    catalog: &Catalog,
    sel: &Select,
    outer: Option<&Env>,
) -> DbResult<(Scope, Vec<Vec<Value>>)> {
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
    /// Whether ORDER BY may refer to this column by `name`: only explicit
    /// aliases and plain column references (incl. wildcard expansion), never
    /// derived names such as literal text, function names or `EXPR`.
    order_name: bool,
}

/// Column reference that resolves exactly to scope slot `c`.
fn exact_column(c: &crate::execution::eval::ScopeColumn) -> Expr {
    Expr::Column {
        table: c.qualifier.clone().map(Ident::quoted),
        name: Ident::quoted(c.name.clone()),
    }
}

fn derived_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.value.clone(),
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
                        expr: exact_column(c),
                        direct: Some(i),
                        name: c.name.clone(),
                        order_name: true,
                    });
                }
            }
            SelectItem::QualifiedWildcard(q) => {
                let before = items.len();
                for (i, c) in scope.columns.iter().enumerate() {
                    if c.qualifier.as_deref().is_some_and(|cq| q.matches(cq)) {
                        items.push(OutItem {
                            expr: exact_column(c),
                            direct: Some(i),
                            name: c.name.clone(),
                            order_name: true,
                        });
                    }
                }
                if items.len() == before {
                    return Err(DbError::TableNotFound(q.value.clone()));
                }
            }
            SelectItem::Expr { expr, alias } => items.push(OutItem {
                expr: expr.clone(),
                direct: None,
                name: alias.clone().unwrap_or_else(|| derived_name(expr)),
                order_name: alias.is_some() || matches!(expr, Expr::Column { .. }),
            }),
        }
    }
    Ok(items)
}

/// Replace every column reference resolvable in `scope` by a positional
/// marker so qualified and unqualified spellings compare equal.
fn canonicalize(expr: &Expr, scope: &Scope) -> Expr {
    expr.transform(&mut |e| match &e {
        Expr::Column { table, name } => match scope.resolve(table.as_ref(), name) {
            Ok(Some(i)) => slot_marker(i),
            _ => e,
        },
        _ => e,
    })
}

/// Canonical form of a column reference resolved to scope slot `i`.
fn slot_marker(i: usize) -> Expr {
    Expr::Column {
        table: Some(Ident::quoted(format!("\u{0}{}", i))),
        name: Ident::quoted(""),
    }
}

/// Output names paired with whether ORDER BY may match them by name.
type OutputNames = [(String, bool)];

fn output_names(cols: &[ResultColumn], matchable: &[bool]) -> Vec<(String, bool)> {
    cols.iter()
        .zip(matchable)
        .map(|(c, m)| (c.name.clone(), *m))
        .collect()
}

/// Resolve an ORDER BY item to an output column (ordinal, output name, or an
/// expression identical to a select item). Returns `Err` with the standard
/// message when the item must reference the select list but does not.
fn resolve_output_order(
    item: &OrderByItem,
    names: &OutputNames,
    item_exprs: Option<&[Expr]>,
    scope: Option<&Scope>,
) -> DbResult<usize> {
    match try_output_order(item, names, item_exprs, scope)? {
        Some(i) => Ok(i),
        None => Err(DbError::Syntax(
            "ORDER BY expressions must appear in select list".into(),
        )),
    }
}

fn try_output_order(
    item: &OrderByItem,
    names: &OutputNames,
    item_exprs: Option<&[Expr]>,
    scope: Option<&Scope>,
) -> DbResult<Option<usize>> {
    match &item.expr {
        Expr::Literal(Value::Int(n)) => {
            if *n >= 1 && (*n as usize) <= names.len() {
                return Ok(Some(*n as usize - 1));
            }
            return Err(DbError::Syntax(format!(
                "ORDER BY position {} is not in select list",
                n
            )));
        }
        Expr::Column { table: None, name } => {
            let hits: Vec<usize> = names
                .iter()
                .enumerate()
                .filter(|(_, (n, matchable))| *matchable && name.matches(n))
                .map(|(i, _)| i)
                .collect();
            if hits.len() == 1 {
                return Ok(Some(hits[0]));
            }
            if hits.len() > 1 {
                return Err(DbError::AmbiguousColumn(name.value.clone()));
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
fn sort_rows(
    mut keyed: Vec<(Vec<Value>, Vec<Value>)>,
    order: &[OrderByItem],
) -> DbResult<Vec<Vec<Value>>> {
    let error: RefCell<Option<DbError>> = RefCell::new(None);
    keyed.sort_by(|(_, a), (_, b)| {
        for (i, item) in order.iter().enumerate() {
            let nulls_first = item.nulls_first.unwrap_or(item.descending);
            let ord = match (a[i].is_null(), b[i].is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (false, true) => {
                    if nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (false, false) => match a[i].compare(&b[i]) {
                    Ok(Some(o)) => {
                        if item.descending {
                            o.reverse()
                        } else {
                            o
                        }
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
        Expr::Column { table, name } => match scope.resolve(table.as_ref(), name) {
            Ok(Some(i)) => scope.columns[i].ty,
            _ => SqlType::Null,
        },
        Expr::Unary {
            op: UnaryOp::Not, ..
        } => SqlType::Boolean,
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
        Expr::Case {
            whens, else_result, ..
        } => whens
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
                    Expr::Column { table, name } => match scope.resolve(table.as_ref(), name) {
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
            ResultColumn {
                name: item.name.clone(),
                ty,
            }
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

/// What a grouped query's expressions are validated against.
struct GroupCheck<'a> {
    catalog: &'a Catalog,
    scope: &'a Scope,
    group_canon: &'a [Expr],
}

impl GroupCheck<'_> {
    /// Every column reference outside an aggregate must match a GROUP BY
    /// expression (or be an outer reference). Subqueries are checked too:
    /// they are evaluated once per group, so any reference they make to this
    /// query's columns must be to a grouped column.
    fn check(&self, expr: &Expr) -> DbResult<()> {
        if self.group_canon.contains(&canonicalize(expr, self.scope)) {
            return Ok(());
        }
        match expr {
            Expr::Aggregate { .. } => Ok(()),
            Expr::Column { table, name } => self.check_column(table.as_ref(), name),
            Expr::ScalarSubquery(q) | Expr::Exists { query: q, .. } => {
                self.check_query(q, &mut Vec::new())
            }
            Expr::InSubquery { expr, query, .. } => {
                self.check(expr)?;
                self.check_query(query, &mut Vec::new())
            }
            _ => {
                for child in expr.children() {
                    self.check(child)?;
                }
                Ok(())
            }
        }
    }

    /// A reference that resolves in the grouped scope must be a grouped column.
    fn check_column(&self, table: Option<&Ident>, name: &Ident) -> DbResult<()> {
        match self.scope.resolve(table, name)? {
            Some(slot) if !self.group_canon.contains(&slot_marker(slot)) => Err(
                DbError::GroupByMismatch(display_column(table.map(Ident::as_str), name)),
            ),
            _ => Ok(()),
        }
    }

    /// Walk a nested query; `stack` holds the scopes of the subqueries
    /// between it and the grouped query (innermost last).
    fn check_query(&self, query: &Query, stack: &mut Vec<Scope>) -> DbResult<()> {
        self.check_body(&query.body, &query.order_by, stack)
    }

    fn check_body(
        &self,
        body: &SetExpr,
        order_by: &[OrderByItem],
        stack: &mut Vec<Scope>,
    ) -> DbResult<()> {
        match body {
            // A set operation's ORDER BY names output columns only.
            SetExpr::SetOp { left, right, .. } => {
                self.check_body(left, &[], stack)?;
                self.check_body(right, &[], stack)
            }
            SetExpr::Select(sel) => {
                // FROM-subqueries see the enclosing scopes, not this SELECT's.
                for tref in sel.from.iter().chain(sel.joins.iter().map(|j| &j.table)) {
                    if let TableRef::Subquery { query, .. } = tref {
                        self.check_query(query, stack)?;
                    }
                }
                let scope = static_source_scope(self.catalog, sel)?;
                let items = expand_items(sel, &scope)?;
                stack.push(scope);
                let mut exprs: Vec<&Expr> = items.iter().map(|i| &i.expr).collect();
                exprs.extend(sel.joins.iter().filter_map(|j| j.predicate.as_ref()));
                exprs.extend(sel.selection.iter());
                exprs.extend(sel.group_by.iter());
                exprs.extend(sel.having.iter());
                for o in order_by {
                    let output_name = match &o.expr {
                        Expr::Column { table: None, name } => {
                            items.iter().any(|i| i.order_name && name.matches(&i.name))
                        }
                        _ => false,
                    };
                    if !output_name {
                        exprs.push(&o.expr);
                    }
                }
                let result = exprs
                    .into_iter()
                    .try_for_each(|e| self.check_nested_expr(e, stack));
                stack.pop();
                result
            }
        }
    }

    fn check_nested_expr(&self, expr: &Expr, stack: &mut Vec<Scope>) -> DbResult<()> {
        match expr {
            Expr::Column { table, name } => {
                for s in stack.iter().rev() {
                    if s.resolve(table.as_ref(), name)?.is_some() {
                        return Ok(());
                    }
                }
                self.check_column(table.as_ref(), name)
            }
            Expr::ScalarSubquery(q) | Expr::Exists { query: q, .. } => self.check_query(q, stack),
            Expr::InSubquery { expr, query, .. } => {
                self.check_nested_expr(expr, stack)?;
                self.check_query(query, stack)
            }
            _ => expr
                .children()
                .into_iter()
                .try_for_each(|c| self.check_nested_expr(c, stack)),
        }
    }
}

/// Replace `*` and `q.*` in the select lists that determine `query`'s output
/// (every set-operation branch, and the FROM-subqueries feeding them) by
/// explicit, exactly-quoted column references. CREATE VIEW stores the
/// expanded text, so a view keeps the column set it had when it was created
/// even if a table gains columns later (ALTER TABLE ... ADD COLUMN).
/// Subqueries inside expressions (IN / EXISTS / scalar, at any depth) are
/// expanded too: `x IN (SELECT * FROM t2)` must keep one column.
pub fn expand_wildcards(catalog: &Catalog, query: &mut Query) -> DbResult<()> {
    expand_body_wildcards(catalog, &mut query.body)?;
    for item in &mut query.order_by {
        expand_expr_wildcards(catalog, &mut item.expr)?;
    }
    Ok(())
}

/// Expand wildcards in every subquery nested in `expr`.
fn expand_expr_wildcards(catalog: &Catalog, expr: &mut Expr) -> DbResult<()> {
    let mut failure = None;
    let rebuilt = expr.transform(&mut |e| {
        let expand = |q: &Query, failure: &mut Option<DbError>| -> Box<Query> {
            let mut q = q.clone();
            if failure.is_none()
                && let Err(err) = expand_wildcards(catalog, &mut q)
            {
                *failure = Some(err);
            }
            Box::new(q)
        };
        match e {
            Expr::ScalarSubquery(q) => Expr::ScalarSubquery(expand(&q, &mut failure)),
            Expr::Exists { query, negated } => Expr::Exists {
                query: expand(&query, &mut failure),
                negated,
            },
            Expr::InSubquery {
                expr,
                query,
                negated,
            } => Expr::InSubquery {
                expr,
                query: expand(&query, &mut failure),
                negated,
            },
            other => other,
        }
    });
    if let Some(err) = failure {
        return Err(err);
    }
    *expr = rebuilt;
    Ok(())
}

fn expand_body_wildcards(catalog: &Catalog, body: &mut SetExpr) -> DbResult<()> {
    match body {
        SetExpr::SetOp { left, right, .. } => {
            expand_body_wildcards(catalog, left)?;
            expand_body_wildcards(catalog, right)
        }
        SetExpr::Select(sel) => {
            for tref in sel
                .from
                .iter_mut()
                .chain(sel.joins.iter_mut().map(|j| &mut j.table))
            {
                if let TableRef::Subquery { query, .. } = tref {
                    expand_wildcards(catalog, query)?;
                }
            }
            let Select {
                projection,
                joins,
                selection,
                group_by,
                having,
                ..
            } = &mut **sel;
            let item_exprs = projection.iter_mut().filter_map(|i| match i {
                SelectItem::Expr { expr, .. } => Some(expr),
                _ => None,
            });
            for e in item_exprs
                .chain(joins.iter_mut().filter_map(|j| j.predicate.as_mut()))
                .chain(selection.as_mut())
                .chain(group_by.iter_mut())
                .chain(having.as_mut())
            {
                expand_expr_wildcards(catalog, e)?;
            }
            if !sel
                .projection
                .iter()
                .any(|i| !matches!(i, SelectItem::Expr { .. }))
            {
                return Ok(());
            }
            let scope = static_source_scope(catalog, sel)?;
            let mut projection = Vec::with_capacity(sel.projection.len());
            for item in &sel.projection {
                match item {
                    SelectItem::Wildcard => {
                        projection.extend(scope.columns.iter().map(|c| SelectItem::Expr {
                            expr: exact_column(c),
                            alias: None,
                        }));
                    }
                    SelectItem::QualifiedWildcard(q) => {
                        let before = projection.len();
                        projection.extend(
                            scope
                                .columns
                                .iter()
                                .filter(|c| c.qualifier.as_deref().is_some_and(|cq| q.matches(cq)))
                                .map(|c| SelectItem::Expr {
                                    expr: exact_column(c),
                                    alias: None,
                                }),
                        );
                        if projection.len() == before {
                            return Err(DbError::TableNotFound(q.value.clone()));
                        }
                    }
                    other => projection.push(other.clone()),
                }
            }
            sel.projection = projection;
            Ok(())
        }
    }
}

/// Columns a SELECT's FROM clause exposes, from catalog metadata only (no
/// rows are read; types are not needed for name resolution).
fn static_source_scope(catalog: &Catalog, sel: &Select) -> DbResult<Scope> {
    let mut scope = Scope::new();
    for tref in sel.from.iter().chain(sel.joins.iter().map(|j| &j.table)) {
        match tref {
            TableRef::Named {
                schema: Some(schema),
                name,
                alias,
            } => {
                let view = crate::execution::information_schema::resolve(schema, name)?;
                let qualifier = alias.clone().unwrap_or_else(|| view.name.to_string());
                for (col, _) in view.columns {
                    scope.push(Some(qualifier.clone()), col.to_string(), SqlType::Null);
                }
            }
            TableRef::Named {
                schema: None,
                name,
                alias,
            } => {
                let (rel_name, columns): (&str, Vec<&String>) =
                    match catalog.resolve_relation(name)? {
                        Relation::Table(info) => {
                            (&info.name, info.columns.iter().map(|(c, _)| c).collect())
                        }
                        Relation::View(view) => (&view.name, view.columns.iter().collect()),
                    };
                let qualifier = alias.clone().unwrap_or_else(|| rel_name.to_string());
                for col in columns {
                    scope.push(Some(qualifier.clone()), col.clone(), SqlType::Null);
                }
            }
            TableRef::Subquery { query, alias } => {
                for (n, _) in static_output_names(catalog, &query.body)? {
                    scope.push(Some(alias.clone()), n, SqlType::Null);
                }
            }
        }
    }
    Ok(scope)
}

fn static_output_names(catalog: &Catalog, body: &SetExpr) -> DbResult<Vec<(String, bool)>> {
    match body {
        SetExpr::SetOp { left, .. } => static_output_names(catalog, left),
        SetExpr::Select(sel) => {
            let scope = static_source_scope(catalog, sel)?;
            Ok(expand_items(sel, &scope)?
                .into_iter()
                .map(|i| (i.name, i.order_name))
                .collect())
        }
    }
}

/// An aggregate whose argument references only outer-query columns belongs to
/// the outer query in standard SQL; evaluating it over this query's rows would
/// be wrong, so it is rejected.
fn check_aggregate_level(agg: &Expr, scope: &Scope, outer: Option<&Env>) -> DbResult<()> {
    let Expr::Aggregate { arg: Some(arg), .. } = agg else {
        return Ok(());
    };
    let mut refs = Vec::new();
    arg.walk(&mut |e| {
        if let Expr::Column { table, name } = e {
            refs.push((table.clone(), name.clone()));
        }
    });
    if refs.is_empty() {
        return Ok(());
    }
    for (table, name) in &refs {
        if scope.resolve(table.as_ref(), name)?.is_some() {
            return Ok(());
        }
    }
    let outer_only = refs
        .iter()
        .all(|(table, name)| outer.is_some_and(|o| o.resolves(table.as_ref(), name)));
    if outer_only {
        return Err(DbError::FeatureNotSupported(format!(
            "aggregate {} over outer-query columns only",
            agg.to_sql()
        )));
    }
    Ok(())
}

fn numeric_sum(vals: &[Value]) -> DbResult<Value> {
    let mut acc = vals[0].clone();
    if !matches!(acc, Value::Int(_) | Value::Decimal(_) | Value::Double(_)) {
        return Err(DbError::TypeMismatch(format!(
            "SUM/AVG is not defined for {}",
            acc.type_name()
        )));
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
            let want = if func == AggFunc::Min {
                Ordering::Less
            } else {
                Ordering::Greater
            };
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

/// Run one SELECT; also returns, per output column, whether ORDER BY of an
/// enclosing set operation may refer to it by name.
fn run_select(
    catalog: &Catalog,
    sel: &Select,
    order_by: &[OrderByItem],
    outer: Option<&Env>,
) -> DbResult<(QueryResult, Vec<bool>)> {
    let cx = Ctx::with_catalog(catalog);
    let (scope, mut rows) = build_source(catalog, sel, outer)?;

    if let Some(pred) = &sel.selection {
        if pred.contains_aggregate() {
            return Err(DbError::Syntax(
                "aggregate functions are not allowed in WHERE".into(),
            ));
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
    let names: Vec<(String, bool)> = items
        .iter()
        .map(|i| (i.name.clone(), i.order_name))
        .collect();
    let item_exprs: Vec<Expr> = items.iter().map(|i| i.expr.clone()).collect();

    let mut sort_keys = Vec::with_capacity(order_by.len());
    for item in order_by {
        let key = if sel.distinct {
            SortKey::Output(resolve_output_order(
                item,
                &names,
                Some(&item_exprs),
                Some(&scope),
            )?)
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
    for a in &aggregates {
        check_aggregate_level(a, &scope, outer)?;
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
        // An integer literal in GROUP BY is a select-list ordinal.
        let mut group_by = Vec::with_capacity(sel.group_by.len());
        for g in &sel.group_by {
            let g = match g {
                Expr::Literal(Value::Int(n)) => {
                    if *n >= 1 && (*n as usize) <= items.len() {
                        item_exprs[*n as usize - 1].clone()
                    } else {
                        return Err(DbError::Syntax(format!(
                            "GROUP BY position {} is not in select list",
                            n
                        )));
                    }
                }
                other => other.clone(),
            };
            if g.contains_aggregate() {
                return Err(DbError::Syntax(
                    "aggregate functions are not allowed in GROUP BY".into(),
                ));
            }
            group_by.push(g);
        }
        let group_canon: Vec<Expr> = group_by.iter().map(|g| canonicalize(g, &scope)).collect();
        let checker = GroupCheck {
            catalog,
            scope: &scope,
            group_canon: &group_canon,
        };
        for e in &item_exprs {
            checker.check(e)?;
        }
        if let Some(h) = &sel.having {
            checker.check(h)?;
        }
        for k in &sort_keys {
            if let SortKey::Expr(e) = k {
                checker.check(e)?;
            }
        }

        // Group rows by normalized key, preserving first-appearance order.
        let mut groups: Vec<Vec<&Vec<Value>>> = Vec::new();
        let mut index: HashMap<Vec<KeyPart>, usize> = HashMap::new();
        if group_by.is_empty() {
            groups.push(rows.iter().collect());
        } else {
            for row in &rows {
                let env = Env::new(&scope, row, outer);
                let mut key = Vec::with_capacity(group_by.len());
                for g in &group_by {
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
                if let Expr::Aggregate {
                    func,
                    arg,
                    distinct,
                } = a
                {
                    let v = compute_aggregate(
                        *func,
                        arg.as_deref(),
                        *distinct,
                        group,
                        &scope,
                        outer,
                        &cx,
                    )?;
                    agg_values.push((a.clone(), v));
                }
            }
            let rep: &[Value] = group.first().map(|r| r.as_slice()).unwrap_or(&null_row);
            let env = Env {
                scope: &scope,
                row: rep,
                aggregates: &agg_values,
                outer,
            };
            if let Some(h) = &sel.having
                && !eval_predicate(h, &env, &cx)?
            {
                continue;
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
    let matchable = items.iter().map(|i| i.order_name).collect();
    Ok((
        QueryResult {
            columns,
            rows: rows_out,
        },
        matchable,
    ))
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

fn run_set_expr(
    catalog: &Catalog,
    body: &SetExpr,
    outer: Option<&Env>,
) -> DbResult<(QueryResult, Vec<bool>)> {
    match body {
        SetExpr::Select(sel) => run_select(catalog, sel, &[], outer),
        SetExpr::SetOp {
            op,
            all,
            left,
            right,
        } => {
            let (l, matchable) = run_set_expr(catalog, left, outer)?;
            let (r, _) = run_set_expr(catalog, right, outer)?;
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
                    if lc.ty == rc.ty {
                        lc.ty
                    } else {
                        widen_numeric(lc.ty, rc.ty)
                    }
                } else if cl == 0 {
                    rc.ty
                } else {
                    lc.ty
                };
                columns.push(ResultColumn {
                    name: lc.name.clone(),
                    ty,
                });
            }
            let rows = combine(*op, *all, l.rows, r.rows);
            Ok((QueryResult { columns, rows }, matchable))
        }
    }
}

fn combine(
    op: SetOperator,
    all: bool,
    left: Vec<Vec<Value>>,
    right: Vec<Vec<Value>>,
) -> Vec<Vec<Value>> {
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
        assert_eq!(
            key_of(&Value::Char("a  ".into())),
            key_of(&Value::Text("a".into()))
        );
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
        assert_eq!(
            ints(combine(
                SetOperator::Except,
                true,
                rows(&[1, 1, 2, 3]),
                rows(&[1, 3])
            )),
            vec![1, 2]
        );
        assert_eq!(
            ints(combine(
                SetOperator::Intersect,
                true,
                rows(&[1, 1, 2]),
                rows(&[1, 1, 1])
            )),
            vec![1, 1]
        );
        assert_eq!(
            ints(combine(
                SetOperator::Except,
                false,
                rows(&[1, 1, 2]),
                rows(&[2])
            )),
            vec![1]
        );
        assert_eq!(
            ints(combine(
                SetOperator::Union,
                false,
                rows(&[1, 1]),
                rows(&[2, 1])
            )),
            vec![1, 2]
        );
    }
}
