//! Static expression binding: a type-category check over every expression of
//! a statement, run once before any row is read or evaluated.
//!
//! The evaluator is lazy (CASE / COALESCE / AND / OR evaluate only what they
//! need) and NULL-tolerant, so a category error inside an unused branch or
//! next to a NULL operand would otherwise depend on the data. Binding derives
//! a static type for every expression — literal types, column types from the
//! scope (tables, views, FROM-subqueries, correlated outer queries), CAST
//! targets and function / aggregate result types — and rejects incompatible
//! operands with `TypeMismatch` (42804) whatever the rows are.
//!
//! Categories: numeric (INTEGER, SMALLINT, BIGINT, DECIMAL, DOUBLE),
//! character (TEXT, CHAR, VARCHAR), boolean, date, date-time (DATETIME,
//! TIMESTAMP), time, year, and *unknown* (an untyped NULL), which is
//! compatible with everything.
//!
//! | construct | rule |
//! |---|---|
//! | `=` `<>` `<` `<=` `>` `>=`, IN list / IN subquery, BETWEEN, simple CASE operand vs WHEN, NULLIF | operands comparable: same category, or a character *literal* against a date/time kind (implicit literal coercion, e.g. `d = '2024-01-02'`) |
//! | CASE THEN/ELSE, COALESCE arguments | one common category (same rule; numeric kinds widen INTEGER < DECIMAL < DOUBLE) |
//! | `+ - * / %`, unary `+ -`, SUM, AVG, ABS | numeric |
//! | `& \| ^` | integer kinds |
//! | AND, OR, NOT, IS [NOT] TRUE/FALSE/UNKNOWN, searched CASE WHEN, WHERE, HAVING, JOIN ON, CHECK | boolean |
//! | LIKE operand, pattern and ESCAPE; UPPER, LOWER, CHAR_LENGTH | character |
//! | `\|\|` | any (renders text) |
//! | CAST | only conversions the evaluator can perform for some value (e.g. no BOOLEAN to DATE) |
//! | UNION / EXCEPT / INTERSECT columns | same category |
//!
//! This is a bounded, fail-closed policy for the engine's own type system,
//! not the full SQL-standard type-derivation algorithm. Assignment coercion
//! of INSERT/UPDATE values to column types is unchanged (checked per value).

use crate::catalog::{Catalog, Relation};
use crate::error::{DbError, DbResult};
use crate::execution::eval::{Env, Scope, display_column};
use crate::sql::ast::{
    AggFunc, BinaryOp, Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableRef, UnaryOp,
};
use crate::sql::value::{SqlType, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Category {
    Unknown,
    Numeric,
    Character,
    Boolean,
    Date,
    DateTime,
    Time,
    Year,
}

impl Category {
    fn of(ty: &SqlType) -> Category {
        match ty {
            SqlType::Null => Category::Unknown,
            t if t.is_numeric() => Category::Numeric,
            t if t.is_text() => Category::Character,
            SqlType::Boolean => Category::Boolean,
            SqlType::Date => Category::Date,
            SqlType::DateTime | SqlType::Timestamp => Category::DateTime,
            SqlType::Time => Category::Time,
            _ => Category::Year,
        }
    }

    fn is_temporal(self) -> bool {
        matches!(self, Category::Date | Category::DateTime | Category::Time | Category::Year)
    }
}

/// Static type of a bound expression. `text_literal` marks a string literal,
/// which may stand for a date/time value (implicit literal coercion).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bound {
    pub ty: SqlType,
    text_literal: bool,
}

impl Bound {
    fn of(ty: SqlType) -> Bound {
        Bound { ty, text_literal: false }
    }

    fn unknown() -> Bound {
        Bound::of(SqlType::Null)
    }

    fn boolean() -> Bound {
        Bound::of(SqlType::Boolean)
    }

    fn category(&self) -> Category {
        Category::of(&self.ty)
    }

    fn type_name(&self) -> String {
        if self.ty == SqlType::Null { "unknown".into() } else { self.ty.name() }
    }
}

fn mismatch(msg: String) -> DbError {
    DbError::TypeMismatch(msg)
}

fn widen(a: SqlType, b: SqlType) -> SqlType {
    match (a, b) {
        (SqlType::Null, t) | (t, SqlType::Null) => t,
        (SqlType::Double, _) | (_, SqlType::Double) => SqlType::Double,
        (d @ SqlType::Decimal { .. }, _) | (_, d @ SqlType::Decimal { .. }) => d,
        (t, _) => t,
    }
}

/// Whether values of `a` and `b` may be compared.
fn comparable(a: &Bound, b: &Bound) -> bool {
    let (ca, cb) = (a.category(), b.category());
    ca == Category::Unknown
        || cb == Category::Unknown
        || ca == cb
        || (ca.is_temporal() && b.text_literal)
        || (cb.is_temporal() && a.text_literal)
}

/// Common type of two alternatives (CASE results, COALESCE arguments) or of
/// two compared operands; `what` names the construct in the error.
fn unify(a: Bound, b: Bound, what: &str) -> DbResult<Bound> {
    if !comparable(&a, &b) {
        return Err(mismatch(format!("{} types {} and {} cannot be matched", what, a.type_name(), b.type_name())));
    }
    let (ca, cb) = (a.category(), b.category());
    Ok(match (ca, cb) {
        (Category::Unknown, _) => b,
        (_, Category::Unknown) => a,
        (Category::Numeric, Category::Numeric) => Bound::of(widen(a.ty, b.ty)),
        (Category::Character, Category::Character) => Bound {
            ty: if a.ty == b.ty { a.ty } else { SqlType::Text },
            text_literal: a.text_literal && b.text_literal,
        },
        // A character literal next to a date/time kind takes that kind.
        (Category::Character, _) => b,
        _ => a,
    })
}

fn require(b: &Bound, ok: impl Fn(Category) -> bool, what: &str, expected: &str) -> DbResult<()> {
    let c = b.category();
    if c == Category::Unknown || ok(c) {
        Ok(())
    } else {
        Err(mismatch(format!("{} requires {}, found {}", what, expected, b.type_name())))
    }
}

fn require_boolean(b: &Bound, what: &str) -> DbResult<()> {
    require(b, |c| c == Category::Boolean, what, "BOOLEAN")
}

fn require_numeric(b: &Bound, what: &str) -> DbResult<()> {
    require(b, |c| c == Category::Numeric, what, "a numeric type")
}

fn require_character(b: &Bound, what: &str) -> DbResult<()> {
    require(b, |c| c == Category::Character, what, "a character type")
}

fn is_integer(ty: &SqlType) -> bool {
    matches!(ty, SqlType::Integer | SqlType::SmallInt | SqlType::BigInt)
}

/// Whether `CAST(x AS to)` can succeed for some non-NULL `x` of type `from`
/// (mirrors `Value::cast_to`).
fn cast_allowed(from: &Bound, to: &SqlType) -> bool {
    use Category as C;
    let c = from.category();
    if c == C::Unknown {
        return true;
    }
    match to {
        SqlType::Null | SqlType::Text | SqlType::Char(_) | SqlType::Varchar(_) => true,
        SqlType::Boolean => matches!(c, C::Boolean | C::Character) || is_integer(&from.ty),
        SqlType::Integer | SqlType::SmallInt | SqlType::BigInt => matches!(c, C::Numeric | C::Boolean | C::Character),
        SqlType::Decimal { .. } | SqlType::Double => matches!(c, C::Numeric | C::Character),
        SqlType::Date | SqlType::DateTime | SqlType::Timestamp => matches!(c, C::Date | C::DateTime | C::Character),
        SqlType::Time => matches!(c, C::Time | C::Character),
        SqlType::Year => matches!(c, C::Year | C::Character),
    }
}

/// Output name of a select-list expression without an alias (the executor's
/// naming rule).
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

/// One output column of a bound query: name, static type, and whether ORDER
/// BY may refer to it by name.
struct OutColumn {
    name: String,
    bound: Bound,
    order_name: bool,
}

struct Binder<'a> {
    /// `None` for expressions that cannot contain subqueries (CHECK).
    catalog: Option<&'a Catalog>,
}

impl Binder<'_> {
    fn catalog(&self) -> DbResult<&Catalog> {
        self.catalog.ok_or_else(|| DbError::FeatureNotSupported("subqueries in this context".into()))
    }

    // ---- queries ------------------------------------------------------------

    fn query(&self, q: &Query, outer: &[Scope]) -> DbResult<Vec<OutColumn>> {
        match &q.body {
            SetExpr::Select(sel) => self.select(sel, &q.order_by, outer),
            // A set operation's ORDER BY names output columns only.
            body => self.set_expr(body, outer),
        }
    }

    fn set_expr(&self, body: &SetExpr, outer: &[Scope]) -> DbResult<Vec<OutColumn>> {
        match body {
            SetExpr::Select(sel) => self.select(sel, &[], outer),
            SetExpr::SetOp { op, left, right, .. } => {
                let l = self.set_expr(left, outer)?;
                let r = self.set_expr(right, outer)?;
                if l.len() != r.len() {
                    return Err(DbError::Syntax(format!(
                        "each {} query must have the same number of columns",
                        op.as_str()
                    )));
                }
                let mut out = Vec::with_capacity(l.len());
                for (lc, rc) in l.into_iter().zip(r) {
                    let (cl, cr) = (lc.bound.category(), rc.bound.category());
                    if cl != Category::Unknown && cr != Category::Unknown && cl != cr {
                        return Err(mismatch(format!(
                            "{} types {} and {} cannot be matched",
                            op.as_str(),
                            lc.bound.type_name(),
                            rc.bound.type_name()
                        )));
                    }
                    let ty = if cl == Category::Unknown { rc.bound.ty } else { widen(lc.bound.ty, rc.bound.ty) };
                    out.push(OutColumn { name: lc.name, bound: Bound::of(ty), order_name: lc.order_name });
                }
                Ok(out)
            }
        }
    }

    /// Columns one FROM item exposes.
    fn table_ref(&self, tref: &TableRef, outer: &[Scope]) -> DbResult<Scope> {
        let catalog = self.catalog()?;
        let mut scope = Scope::new();
        match tref {
            TableRef::Named { name, alias } => match catalog.resolve_relation(name)? {
                Relation::Table(info) => {
                    let qualifier = alias.clone().unwrap_or_else(|| info.name.clone());
                    for (col, ty) in &info.columns {
                        scope.push(Some(qualifier.clone()), col.clone(), SqlType::from_column_type(ty));
                    }
                }
                Relation::View(view) => {
                    let cols = self.query(&view.query, &[])?;
                    if cols.len() != view.columns.len() {
                        return Err(DbError::ColumnNotFound(format!(
                            "view \"{}\" defines {} columns but its query now returns {}",
                            view.name,
                            view.columns.len(),
                            cols.len()
                        )));
                    }
                    let qualifier = alias.clone().unwrap_or_else(|| view.name.clone());
                    for (name, col) in view.columns.iter().zip(cols) {
                        scope.push(Some(qualifier.clone()), name.clone(), col.bound.ty);
                    }
                }
            },
            TableRef::Subquery { query, alias } => {
                for c in self.query(query, outer)? {
                    scope.push(Some(alias.clone()), c.name, c.bound.ty);
                }
            }
        }
        Ok(scope)
    }

    fn select(&self, sel: &Select, order_by: &[crate::sql::ast::OrderByItem], outer: &[Scope]) -> DbResult<Vec<OutColumn>> {
        let mut scope = Scope::new();
        for tref in &sel.from {
            let part = self.table_ref(tref, outer)?;
            scope.extend(&part);
        }
        for join in &sel.joins {
            let part = self.table_ref(&join.table, outer)?;
            scope.extend(&part);
            if let Some(p) = &join.predicate {
                require_boolean(&self.expr(p, &scope, outer)?, "JOIN ... ON")?;
            }
        }
        if let Some(w) = &sel.selection {
            require_boolean(&self.expr(w, &scope, outer)?, "WHERE")?;
        }
        for g in &sel.group_by {
            // An integer literal is a select-list ordinal.
            if !matches!(g, Expr::Literal(Value::Int(_))) {
                self.expr(g, &scope, outer)?;
            }
        }
        if let Some(h) = &sel.having {
            require_boolean(&self.expr(h, &scope, outer)?, "HAVING")?;
        }
        let mut out = Vec::new();
        for item in &sel.projection {
            match item {
                SelectItem::Wildcard => {
                    for c in &scope.columns {
                        out.push(OutColumn { name: c.name.clone(), bound: Bound::of(c.ty), order_name: true });
                    }
                }
                SelectItem::QualifiedWildcard(q) => {
                    let before = out.len();
                    for c in &scope.columns {
                        if c.qualifier.as_deref().is_some_and(|cq| q.matches(cq)) {
                            out.push(OutColumn { name: c.name.clone(), bound: Bound::of(c.ty), order_name: true });
                        }
                    }
                    if out.len() == before {
                        return Err(DbError::TableNotFound(q.value.clone()));
                    }
                }
                SelectItem::Expr { expr, alias } => {
                    let bound = self.expr(expr, &scope, outer)?;
                    out.push(OutColumn {
                        name: alias.clone().unwrap_or_else(|| derived_name(expr)),
                        bound,
                        order_name: alias.is_some() || matches!(expr, Expr::Column { .. }),
                    });
                }
            }
        }
        for o in order_by {
            let by_output = match &o.expr {
                Expr::Literal(Value::Int(_)) => true,
                Expr::Column { table: None, name } => out.iter().any(|c| c.order_name && name.matches(&c.name)),
                _ => false,
            };
            if !by_output {
                self.expr(&o.expr, &scope, outer)?;
            }
        }
        Ok(out)
    }

    /// Bind a subquery nested in an expression of `scope`.
    fn subquery(&self, q: &Query, scope: &Scope, outer: &[Scope]) -> DbResult<Vec<OutColumn>> {
        let mut chain = outer.to_vec();
        chain.push(scope.clone());
        self.query(q, &chain)
    }

    fn single_column(&self, q: &Query, scope: &Scope, outer: &[Scope]) -> DbResult<Bound> {
        let cols = self.subquery(q, scope, outer)?;
        if cols.len() != 1 {
            return Err(DbError::Syntax("subquery has too many columns".into()));
        }
        Ok(cols[0].bound)
    }

    // ---- expressions ----------------------------------------------------------

    fn column(&self, table: Option<&Ident>, name: &Ident, scope: &Scope, outer: &[Scope]) -> DbResult<Bound> {
        if let Some(i) = scope.resolve(table, name)? {
            return Ok(Bound::of(scope.columns[i].ty));
        }
        for s in outer.iter().rev() {
            if let Some(i) = s.resolve(table, name)? {
                return Ok(Bound::of(s.columns[i].ty));
            }
        }
        Err(DbError::ColumnNotFound(display_column(table.map(Ident::as_str), name)))
    }

    fn expr(&self, e: &Expr, scope: &Scope, outer: &[Scope]) -> DbResult<Bound> {
        let bind = |x: &Expr| self.expr(x, scope, outer);
        match e {
            Expr::Literal(v) => Ok(Bound { ty: v.sql_type(), text_literal: matches!(v, Value::Text(_)) }),
            Expr::Column { table, name } => self.column(table.as_ref(), name, scope, outer),
            Expr::Unary { op, expr } => {
                let b = bind(expr)?;
                match op {
                    UnaryOp::Not => {
                        require_boolean(&b, "NOT")?;
                        Ok(Bound::boolean())
                    }
                    UnaryOp::Neg | UnaryOp::Plus => {
                        require_numeric(&b, if *op == UnaryOp::Neg { "unary -" } else { "unary +" })?;
                        Ok(Bound::of(b.ty))
                    }
                }
            }
            Expr::Binary { op, left, right } => {
                let (l, r) = (bind(left)?, bind(right)?);
                match op {
                    BinaryOp::And | BinaryOp::Or => {
                        require_boolean(&l, op.symbol())?;
                        require_boolean(&r, op.symbol())?;
                        Ok(Bound::boolean())
                    }
                    op if op.is_comparison() => {
                        if !comparable(&l, &r) {
                            return Err(mismatch(format!(
                                "operator {} is not defined for {} and {}",
                                op.symbol(),
                                l.type_name(),
                                r.type_name()
                            )));
                        }
                        Ok(Bound::boolean())
                    }
                    BinaryOp::Concat => Ok(Bound::of(SqlType::Text)),
                    BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::BitXor => {
                        for b in [&l, &r] {
                            if b.category() != Category::Unknown && !is_integer(&b.ty) {
                                return Err(mismatch(format!(
                                    "operator {} requires integer operands, found {}",
                                    op.symbol(),
                                    b.type_name()
                                )));
                            }
                        }
                        Ok(Bound::of(SqlType::Integer))
                    }
                    _ => {
                        for b in [&l, &r] {
                            require_numeric(b, &format!("operator {}", op.symbol()))?;
                        }
                        Ok(Bound::of(widen(l.ty, r.ty)))
                    }
                }
            }
            Expr::IsNull { expr, .. } => {
                bind(expr)?;
                Ok(Bound::boolean())
            }
            Expr::IsTruth { expr, .. } => {
                require_boolean(&bind(expr)?, "IS TRUE / FALSE / UNKNOWN")?;
                Ok(Bound::boolean())
            }
            Expr::InList { expr, list, .. } => {
                let mut acc = bind(expr)?;
                for item in list {
                    acc = unify(acc, bind(item)?, "IN")?;
                }
                Ok(Bound::boolean())
            }
            Expr::InSubquery { expr, query, .. } => {
                let left = bind(expr)?;
                let col = self.single_column(query, scope, outer)?;
                unify(left, col, "IN")?;
                Ok(Bound::boolean())
            }
            Expr::Exists { query, .. } => {
                self.subquery(query, scope, outer)?;
                Ok(Bound::boolean())
            }
            Expr::Between { expr, low, high, .. } => {
                let acc = unify(bind(expr)?, bind(low)?, "BETWEEN")?;
                unify(acc, bind(high)?, "BETWEEN")?;
                Ok(Bound::boolean())
            }
            Expr::Like { expr, pattern, escape, .. } => {
                require_character(&bind(expr)?, "LIKE")?;
                require_character(&bind(pattern)?, "LIKE pattern")?;
                if let Some(esc) = escape {
                    require_character(&bind(esc)?, "LIKE ESCAPE")?;
                }
                Ok(Bound::boolean())
            }
            Expr::Case { operand, whens, else_result } => {
                let mut op = match operand {
                    Some(o) => Some(bind(o)?),
                    None => None,
                };
                let mut result = Bound::unknown();
                for (cond, then) in whens {
                    let c = bind(cond)?;
                    match op {
                        Some(o) => op = Some(unify(o, c, "CASE/WHEN")?),
                        None => require_boolean(&c, "CASE WHEN")?,
                    }
                    result = unify(result, bind(then)?, "CASE")?;
                }
                if let Some(e) = else_result {
                    result = unify(result, bind(e)?, "CASE")?;
                }
                Ok(result)
            }
            Expr::Cast { expr, ty } => {
                let from = bind(expr)?;
                if !cast_allowed(&from, ty) {
                    return Err(mismatch(format!("cannot cast {} to {}", from.type_name(), ty.name())));
                }
                Ok(Bound::of(*ty))
            }
            Expr::Function { name, args } => {
                let bound = args.iter().map(&bind).collect::<DbResult<Vec<_>>>()?;
                match name.as_str() {
                    "COALESCE" => {
                        let mut acc = Bound::unknown();
                        for b in bound {
                            acc = unify(acc, b, "COALESCE")?;
                        }
                        Ok(acc)
                    }
                    "NULLIF" if bound.len() == 2 => {
                        unify(bound[0], bound[1], "NULLIF")?;
                        Ok(Bound::of(bound[0].ty))
                    }
                    "UPPER" | "LOWER" if bound.len() == 1 => {
                        require_character(&bound[0], name)?;
                        Ok(Bound::of(SqlType::Text))
                    }
                    "CHAR_LENGTH" | "CHARACTER_LENGTH" if bound.len() == 1 => {
                        require_character(&bound[0], name)?;
                        Ok(Bound::of(SqlType::Integer))
                    }
                    "ABS" if bound.len() == 1 => {
                        require_numeric(&bound[0], name)?;
                        Ok(Bound::of(bound[0].ty))
                    }
                    "CURRENT_TIMESTAMP" | "GETDATE" | "GETUTCDATE" => Ok(Bound::of(SqlType::DateTime)),
                    // Arity errors and unknown functions are reported by the evaluator.
                    _ => Ok(Bound::unknown()),
                }
            }
            Expr::Aggregate { func, arg, .. } => {
                let Some(arg) = arg else {
                    return Ok(Bound::of(SqlType::Integer));
                };
                let b = bind(arg)?;
                match func {
                    AggFunc::Count => Ok(Bound::of(SqlType::Integer)),
                    AggFunc::Sum => {
                        require_numeric(&b, "SUM")?;
                        Ok(Bound::of(b.ty))
                    }
                    AggFunc::Avg => {
                        require_numeric(&b, "AVG")?;
                        Ok(Bound::of(match b.ty {
                            d @ SqlType::Decimal { .. } => d,
                            SqlType::Null => SqlType::Null,
                            _ => SqlType::Double,
                        }))
                    }
                    AggFunc::Min | AggFunc::Max => Ok(Bound::of(b.ty)),
                }
            }
            Expr::ScalarSubquery(q) => Ok(Bound::of(self.single_column(q, scope, outer)?.ty)),
            Expr::Default => Ok(Bound::unknown()),
        }
    }
}

/// The scopes of `env` and its correlation chain, outermost first.
fn env_scopes(env: Option<&Env>) -> Vec<Scope> {
    let mut chain = Vec::new();
    let mut cur = env;
    while let Some(e) = cur {
        chain.push(e.scope.clone());
        cur = e.outer;
    }
    chain.reverse();
    chain
}

/// Bind a query (optionally correlated with `outer`) before it is executed.
pub fn bind_query(catalog: &Catalog, query: &Query, outer: Option<&Env>) -> DbResult<()> {
    Binder { catalog: Some(catalog) }.query(query, &env_scopes(outer)).map(|_| ())
}

/// Bind a row predicate over `scope` (WHERE of UPDATE/DELETE, index scans):
/// it must be BOOLEAN.
pub fn bind_predicate(catalog: &Catalog, scope: &Scope, expr: &Expr, what: &str) -> DbResult<()> {
    let b = Binder { catalog: Some(catalog) }.expr(expr, scope, &[])?;
    require_boolean(&b, what)
}

/// Bind a CHECK constraint expression over the table's columns: it must be
/// BOOLEAN (CHECK expressions never contain subqueries).
pub fn bind_check(scope: &Scope, expr: &Expr) -> DbResult<()> {
    let b = Binder { catalog: None }.expr(expr, scope, &[])?;
    require_boolean(&b, "CHECK")
}

/// Bind every expression of a DML statement (INSERT values, UPDATE SET and
/// WHERE, DELETE WHERE) before it touches any row, and the DEFAULT
/// expressions of CREATE TABLE / ALTER TABLE ADD COLUMN. Queries are bound by
/// [`crate::execution::query::execute_query`]; CHECK constraints at CREATE
/// TABLE / ALTER TABLE by `keys::resolve_table_constraints`.
pub fn bind_statement(catalog: &Catalog, stmt: &Statement) -> DbResult<()> {
    let binder = Binder { catalog: Some(catalog) };
    match stmt {
        Statement::Query(q) => binder.query(q, &[]).map(|_| ()),
        Statement::Insert { rows, .. } => {
            let empty = Scope::new();
            for row in rows {
                for e in row {
                    binder.expr(e, &empty, &[])?;
                }
            }
            Ok(())
        }
        Statement::Update { table_name, assignments, selection } => {
            let scope = crate::constraints::keys::table_scope(catalog.resolve_table(table_name)?);
            for (_, e) in assignments {
                binder.expr(e, &scope, &[])?;
            }
            if let Some(w) = selection {
                require_boolean(&binder.expr(w, &scope, &[])?, "WHERE")?;
            }
            Ok(())
        }
        Statement::Delete { table_name, selection } => {
            let scope = crate::constraints::keys::table_scope(catalog.resolve_table(table_name)?);
            if let Some(w) = selection {
                require_boolean(&binder.expr(w, &scope, &[])?, "WHERE")?;
            }
            Ok(())
        }
        // DEFAULT expressions are constants evaluated at INSERT time.
        Statement::CreateTable { columns, .. } => {
            let empty = Scope::new();
            for d in columns.iter().filter_map(|c| c.default_value.as_ref()) {
                binder.expr(d, &empty, &[])?;
            }
            Ok(())
        }
        Statement::AlterTableAddColumn { column, .. } => {
            if let Some(d) = &column.default_value {
                binder.expr(d, &Scope::new(), &[])?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
