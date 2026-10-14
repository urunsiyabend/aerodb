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
//! | `=` `<>` `<` `<=` `>` `>=`, IN list / IN subquery, BETWEEN, simple CASE operand vs WHEN, NULLIF | operands comparable: same category, or a character *literal* against a date/time kind (implicit literal coercion, below) |
//! | CASE THEN/ELSE, COALESCE arguments, UNION / EXCEPT / INTERSECT columns | one common category (same rule; numeric kinds widen INTEGER < DECIMAL < DOUBLE) |
//! | `+ - * / %`, unary `+ -`, SUM, AVG, ABS | numeric |
//! | `& \| ^` | integer kinds |
//! | AND, OR, NOT, IS [NOT] TRUE/FALSE/UNKNOWN, searched CASE WHEN, WHERE, HAVING, JOIN ON, CHECK | boolean |
//! | LIKE operand, pattern and ESCAPE; UPPER, LOWER, CHAR_LENGTH | character |
//! | `\|\|` | any (renders text) |
//! | CAST | only conversions the evaluator can perform for some value (e.g. no BOOLEAN to DATE) |
//! | INSERT VALUES / UPDATE SET value vs its target column | same category as the column; a character literal may also be assigned to a numeric or date/time column (converted per value, e.g. `'42'`, `'2024-01-02'`) |
//!
//! **Implicit literal coercion.** A character *literal* (a string constant,
//! or a CASE / COALESCE whose results are all string constants or NULL)
//! matched against a date/time kind is converted to that kind once, at bind
//! time, in the statement that is then executed: `COALESCE(d, '2024-01-01')`
//! yields DATE values, `SELECT d ... UNION SELECT '2024-01-01'` a DATE
//! column, and a literal that is not a valid value of the kind (`COALESCE(d,
//! 'never')`) fails with 22018 before any row is read, whatever the data. A
//! non-literal character expression (a TEXT column, `UPPER(..)`, ...) never
//! mixes with a date/time kind (42804).
//!
//! This is a bounded, fail-closed policy for the engine's own type system,
//! not the full SQL-standard type-derivation algorithm.

use crate::catalog::{Catalog, Relation};
use crate::error::{DbError, DbResult};
use crate::execution::eval::{Env, Scope, display_column};
use crate::sql::ast::{
    AggFunc, BinaryOp, Expr, Ident, OrderByItem, Query, Select, SelectItem, SetExpr, Statement,
    TableRef, UnaryOp,
};
use crate::sql::value::{SqlType, Value};
use crate::storage::row::ColumnType;

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
        matches!(
            self,
            Category::Date | Category::DateTime | Category::Time | Category::Year
        )
    }
}

/// Static type of a bound expression. `text_literal` marks a character
/// literal (see the module doc), which may stand for a date/time value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bound {
    pub ty: SqlType,
    text_literal: bool,
}

impl Bound {
    fn of(ty: SqlType) -> Bound {
        Bound {
            ty,
            text_literal: false,
        }
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
        if self.ty == SqlType::Null {
            "unknown".into()
        } else {
            self.ty.name()
        }
    }

    /// The date/time type a character literal bound as `self` must be
    /// converted to when matched against `other`.
    fn literal_target(&self, other: &Bound) -> Option<SqlType> {
        (self.text_literal && other.category().is_temporal()).then_some(other.ty)
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

/// Common type of two alternatives (CASE results, COALESCE arguments,
/// set-operation columns) or of two compared operands; `what` names the
/// construct in the error. A character literal next to a date/time kind
/// takes that kind (the caller converts the literal with [`coerce`]).
fn unify(a: Bound, b: Bound, what: &str) -> DbResult<Bound> {
    if !comparable(&a, &b) {
        return Err(mismatch(format!(
            "{} types {} and {} cannot be matched",
            what,
            a.type_name(),
            b.type_name()
        )));
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
        (Category::Character, _) => Bound::of(b.ty),
        _ => Bound::of(a.ty),
    })
}

/// Convert the character literal(s) of `e` — a string constant, or the
/// results of a CASE / COALESCE made of string constants and NULLs — to the
/// date/time type `ty`. An invalid literal fails here (22018), at bind time.
fn coerce(e: &mut Expr, ty: &SqlType) -> DbResult<()> {
    match e {
        Expr::Literal(v @ Value::Text(_)) => *v = v.cast_to(ty)?,
        Expr::Function { name, args } if name.as_str() == "COALESCE" => {
            for a in args {
                coerce(a, ty)?;
            }
        }
        Expr::Case {
            whens, else_result, ..
        } => {
            for (_, then) in whens {
                coerce(then, ty)?;
            }
            if let Some(e) = else_result {
                coerce(e, ty)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Convert `e` (bound as `b`) to `target` if it is a character literal and
/// `target` is a date/time kind.
fn coerce_to(e: &mut Expr, b: &Bound, target: &Bound) -> DbResult<()> {
    match b.literal_target(target) {
        Some(ty) => coerce(e, &ty),
        None => Ok(()),
    }
}

fn require(b: &Bound, ok: impl Fn(Category) -> bool, what: &str, expected: &str) -> DbResult<()> {
    let c = b.category();
    if c == Category::Unknown || ok(c) {
        Ok(())
    } else {
        Err(mismatch(format!(
            "{} requires {}, found {}",
            what,
            expected,
            b.type_name()
        )))
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
        SqlType::Integer | SqlType::SmallInt | SqlType::BigInt => {
            matches!(c, C::Numeric | C::Boolean | C::Character)
        }
        SqlType::Decimal { .. } | SqlType::Double => matches!(c, C::Numeric | C::Character),
        SqlType::Date | SqlType::DateTime | SqlType::Timestamp => {
            matches!(c, C::Date | C::DateTime | C::Character)
        }
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
        self.catalog
            .ok_or_else(|| DbError::FeatureNotSupported("subqueries in this context".into()))
    }

    // ---- queries ------------------------------------------------------------

    fn query(&self, q: &mut Query, outer: &[Scope]) -> DbResult<Vec<OutColumn>> {
        match &mut q.body {
            SetExpr::Select(sel) => self.select(sel, &mut q.order_by, outer),
            // A set operation's ORDER BY names output columns only.
            body => self.set_expr(body, outer),
        }
    }

    fn set_expr(&self, body: &mut SetExpr, outer: &[Scope]) -> DbResult<Vec<OutColumn>> {
        match body {
            SetExpr::Select(sel) => self.select(sel, &mut [], outer),
            SetExpr::SetOp {
                op, left, right, ..
            } => {
                let l = self.set_expr(left, outer)?;
                let r = self.set_expr(right, outer)?;
                if l.len() != r.len() {
                    return Err(DbError::Syntax(format!(
                        "each {} query must have the same number of columns",
                        op.as_str()
                    )));
                }
                let mut out = Vec::with_capacity(l.len());
                let mut left_targets = Vec::with_capacity(l.len());
                let mut right_targets = Vec::with_capacity(l.len());
                for (lc, rc) in l.into_iter().zip(r) {
                    let bound = unify(lc.bound, rc.bound, op.as_str())?;
                    left_targets.push(lc.bound.literal_target(&rc.bound));
                    right_targets.push(rc.bound.literal_target(&lc.bound));
                    out.push(OutColumn {
                        name: lc.name,
                        bound,
                        order_name: lc.order_name,
                    });
                }
                if left_targets.iter().any(Option::is_some) {
                    self.coerce_output(left, &left_targets, outer)?;
                }
                if right_targets.iter().any(Option::is_some) {
                    self.coerce_output(right, &right_targets, outer)?;
                }
                Ok(out)
            }
        }
    }

    /// Convert the character-literal output columns of `body` that a set
    /// operation (or IN subquery) matches against a date/time kind:
    /// `targets[i]` is the type for output column `i`.
    fn coerce_output(
        &self,
        body: &mut SetExpr,
        targets: &[Option<SqlType>],
        outer: &[Scope],
    ) -> DbResult<()> {
        match body {
            SetExpr::SetOp { left, right, .. } => {
                self.coerce_output(left, targets, outer)?;
                self.coerce_output(right, targets, outer)
            }
            SetExpr::Select(sel) => {
                // Output position of each select-list item (wildcards expand
                // to the columns of the FROM scope).
                let has_wildcard = sel
                    .projection
                    .iter()
                    .any(|i| !matches!(i, SelectItem::Expr { .. }));
                let scope = if has_wildcard {
                    Some(self.from_scope(sel, outer)?)
                } else {
                    None
                };
                let mut pos = 0;
                for item in &mut sel.projection {
                    match item {
                        SelectItem::Expr { expr, .. } => {
                            if let Some(Some(ty)) = targets.get(pos) {
                                coerce(expr, ty)?;
                            }
                            pos += 1;
                        }
                        SelectItem::Wildcard => {
                            pos += scope.as_ref().map_or(0, |s| s.columns.len())
                        }
                        SelectItem::QualifiedWildcard(q) => {
                            pos += scope.as_ref().map_or(0, |s| {
                                s.columns
                                    .iter()
                                    .filter(|c| {
                                        c.qualifier.as_deref().is_some_and(|cq| q.matches(cq))
                                    })
                                    .count()
                            })
                        }
                    }
                }
                Ok(())
            }
        }
    }

    /// Columns one FROM item exposes.
    fn table_ref(&self, tref: &mut TableRef, outer: &[Scope]) -> DbResult<Scope> {
        let catalog = self.catalog()?;
        let mut scope = Scope::new();
        match tref {
            TableRef::Named {
                schema: Some(schema),
                name,
                alias,
            } => {
                let view = crate::execution::information_schema::resolve(schema, name)?;
                let qualifier = alias.clone().unwrap_or_else(|| view.name.to_string());
                for (col, ty) in view.columns {
                    scope.push(Some(qualifier.clone()), col.to_string(), *ty);
                }
            }
            TableRef::Named {
                schema: None,
                name,
                alias,
            } => match catalog.resolve_relation(name)? {
                Relation::Table(info) => {
                    let qualifier = alias.clone().unwrap_or_else(|| info.name.clone());
                    for (col, ty) in &info.columns {
                        scope.push(
                            Some(qualifier.clone()),
                            col.clone(),
                            SqlType::from_column_type(ty),
                        );
                    }
                }
                Relation::View(view) => {
                    // The stored query is bound (and coerced) again each
                    // time it runs; see `bound_view_query`.
                    let cols = self.query(&mut view.query.clone(), &[])?;
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

    /// The scope of a SELECT's FROM items and joins (ON predicates bound).
    fn from_scope(&self, sel: &mut Select, outer: &[Scope]) -> DbResult<Scope> {
        let mut scope = Scope::new();
        for tref in &mut sel.from {
            let part = self.table_ref(tref, outer)?;
            scope.extend(&part);
        }
        for join in &mut sel.joins {
            let part = self.table_ref(&mut join.table, outer)?;
            scope.extend(&part);
            if let Some(p) = &mut join.predicate {
                require_boolean(&self.expr(p, &scope, outer)?, "JOIN ... ON")?;
            }
        }
        Ok(scope)
    }

    fn select(
        &self,
        sel: &mut Select,
        order_by: &mut [OrderByItem],
        outer: &[Scope],
    ) -> DbResult<Vec<OutColumn>> {
        let scope = self.from_scope(sel, outer)?;
        if let Some(w) = &mut sel.selection {
            require_boolean(&self.expr(w, &scope, outer)?, "WHERE")?;
        }
        for g in &mut sel.group_by {
            // An integer literal is a select-list ordinal.
            if !matches!(g, Expr::Literal(Value::Int(_))) {
                self.expr(g, &scope, outer)?;
            }
        }
        if let Some(h) = &mut sel.having {
            require_boolean(&self.expr(h, &scope, outer)?, "HAVING")?;
        }
        let mut out = Vec::new();
        for item in &mut sel.projection {
            match item {
                SelectItem::Wildcard => {
                    for c in &scope.columns {
                        out.push(OutColumn {
                            name: c.name.clone(),
                            bound: Bound::of(c.ty),
                            order_name: true,
                        });
                    }
                }
                SelectItem::QualifiedWildcard(q) => {
                    let before = out.len();
                    for c in &scope.columns {
                        if c.qualifier.as_deref().is_some_and(|cq| q.matches(cq)) {
                            out.push(OutColumn {
                                name: c.name.clone(),
                                bound: Bound::of(c.ty),
                                order_name: true,
                            });
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
                Expr::Column { table: None, name } => {
                    out.iter().any(|c| c.order_name && name.matches(&c.name))
                }
                _ => false,
            };
            if !by_output {
                self.expr(&mut o.expr, &scope, outer)?;
            }
        }
        Ok(out)
    }

    /// Bind a subquery nested in an expression of `scope`.
    fn subquery(&self, q: &mut Query, scope: &Scope, outer: &[Scope]) -> DbResult<Vec<OutColumn>> {
        let mut chain = outer.to_vec();
        chain.push(scope.clone());
        self.query(q, &chain)
    }

    fn single_column(&self, q: &mut Query, scope: &Scope, outer: &[Scope]) -> DbResult<Bound> {
        let cols = self.subquery(q, scope, outer)?;
        if cols.len() != 1 {
            return Err(DbError::Syntax("subquery has too many columns".into()));
        }
        Ok(cols[0].bound)
    }

    // ---- expressions ----------------------------------------------------------

    fn column(
        &self,
        table: Option<&Ident>,
        name: &Ident,
        scope: &Scope,
        outer: &[Scope],
    ) -> DbResult<Bound> {
        if let Some(i) = scope.resolve(table, name)? {
            return Ok(Bound::of(scope.columns[i].ty));
        }
        for s in outer.iter().rev() {
            if let Some(i) = s.resolve(table, name)? {
                return Ok(Bound::of(s.columns[i].ty));
            }
        }
        Err(DbError::ColumnNotFound(display_column(
            table.map(Ident::as_str),
            name,
        )))
    }

    /// Bind `exprs` as alternatives of one construct: unify them, then
    /// convert character literals among them to a date/time result type.
    fn alternatives<'e>(
        &self,
        exprs: impl IntoIterator<Item = &'e mut Expr>,
        start: Bound,
        what: &str,
        scope: &Scope,
        outer: &[Scope],
    ) -> DbResult<Bound> {
        let mut bound: Vec<(&'e mut Expr, Bound)> = Vec::new();
        let mut acc = start;
        for e in exprs {
            let b = self.expr(e, scope, outer)?;
            acc = unify(acc, b, what)?;
            bound.push((e, b));
        }
        for (e, b) in bound {
            coerce_to(e, &b, &acc)?;
        }
        Ok(acc)
    }

    fn expr(&self, e: &mut Expr, scope: &Scope, outer: &[Scope]) -> DbResult<Bound> {
        match e {
            Expr::Literal(v) => Ok(Bound {
                ty: v.sql_type(),
                text_literal: matches!(v, Value::Text(_)),
            }),
            Expr::Column { table, name } => self.column(table.as_ref(), name, scope, outer),
            Expr::Unary { op, expr } => {
                let b = self.expr(expr, scope, outer)?;
                match op {
                    UnaryOp::Not => {
                        require_boolean(&b, "NOT")?;
                        Ok(Bound::boolean())
                    }
                    UnaryOp::Neg | UnaryOp::Plus => {
                        require_numeric(
                            &b,
                            if *op == UnaryOp::Neg {
                                "unary -"
                            } else {
                                "unary +"
                            },
                        )?;
                        Ok(Bound::of(b.ty))
                    }
                }
            }
            Expr::Binary { op, left, right } => {
                let l = self.expr(left, scope, outer)?;
                let r = self.expr(right, scope, outer)?;
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
                        coerce_to(left, &l, &r)?;
                        coerce_to(right, &r, &l)?;
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
                self.expr(expr, scope, outer)?;
                Ok(Bound::boolean())
            }
            Expr::IsTruth { expr, .. } => {
                require_boolean(&self.expr(expr, scope, outer)?, "IS TRUE / FALSE / UNKNOWN")?;
                Ok(Bound::boolean())
            }
            Expr::InList { expr, list, .. } => {
                self.alternatives(
                    std::iter::once(&mut **expr).chain(list.iter_mut()),
                    Bound::unknown(),
                    "IN",
                    scope,
                    outer,
                )?;
                Ok(Bound::boolean())
            }
            Expr::InSubquery { expr, query, .. } => {
                let left = self.expr(expr, scope, outer)?;
                let col = self.single_column(query, scope, outer)?;
                unify(left, col, "IN")?;
                coerce_to(expr, &left, &col)?;
                if let Some(ty) = col.literal_target(&left) {
                    let mut chain = outer.to_vec();
                    chain.push(scope.clone());
                    self.coerce_output(&mut query.body, &[Some(ty)], &chain)?;
                }
                Ok(Bound::boolean())
            }
            Expr::Exists { query, .. } => {
                self.subquery(query, scope, outer)?;
                Ok(Bound::boolean())
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.alternatives(
                    [&mut **expr, &mut **low, &mut **high],
                    Bound::unknown(),
                    "BETWEEN",
                    scope,
                    outer,
                )?;
                Ok(Bound::boolean())
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                require_character(&self.expr(expr, scope, outer)?, "LIKE")?;
                require_character(&self.expr(pattern, scope, outer)?, "LIKE pattern")?;
                if let Some(esc) = escape {
                    require_character(&self.expr(esc, scope, outer)?, "LIKE ESCAPE")?;
                }
                Ok(Bound::boolean())
            }
            Expr::Case {
                operand,
                whens,
                else_result,
            } => {
                let mut results: Vec<&mut Expr> = Vec::with_capacity(whens.len() + 1);
                let mut conds: Vec<&mut Expr> = Vec::with_capacity(whens.len());
                for (cond, then) in whens.iter_mut() {
                    conds.push(cond);
                    results.push(then);
                }
                match operand {
                    // Simple CASE: the operand and every WHEN value are compared.
                    Some(o) => {
                        self.alternatives(
                            std::iter::once(&mut **o).chain(conds),
                            Bound::unknown(),
                            "CASE/WHEN",
                            scope,
                            outer,
                        )?;
                    }
                    None => {
                        for c in conds {
                            require_boolean(&self.expr(c, scope, outer)?, "CASE WHEN")?;
                        }
                    }
                }
                if let Some(e) = else_result {
                    results.push(&mut **e);
                }
                self.alternatives(results, Bound::unknown(), "CASE", scope, outer)
            }
            Expr::Cast { expr, ty } => {
                let from = self.expr(expr, scope, outer)?;
                if !cast_allowed(&from, ty) {
                    return Err(mismatch(format!(
                        "cannot cast {} to {}",
                        from.type_name(),
                        ty.name()
                    )));
                }
                Ok(Bound::of(*ty))
            }
            Expr::Function { name, args } => {
                if name.as_str() == "COALESCE" {
                    return self.alternatives(
                        args.iter_mut(),
                        Bound::unknown(),
                        "COALESCE",
                        scope,
                        outer,
                    );
                }
                if name.as_str() == "NULLIF" && args.len() == 2 {
                    let a = self.expr(&mut args[0], scope, outer)?;
                    let b = self.expr(&mut args[1], scope, outer)?;
                    unify(a, b, "NULLIF")?;
                    coerce_to(&mut args[0], &a, &b)?;
                    coerce_to(&mut args[1], &b, &a)?;
                    // The result is the first argument (converted if it was a literal).
                    return Ok(Bound::of(if a.literal_target(&b).is_some() {
                        b.ty
                    } else {
                        a.ty
                    }));
                }
                let bound = args
                    .iter_mut()
                    .map(|a| self.expr(a, scope, outer))
                    .collect::<DbResult<Vec<_>>>()?;
                match name.as_str() {
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
                    "CURRENT_TIMESTAMP" | "GETDATE" | "GETUTCDATE" => {
                        Ok(Bound::of(SqlType::DateTime))
                    }
                    // Arity errors and unknown functions are reported by the evaluator.
                    _ => Ok(Bound::unknown()),
                }
            }
            Expr::Aggregate { func, arg, .. } => {
                let Some(arg) = arg else {
                    return Ok(Bound::of(SqlType::Integer));
                };
                let b = self.expr(arg, scope, outer)?;
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

    /// Bind a value assigned to a column of type `col` (INSERT VALUES,
    /// UPDATE SET): same category, or a character literal (converted at bind
    /// time for a date/time column; per value for a numeric one, as before).
    fn assignment(
        &self,
        e: &mut Expr,
        colty: &ColumnType,
        column: &str,
        scope: &Scope,
    ) -> DbResult<()> {
        let col = SqlType::from_column_type(colty);
        let b = self.expr(e, scope, &[])?;
        let target = Bound::of(col);
        let (cb, ct) = (b.category(), target.category());
        let ok = cb == Category::Unknown
            || cb == ct
            // a character literal converts to any column type
            || b.text_literal
            // any value converts to a character column
            || ct == Category::Character
            // integers assign to YEAR
            || (ct == Category::Year && is_integer(&b.ty));
        if !ok {
            return Err(mismatch(format!(
                "column \"{}\" is of type {} but expression is of type {}",
                column,
                col.name(),
                b.type_name()
            )));
        }
        coerce_to(e, &b, &target)?;
        // A character literal is converted exactly as at runtime, so an
        // invalid one fails here even on an empty table.
        if let Expr::Literal(v @ Value::Text(_)) = e {
            v.to_column_value(colty, column)
                .map_err(|err| crate::execution::runtime::assignment_error(column, err))?;
        }
        Ok(())
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

/// Bind a query (optionally correlated with `outer`) before it is executed,
/// converting its character literals that stand for date/time values (the
/// query is modified in place; execute the bound form).
pub fn bind_query(catalog: &Catalog, query: &mut Query, outer: Option<&Env>) -> DbResult<()> {
    Binder {
        catalog: Some(catalog),
    }
    .query(query, &env_scopes(outer))
    .map(|_| ())
}

/// A view's stored query, bound for execution (see [`bind_query`]).
pub fn bound_view_query(catalog: &Catalog, query: &Query) -> DbResult<Query> {
    let mut q = query.clone();
    bind_query(catalog, &mut q, None)?;
    Ok(q)
}

/// The static result type of each output column of a view's stored query
/// (`SqlType::Null` where none is derivable, e.g. an untyped NULL).
pub fn view_column_types(catalog: &Catalog, query: &Query) -> DbResult<Vec<SqlType>> {
    let cols = Binder {
        catalog: Some(catalog),
    }
    .query(&mut query.clone(), &[])?;
    Ok(cols.into_iter().map(|c| c.bound.ty).collect())
}

/// Bind a row predicate over `scope` (WHERE of UPDATE/DELETE, index scans):
/// it must be BOOLEAN. Modified in place like [`bind_query`].
pub fn bind_predicate(
    catalog: &Catalog,
    scope: &Scope,
    expr: &mut Expr,
    what: &str,
) -> DbResult<()> {
    let b = Binder {
        catalog: Some(catalog),
    }
    .expr(expr, scope, &[])?;
    require_boolean(&b, what)
}

/// Bind a CHECK constraint expression over the table's columns: it must be
/// BOOLEAN (CHECK expressions never contain subqueries). The stored
/// expression is left as written (it is bound on a copy).
pub fn bind_check(scope: &Scope, expr: &Expr) -> DbResult<()> {
    let b = Binder { catalog: None }.expr(&mut expr.clone(), scope, &[])?;
    require_boolean(&b, "CHECK")
}

/// Bind every expression of a statement before it touches any row: queries,
/// INSERT values and UPDATE SET values (each against its target column),
/// UPDATE / DELETE WHERE — modified in place like [`bind_query`] — and the
/// DEFAULT expressions of CREATE TABLE / ALTER TABLE ADD COLUMN (bound on a
/// copy: the stored default stays as written). CHECK constraints are bound by
/// `keys::resolve_table_constraints`.
pub fn bind_statement(catalog: &Catalog, stmt: &mut Statement) -> DbResult<()> {
    let binder = Binder {
        catalog: Some(catalog),
    };
    match stmt {
        Statement::Query(q) => binder.query(q, &[]).map(|_| ()),
        Statement::Insert {
            table_name,
            columns,
            rows,
        } => {
            let info = catalog.resolve_table(table_name)?;
            let targets: Vec<usize> = match columns {
                Some(cols) => crate::execution::runtime::resolve_targets(info, cols.iter())?,
                None => (0..info.columns.len()).collect(),
            };
            let empty = Scope::new();
            for row in rows {
                // A row of the wrong width is reported by the executor.
                for (i, e) in row.iter_mut().enumerate() {
                    match targets.get(i) {
                        Some(&t) => {
                            let (name, ty) = &info.columns[t];
                            binder.assignment(e, ty, name, &empty)?;
                        }
                        None => {
                            binder.expr(e, &empty, &[])?;
                        }
                    }
                }
            }
            Ok(())
        }
        Statement::Update {
            table_name,
            assignments,
            selection,
        } => {
            let info = catalog.resolve_table(table_name)?;
            let scope = crate::constraints::keys::table_scope(info);
            let targets = crate::execution::runtime::resolve_targets(
                info,
                assignments.iter().map(|(c, _)| c),
            )?;
            for ((_, e), t) in assignments.iter_mut().zip(targets) {
                let (name, ty) = &info.columns[t];
                binder.assignment(e, ty, name, &scope)?;
            }
            if let Some(w) = selection {
                require_boolean(&binder.expr(w, &scope, &[])?, "WHERE")?;
            }
            Ok(())
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
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
                binder.expr(&mut d.clone(), &empty, &[])?;
            }
            Ok(())
        }
        Statement::AlterTableAddColumn { column, .. } => {
            if let Some(d) = &column.default_value {
                binder.expr(&mut d.clone(), &Scope::new(), &[])?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
