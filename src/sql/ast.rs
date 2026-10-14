//! Typed SQL abstract syntax tree produced by `sql::parser`.

use crate::sql::value::{SqlType, Value};
use crate::storage::row::ColumnType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Plus,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    And,
    Or,
    Concat,
    BitAnd,
    BitOr,
    BitXor,
}

impl BinaryOp {
    pub fn symbol(self) -> &'static str {
        match self {
            BinaryOp::Eq => "=",
            BinaryOp::NotEq => "<>",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "%",
            BinaryOp::And => "AND",
            BinaryOp::Or => "OR",
            BinaryOp::Concat => "||",
            BinaryOp::BitAnd => "&",
            BinaryOp::BitOr => "|",
            BinaryOp::BitXor => "^",
        }
    }

    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Min,
    Max,
    Count,
    Sum,
    Avg,
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

/// An identifier as written in a column reference. Unquoted identifiers match
/// declared names case-insensitively; quoted (`"Name"`) ones match exactly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident {
    pub value: String,
    pub quoted: bool,
}

impl Ident {
    pub fn new(value: impl Into<String>) -> Self {
        Ident { value: value.into(), quoted: false }
    }

    pub fn quoted(value: impl Into<String>) -> Self {
        Ident { value: value.into(), quoted: true }
    }

    /// Whether this reference denotes the declared name `declared`.
    pub fn matches(&self, declared: &str) -> bool {
        if self.quoted { self.value == declared } else { self.value.eq_ignore_ascii_case(declared) }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl From<&str> for Ident {
    fn from(s: &str) -> Self {
        Ident::new(s)
    }
}

impl From<String> for Ident {
    fn from(s: String) -> Self {
        Ident::new(s)
    }
}

impl std::ops::Deref for Ident {
    type Target = str;
    fn deref(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.value)
    }
}

impl PartialEq<str> for Ident {
    fn eq(&self, other: &str) -> bool {
        self.value == other
    }
}

impl PartialEq<&str> for Ident {
    fn eq(&self, other: &&str) -> bool {
        self.value == *other
    }
}

fn ident_sql(id: &Ident) -> String {
    if id.quoted { format!("\"{}\"", id.value.replace('"', "\"\"")) } else { quote_ident(&id.value) }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Value),
    Column {
        table: Option<Ident>,
        name: Ident,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// `IS [NOT] TRUE|FALSE|UNKNOWN`; `value: None` means UNKNOWN.
    IsTruth {
        expr: Box<Expr>,
        value: Option<bool>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        query: Box<Query>,
        negated: bool,
    },
    Exists {
        query: Box<Query>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        negated: bool,
    },
    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        ty: SqlType,
    },
    /// Scalar function call; `name` is upper-cased.
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// `arg: None` is `COUNT(*)`.
    Aggregate {
        func: AggFunc,
        arg: Option<Box<Expr>>,
        distinct: bool,
    },
    ScalarSubquery(Box<Query>),
    /// The `DEFAULT` keyword; only legal as a whole INSERT value / UPDATE SET value.
    Default,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

impl SetOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            SetOperator::Union => "UNION",
            SetOperator::Except => "EXCEPT",
            SetOperator::Intersect => "INTERSECT",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub body: SetExpr,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetExpr {
    Select(Box<Select>),
    SetOp {
        op: SetOperator,
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    pub from: Vec<TableRef>,
    pub joins: Vec<JoinClause>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Wildcard,
    QualifiedWildcard(Ident),
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    Named { name: String, alias: Option<String> },
    Subquery { query: Box<Query>, alias: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableRef,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    NoAction,
    Restrict,
    Cascade,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub parent_table: String,
    pub parent_columns: Vec<String>,
    pub on_delete: Option<Action>,
    pub on_update: Option<Action>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub not_null: bool,
    pub default_value: Option<Expr>,
    pub auto_increment: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSequence {
    pub name: String,
    pub start: i64,
    pub increment: i64,
}

#[derive(Debug, Clone, PartialEq)]
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
    DropIndex {
        name: String,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    },
    Query(Box<Query>),
    Delete {
        table_name: String,
        selection: Option<Expr>,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, Expr)>,
        selection: Option<Expr>,
    },
    CreateSequence(CreateSequence),
    BeginTransaction {
        name: Option<String>,
    },
    Commit,
    Rollback,
    Exit,
}

// ---------------------------------------------------------------------------
// SQL text rendering
// ---------------------------------------------------------------------------

/// Words the parser treats as reserved; identifiers spelled like one of these
/// are rendered quoted so the text parses back to the same AST.
pub const RESERVED_WORDS: &[&str] = &[
    "ALL", "AND", "AS", "ASC", "BETWEEN", "BY", "CASE", "CAST", "CROSS", "DEFAULT", "DESC",
    "DISTINCT", "ELSE", "END", "ESCAPE", "EXCEPT", "EXISTS", "FALSE", "FETCH", "FROM", "FULL",
    "GROUP", "HAVING", "IN", "INNER", "INTERSECT", "INTO", "IS", "JOIN", "LEFT", "LIKE", "LIMIT",
    "NATURAL", "NOT", "NULL", "NULLS", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVER",
    "RIGHT", "SELECT", "SET", "SIMILAR", "THEN", "TRUE", "UNION", "USING", "VALUES", "WHEN",
    "WHERE", "WINDOW", "WITH",
];

pub fn is_reserved_word(word: &str) -> bool {
    RESERVED_WORDS.iter().any(|w| w.eq_ignore_ascii_case(word))
}

/// Render an identifier, quoting it when it is not a plain unreserved word.
pub fn quote_ident(name: &str) -> String {
    let mut chars = name.chars();
    let plain = match chars.next() {
        Some(c) if c.is_alphabetic() || c == '_' => chars.all(|c| c.is_alphanumeric() || c == '_'),
        _ => false,
    };
    if plain && !is_reserved_word(name) {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn literal_sql(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Bool(true) => "TRUE".into(),
        Value::Bool(false) => "FALSE".into(),
        Value::Int(i) => i.to_string(),
        Value::Decimal(d) => {
            let s = d.to_string();
            // An integral decimal must keep a point so it re-lexes as exact numeric.
            if s.contains('.') { s } else { format!("{}.", s) }
        }
        Value::Double(f) => format!("{:e}", f),
        Value::Text(s) => quote_str(s),
        Value::Char(s) => format!("CAST({} AS CHAR({}))", quote_str(s), s.chars().count().max(1)),
        Value::Date(_) => format!("DATE {}", quote_str(&v.to_string())),
        Value::DateTime(_) => format!("CAST({} AS DATETIME)", quote_str(&v.to_string())),
        Value::Timestamp(_) => format!("CAST({} AS TIMESTAMP)", quote_str(&v.to_string())),
        Value::Time(_) => format!("CAST({} AS TIME)", quote_str(&v.to_string())),
        Value::Year(_) => format!("CAST({} AS YEAR)", quote_str(&v.to_string())),
    }
}

fn join_sql(items: &[Expr]) -> String {
    items.iter().map(Expr::to_sql).collect::<Vec<_>>().join(", ")
}

impl Expr {
    /// Round-trippable SQL text for this expression.
    pub fn to_sql(&self) -> String {
        match self {
            Expr::Literal(v) => literal_sql(v),
            Expr::Column { table: Some(t), name } => format!("{}.{}", ident_sql(t), ident_sql(name)),
            Expr::Column { table: None, name } => ident_sql(name),
            Expr::Unary { op, expr } => match op {
                UnaryOp::Neg => format!("(-{})", expr.to_sql()),
                UnaryOp::Plus => format!("(+{})", expr.to_sql()),
                UnaryOp::Not => format!("(NOT {})", expr.to_sql()),
            },
            Expr::Binary { op, left, right } => {
                format!("({} {} {})", left.to_sql(), op.symbol(), right.to_sql())
            }
            Expr::IsNull { expr, negated } => {
                format!("({} IS {}NULL)", expr.to_sql(), if *negated { "NOT " } else { "" })
            }
            Expr::IsTruth { expr, value, negated } => format!(
                "({} IS {}{})",
                expr.to_sql(),
                if *negated { "NOT " } else { "" },
                match value {
                    Some(true) => "TRUE",
                    Some(false) => "FALSE",
                    None => "UNKNOWN",
                }
            ),
            Expr::InList { expr, list, negated } => format!(
                "({} {}IN ({}))",
                expr.to_sql(),
                if *negated { "NOT " } else { "" },
                join_sql(list)
            ),
            Expr::InSubquery { expr, query, negated } => format!(
                "({} {}IN ({}))",
                expr.to_sql(),
                if *negated { "NOT " } else { "" },
                query.to_sql()
            ),
            Expr::Exists { query, negated } => {
                format!("({}EXISTS ({}))", if *negated { "NOT " } else { "" }, query.to_sql())
            }
            Expr::Between { expr, low, high, negated } => format!(
                "({} {}BETWEEN {} AND {})",
                expr.to_sql(),
                if *negated { "NOT " } else { "" },
                low.to_sql(),
                high.to_sql()
            ),
            Expr::Like { expr, pattern, escape, negated } => {
                let mut s = format!(
                    "({} {}LIKE {}",
                    expr.to_sql(),
                    if *negated { "NOT " } else { "" },
                    pattern.to_sql()
                );
                if let Some(e) = escape {
                    s.push_str(&format!(" ESCAPE {}", e.to_sql()));
                }
                s.push(')');
                s
            }
            Expr::Case { operand, whens, else_result } => {
                let mut s = String::from("CASE");
                if let Some(op) = operand {
                    s.push(' ');
                    s.push_str(&op.to_sql());
                }
                for (w, t) in whens {
                    s.push_str(&format!(" WHEN {} THEN {}", w.to_sql(), t.to_sql()));
                }
                if let Some(e) = else_result {
                    s.push_str(&format!(" ELSE {}", e.to_sql()));
                }
                s.push_str(" END");
                s
            }
            Expr::Cast { expr, ty } => format!("CAST({} AS {})", expr.to_sql(), ty.name()),
            Expr::Function { name, args } => {
                if args.is_empty() && name == "CURRENT_TIMESTAMP" {
                    name.clone()
                } else {
                    format!("{}({})", name, join_sql(args))
                }
            }
            Expr::Aggregate { func, arg, distinct } => match arg {
                None => format!("{}(*)", func.as_str()),
                Some(a) => format!(
                    "{}({}{})",
                    func.as_str(),
                    if *distinct { "DISTINCT " } else { "" },
                    a.to_sql()
                ),
            },
            Expr::ScalarSubquery(q) => format!("({})", q.to_sql()),
            Expr::Default => "DEFAULT".into(),
        }
    }

    /// True when this expression contains an aggregate call outside of any
    /// nested subquery.
    pub fn contains_aggregate(&self) -> bool {
        let mut found = false;
        self.walk(&mut |e| {
            if matches!(e, Expr::Aggregate { .. }) {
                found = true;
            }
        });
        found
    }

    /// Pre-order visit of this expression and its children, not descending
    /// into subqueries.
    pub fn walk(&self, f: &mut dyn FnMut(&Expr)) {
        f(self);
        for child in self.children() {
            child.walk(f);
        }
    }

    /// Direct child expressions (subquery bodies excluded).
    pub fn children(&self) -> Vec<&Expr> {
        match self {
            Expr::Literal(_) | Expr::Column { .. } | Expr::Default | Expr::ScalarSubquery(_) => {
                Vec::new()
            }
            Expr::Exists { .. } => Vec::new(),
            Expr::Unary { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::IsTruth { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::InSubquery { expr, .. } => vec![expr],
            Expr::Binary { left, right, .. } => vec![left, right],
            Expr::InList { expr, list, .. } => {
                let mut v: Vec<&Expr> = vec![expr];
                v.extend(list.iter());
                v
            }
            Expr::Between { expr, low, high, .. } => vec![expr, low, high],
            Expr::Like { expr, pattern, escape, .. } => {
                let mut v: Vec<&Expr> = vec![expr, pattern];
                if let Some(e) = escape {
                    v.push(e);
                }
                v
            }
            Expr::Case { operand, whens, else_result } => {
                let mut v: Vec<&Expr> = Vec::new();
                if let Some(o) = operand {
                    v.push(o);
                }
                for (w, t) in whens {
                    v.push(w);
                    v.push(t);
                }
                if let Some(e) = else_result {
                    v.push(e);
                }
                v
            }
            Expr::Function { args, .. } => args.iter().collect(),
            Expr::Aggregate { arg, .. } => arg.iter().map(|a| a.as_ref()).collect(),
        }
    }

    /// Rebuild this expression bottom-up, replacing nodes via `f` (applied
    /// after children are rebuilt). Subquery bodies are left untouched.
    pub fn transform(&self, f: &mut dyn FnMut(Expr) -> Expr) -> Expr {
        let b = |e: &Expr, f: &mut dyn FnMut(Expr) -> Expr| Box::new(e.transform(f));
        let rebuilt = match self {
            Expr::Literal(_) | Expr::Column { .. } | Expr::Default | Expr::ScalarSubquery(_) | Expr::Exists { .. } => {
                self.clone()
            }
            Expr::Unary { op, expr } => Expr::Unary { op: *op, expr: b(expr, f) },
            Expr::Binary { op, left, right } => Expr::Binary { op: *op, left: b(left, f), right: b(right, f) },
            Expr::IsNull { expr, negated } => Expr::IsNull { expr: b(expr, f), negated: *negated },
            Expr::IsTruth { expr, value, negated } => {
                Expr::IsTruth { expr: b(expr, f), value: *value, negated: *negated }
            }
            Expr::InList { expr, list, negated } => Expr::InList {
                expr: b(expr, f),
                list: list.iter().map(|e| e.transform(f)).collect(),
                negated: *negated,
            },
            Expr::InSubquery { expr, query, negated } => {
                Expr::InSubquery { expr: b(expr, f), query: query.clone(), negated: *negated }
            }
            Expr::Between { expr, low, high, negated } => Expr::Between {
                expr: b(expr, f),
                low: b(low, f),
                high: b(high, f),
                negated: *negated,
            },
            Expr::Like { expr, pattern, escape, negated } => Expr::Like {
                expr: b(expr, f),
                pattern: b(pattern, f),
                escape: escape.as_ref().map(|e| b(e, f)),
                negated: *negated,
            },
            Expr::Case { operand, whens, else_result } => Expr::Case {
                operand: operand.as_ref().map(|e| b(e, f)),
                whens: whens.iter().map(|(w, t)| (w.transform(f), t.transform(f))).collect(),
                else_result: else_result.as_ref().map(|e| b(e, f)),
            },
            Expr::Cast { expr, ty } => Expr::Cast { expr: b(expr, f), ty: *ty },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args.iter().map(|e| e.transform(f)).collect(),
            },
            Expr::Aggregate { func, arg, distinct } => Expr::Aggregate {
                func: *func,
                arg: arg.as_ref().map(|e| b(e, f)),
                distinct: *distinct,
            },
        };
        f(rebuilt)
    }
}

fn table_ref_sql(t: &TableRef) -> String {
    match t {
        TableRef::Named { name, alias: Some(a) } => format!("{} AS {}", quote_ident(name), quote_ident(a)),
        TableRef::Named { name, alias: None } => quote_ident(name),
        TableRef::Subquery { query, alias } => format!("({}) AS {}", query.to_sql(), quote_ident(alias)),
    }
}

impl Select {
    pub fn to_sql(&self) -> String {
        let mut s = String::from("SELECT ");
        if self.distinct {
            s.push_str("DISTINCT ");
        }
        let items: Vec<String> = self
            .projection
            .iter()
            .map(|i| match i {
                SelectItem::Wildcard => "*".to_string(),
                SelectItem::QualifiedWildcard(q) => format!("{}.*", ident_sql(q)),
                SelectItem::Expr { expr, alias: Some(a) } => format!("{} AS {}", expr.to_sql(), quote_ident(a)),
                SelectItem::Expr { expr, alias: None } => expr.to_sql(),
            })
            .collect();
        s.push_str(&items.join(", "));
        if !self.from.is_empty() {
            s.push_str(" FROM ");
            s.push_str(&self.from.iter().map(table_ref_sql).collect::<Vec<_>>().join(", "));
        }
        for j in &self.joins {
            let kw = match j.join_type {
                JoinType::Inner => "JOIN",
                JoinType::Left => "LEFT JOIN",
                JoinType::Right => "RIGHT JOIN",
                JoinType::Full => "FULL JOIN",
                JoinType::Cross => "CROSS JOIN",
            };
            s.push_str(&format!(" {} {}", kw, table_ref_sql(&j.table)));
            if let Some(p) = &j.predicate {
                s.push_str(&format!(" ON {}", p.to_sql()));
            }
        }
        if let Some(w) = &self.selection {
            s.push_str(&format!(" WHERE {}", w.to_sql()));
        }
        if !self.group_by.is_empty() {
            s.push_str(&format!(" GROUP BY {}", join_sql(&self.group_by)));
        }
        if let Some(h) = &self.having {
            s.push_str(&format!(" HAVING {}", h.to_sql()));
        }
        s
    }
}

impl SetExpr {
    pub fn to_sql(&self) -> String {
        match self {
            SetExpr::Select(s) => s.to_sql(),
            SetExpr::SetOp { op, all, left, right } => format!(
                "({}) {}{} ({})",
                left.to_sql(),
                op.as_str(),
                if *all { " ALL" } else { "" },
                right.to_sql()
            ),
        }
    }
}

impl Query {
    pub fn to_sql(&self) -> String {
        let mut s = self.body.to_sql();
        if !self.order_by.is_empty() {
            let items: Vec<String> = self
                .order_by
                .iter()
                .map(|o| {
                    let mut t = o.expr.to_sql();
                    if o.descending {
                        t.push_str(" DESC");
                    }
                    match o.nulls_first {
                        Some(true) => t.push_str(" NULLS FIRST"),
                        Some(false) => t.push_str(" NULLS LAST"),
                        None => {}
                    }
                    t
                })
                .collect();
            s.push_str(&format!(" ORDER BY {}", items.join(", ")));
        }
        if let Some(l) = self.limit {
            s.push_str(&format!(" LIMIT {}", l));
        }
        if let Some(o) = self.offset {
            s.push_str(&format!(" OFFSET {}", o));
        }
        s
    }
}
