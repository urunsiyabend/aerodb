//! Fail-closed recursive-descent SQL parser. Every token must be consumed;
//! unsupported syntax is rejected with `FeatureNotSupported` instead of being
//! partially parsed.

use crate::error::{DbError, DbResult};
use crate::sql::ast::{
    Action, AggFunc, BinaryOp, ColumnDef, CreateSequence, Expr, ForeignKey, Ident, JoinClause, JoinType,
    OrderByItem, Query, Select, SelectItem, SetExpr, SetOperator, Statement, TableRef, UnaryOp,
    is_reserved_word,
};
use crate::sql::decimal::Decimal;
use crate::sql::lexer::{Token, TokenKind, tokenize};
use crate::sql::value::{SqlType, Value};
use crate::storage::row::{ColumnType, parse_date};

/// Parse exactly one statement (an optional single trailing `;` is allowed).
pub fn parse_sql(input: &str) -> DbResult<Statement> {
    let mut p = Parser::new(input)?;
    if p.at_eof() || p.peek().is_symbol(";") {
        return Err(DbError::Syntax("empty statement".into()));
    }
    let stmt = p.parse_statement()?;
    p.eat_symbol(";");
    p.expect_eof()?;
    check_top_level(&stmt)?;
    Ok(stmt)
}

/// Parse a `;`-separated script. Comment-only or empty input yields no statements.
pub fn parse_script(input: &str) -> DbResult<Vec<Statement>> {
    let mut p = Parser::new(input)?;
    let mut out = Vec::new();
    loop {
        while p.eat_symbol(";") {}
        if p.at_eof() {
            break;
        }
        let stmt = p.parse_statement()?;
        check_top_level(&stmt)?;
        out.push(stmt);
        if !p.eat_symbol(";") {
            p.expect_eof()?;
            break;
        }
    }
    Ok(out)
}

/// Parse the whole input as one expression.
pub fn parse_expression_sql(input: &str) -> DbResult<Expr> {
    let mut p = Parser::new(input)?;
    let expr = p.parse_expr()?;
    p.expect_eof()?;
    Ok(expr)
}

/// Legacy string-error wrapper around [`parse_sql`].
pub fn parse_statement(input: &str) -> Result<Statement, String> {
    parse_sql(input).map_err(|e| e.to_string())
}

/// A top-level FROM-less SELECT has no scope in which a column reference
/// could ever resolve, so reject such references up front.
fn check_top_level(stmt: &Statement) -> DbResult<()> {
    fn check_set(body: &SetExpr) -> DbResult<()> {
        match body {
            SetExpr::Select(sel) => {
                if !sel.from.is_empty() {
                    return Ok(());
                }
                let mut exprs: Vec<&Expr> = Vec::new();
                for item in &sel.projection {
                    match item {
                        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                            return Err(DbError::Syntax("SELECT * with no tables specified".into()));
                        }
                        SelectItem::Expr { expr, .. } => exprs.push(expr),
                    }
                }
                exprs.extend(sel.selection.iter());
                exprs.extend(sel.group_by.iter());
                exprs.extend(sel.having.iter());
                for e in exprs {
                    let mut missing = None;
                    e.walk(&mut |n| {
                        if let Expr::Column { name, .. } = n {
                            missing.get_or_insert_with(|| name.value.clone());
                        }
                    });
                    if let Some(name) = missing {
                        return Err(DbError::ColumnNotFound(name));
                    }
                }
                Ok(())
            }
            SetExpr::SetOp { left, right, .. } => {
                check_set(left)?;
                check_set(right)
            }
        }
    }
    match stmt {
        Statement::Query(q) => check_set(&q.body),
        _ => Ok(()),
    }
}

/// INSERT column lists and UPDATE SET lists may name each column only once.
/// Two targets are the same column when either could denote the other's
/// spelling under the identifier rule (unquoted: case-insensitive, quoted:
/// exact); the executor re-checks after resolving against the table.
fn reject_duplicate_targets<'a>(names: impl Iterator<Item = &'a Ident>) -> DbResult<()> {
    let mut seen: Vec<&Ident> = Vec::new();
    for n in names {
        if seen.iter().any(|s| s.matches(&n.value) || n.matches(&s.value)) {
            return Err(DbError::Syntax(format!("column \"{}\" specified more than once", n.value)));
        }
        seen.push(n);
    }
    Ok(())
}

fn unsupported(what: &str) -> DbError {
    DbError::FeatureNotSupported(what.to_string())
}

const KNOWN_FUNCTIONS: &[&str] = &[
    "COALESCE",
    "NULLIF",
    "UPPER",
    "LOWER",
    "CHAR_LENGTH",
    "CHARACTER_LENGTH",
    "ABS",
    "CURRENT_TIMESTAMP",
    "GETDATE",
    "GETUTCDATE",
];

fn check_function_arity(name: &str, n: usize) -> DbResult<()> {
    let ok = match name {
        "COALESCE" => n >= 1,
        "NULLIF" => n == 2,
        "UPPER" | "LOWER" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "ABS" => n == 1,
        "CURRENT_TIMESTAMP" | "GETDATE" | "GETUTCDATE" => n == 0,
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(DbError::UndefinedFunction(format!("{} with {} argument(s)", name, n)))
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(input: &str) -> DbResult<Self> {
        Ok(Parser { tokens: tokenize(input)?, pos: 0 })
    }

    // ---- token helpers ------------------------------------------------------

    fn peek(&self) -> &Token {
        &self.tokens[self.pos.min(self.tokens.len() - 1)]
    }

    fn peek_at(&self, n: usize) -> &Token {
        &self.tokens[(self.pos + n).min(self.tokens.len() - 1)]
    }

    fn advance(&mut self) -> Token {
        let t = self.peek().clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek().kind, TokenKind::Eof)
    }

    fn error_here(&self, expected: &str) -> DbError {
        let t = self.peek();
        DbError::Syntax(format!(
            "expected {} but found {} at byte {}",
            expected,
            t.describe(),
            t.offset
        ))
    }

    fn expect_eof(&self) -> DbResult<()> {
        if self.at_eof() {
            Ok(())
        } else {
            let t = self.peek();
            Err(DbError::Syntax(format!(
                "unexpected {} at byte {}",
                t.describe(),
                t.offset
            )))
        }
    }

    fn is_kw(&self, kw: &str) -> bool {
        self.peek().is_keyword(kw)
    }

    fn is_kw_at(&self, n: usize, kw: &str) -> bool {
        self.peek_at(n).is_keyword(kw)
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.is_kw(kw) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> DbResult<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(self.error_here(kw))
        }
    }

    fn is_symbol(&self, s: &str) -> bool {
        self.peek().is_symbol(s)
    }

    fn eat_symbol(&mut self, s: &str) -> bool {
        if self.is_symbol(s) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_symbol(&mut self, s: &str) -> DbResult<()> {
        if self.eat_symbol(s) {
            Ok(())
        } else {
            Err(self.error_here(&format!("'{}'", s)))
        }
    }

    /// An identifier: unquoted non-reserved word or quoted identifier.
    fn parse_ident(&mut self) -> DbResult<String> {
        match &self.peek().kind {
            TokenKind::Ident(s) if !is_reserved_word(s) => {
                let s = s.clone();
                self.advance();
                Ok(s)
            }
            TokenKind::QuotedIdent(s) => {
                let s = s.clone();
                self.advance();
                Ok(s)
            }
            _ => Err(self.error_here("identifier")),
        }
    }

    /// Like [`Self::parse_ident`], recording whether the identifier was quoted.
    fn parse_ident_ref(&mut self) -> DbResult<Ident> {
        let quoted = matches!(self.peek().kind, TokenKind::QuotedIdent(_));
        let value = self.parse_ident()?;
        Ok(Ident { value, quoted })
    }

    fn at_ident(&self) -> bool {
        match &self.peek().kind {
            TokenKind::Ident(s) => !is_reserved_word(s),
            TokenKind::QuotedIdent(_) => true,
            _ => false,
        }
    }

    fn parse_ident_list(&mut self) -> DbResult<Vec<Ident>> {
        self.expect_symbol("(")?;
        let mut out = vec![self.parse_ident_ref()?];
        while self.eat_symbol(",") {
            out.push(self.parse_ident_ref()?);
        }
        self.expect_symbol(")")?;
        Ok(out)
    }

    fn parse_u64(&mut self) -> DbResult<u64> {
        match &self.peek().kind {
            TokenKind::Integer(s) => {
                let v = s
                    .parse::<u64>()
                    .map_err(|_| DbError::Syntax(format!("integer {} out of range", s)))?;
                self.advance();
                Ok(v)
            }
            _ => Err(self.error_here("non-negative integer")),
        }
    }

    fn parse_signed_i64(&mut self) -> DbResult<i64> {
        let neg = if self.eat_symbol("-") {
            true
        } else {
            self.eat_symbol("+");
            false
        };
        let v = self.parse_u64()?;
        let v = i64::try_from(v).map_err(|_| DbError::Syntax(format!("integer {} out of range", v)))?;
        Ok(if neg { -v } else { v })
    }

    // ---- statements ---------------------------------------------------------

    fn parse_statement(&mut self) -> DbResult<Statement> {
        if self.is_symbol("(") {
            return Ok(Statement::Query(Box::new(self.parse_query()?)));
        }
        if self.is_symbol(".") && self.is_kw_at(1, "EXIT") {
            self.advance();
            self.advance();
            return Ok(Statement::Exit);
        }
        let word = match &self.peek().kind {
            TokenKind::Ident(s) => s.to_ascii_uppercase(),
            _ => return Err(self.error_here("statement")),
        };
        match word.as_str() {
            "SELECT" => Ok(Statement::Query(Box::new(self.parse_query()?))),
            "WITH" => Err(unsupported("WITH (common table expressions)")),
            "INSERT" => self.parse_insert(),
            "UPDATE" => self.parse_update(),
            "DELETE" => self.parse_delete(),
            "CREATE" => self.parse_create(),
            "DROP" => self.parse_drop(),
            "ALTER" => Err(unsupported("ALTER TABLE")),
            "BEGIN" => {
                self.advance();
                self.eat_kw("TRANSACTION");
                let name = match &self.peek().kind {
                    TokenKind::Ident(s) if !is_reserved_word(s) => Some(s.clone()),
                    TokenKind::QuotedIdent(s) | TokenKind::String(s) => Some(s.clone()),
                    _ => None,
                };
                if name.is_some() {
                    self.advance();
                }
                Ok(Statement::BeginTransaction { name })
            }
            "COMMIT" => {
                self.advance();
                self.eat_kw("WORK");
                Ok(Statement::Commit)
            }
            "ROLLBACK" => {
                self.advance();
                self.eat_kw("WORK");
                if self.is_kw("TO") {
                    return Err(unsupported("ROLLBACK TO SAVEPOINT"));
                }
                Ok(Statement::Rollback)
            }
            "SAVEPOINT" => Err(unsupported("SAVEPOINT")),
            "RELEASE" => Err(unsupported("RELEASE SAVEPOINT")),
            "DECLARE" => Err(unsupported("DECLARE (cursors)")),
            "SET" => {
                if self.is_kw_at(1, "TRANSACTION") {
                    Err(unsupported("SET TRANSACTION"))
                } else {
                    Err(self.error_here("statement"))
                }
            }
            "EXIT" => {
                self.advance();
                Ok(Statement::Exit)
            }
            _ => Err(self.error_here("statement")),
        }
    }

    fn parse_insert(&mut self) -> DbResult<Statement> {
        self.expect_kw("INSERT")?;
        self.expect_kw("INTO")?;
        let table_name = self.parse_ident_ref()?;
        let columns = if self.is_symbol("(") {
            if self.is_kw_at(1, "SELECT") {
                return Err(unsupported("INSERT ... SELECT"));
            }
            let cols = self.parse_ident_list()?;
            reject_duplicate_targets(cols.iter())?;
            Some(cols)
        } else {
            None
        };
        if self.is_kw("SELECT") || self.is_kw("WITH") || (self.is_symbol("(") && self.is_kw_at(1, "SELECT")) {
            return Err(unsupported("INSERT ... SELECT"));
        }
        self.expect_kw("VALUES")?;
        let mut rows = Vec::new();
        loop {
            self.expect_symbol("(")?;
            let mut row = Vec::new();
            loop {
                if self.is_kw("DEFAULT") && (self.peek_at(1).is_symbol(",") || self.peek_at(1).is_symbol(")")) {
                    self.advance();
                    row.push(Expr::Default);
                } else {
                    row.push(self.parse_expr()?);
                }
                if !self.eat_symbol(",") {
                    break;
                }
            }
            self.expect_symbol(")")?;
            rows.push(row);
            if !self.eat_symbol(",") {
                break;
            }
        }
        Ok(Statement::Insert { table_name, columns, rows })
    }

    fn parse_update(&mut self) -> DbResult<Statement> {
        self.expect_kw("UPDATE")?;
        let table_name = self.parse_ident_ref()?;
        self.expect_kw("SET")?;
        let mut assignments = Vec::new();
        loop {
            let col = self.parse_ident_ref()?;
            self.expect_symbol("=")?;
            let value = if self.is_kw("DEFAULT")
                && (self.peek_at(1).is_symbol(",") || self.peek_at(1).is_keyword("WHERE") || self.peek_at(1).is_symbol(";") || matches!(self.peek_at(1).kind, TokenKind::Eof))
            {
                self.advance();
                Expr::Default
            } else {
                self.parse_expr()?
            };
            assignments.push((col, value));
            if !self.eat_symbol(",") {
                break;
            }
        }
        reject_duplicate_targets(assignments.iter().map(|(c, _)| c))?;
        let selection = if self.eat_kw("WHERE") { Some(self.parse_expr()?) } else { None };
        Ok(Statement::Update { table_name, assignments, selection })
    }

    fn parse_delete(&mut self) -> DbResult<Statement> {
        self.expect_kw("DELETE")?;
        self.expect_kw("FROM")?;
        let table_name = self.parse_ident_ref()?;
        let selection = if self.eat_kw("WHERE") { Some(self.parse_expr()?) } else { None };
        Ok(Statement::Delete { table_name, selection })
    }

    fn parse_drop(&mut self) -> DbResult<Statement> {
        self.expect_kw("DROP")?;
        if self.eat_kw("TABLE") {
            let mut if_exists = false;
            if self.is_kw("IF") && self.is_kw_at(1, "EXISTS") {
                self.advance();
                self.advance();
                if_exists = true;
            }
            let table_name = self.parse_ident()?;
            if self.is_kw("CASCADE") {
                return Err(unsupported("DROP TABLE ... CASCADE"));
            }
            self.eat_kw("RESTRICT");
            Ok(Statement::DropTable { table_name, if_exists })
        } else if self.eat_kw("INDEX") {
            let name = self.parse_ident()?;
            Ok(Statement::DropIndex { name })
        } else if self.is_kw("VIEW") {
            Err(unsupported("DROP VIEW"))
        } else {
            Err(self.error_here("TABLE or INDEX"))
        }
    }

    fn parse_create(&mut self) -> DbResult<Statement> {
        self.expect_kw("CREATE")?;
        if self.eat_kw("SEQUENCE") {
            let name = self.parse_ident()?;
            let mut start = 1i64;
            let mut increment = 1i64;
            loop {
                if self.is_kw("START") {
                    self.advance();
                    self.expect_kw("WITH")?;
                    start = self.parse_signed_i64()?;
                } else if self.is_kw("INCREMENT") {
                    self.advance();
                    self.expect_kw("BY")?;
                    increment = self.parse_signed_i64()?;
                    if increment == 0 {
                        return Err(DbError::Syntax("INCREMENT BY cannot be zero".into()));
                    }
                } else {
                    break;
                }
            }
            return Ok(Statement::CreateSequence(CreateSequence { name, start, increment }));
        }
        if self.eat_kw("INDEX") {
            let index_name = self.parse_ident()?;
            self.expect_kw("ON")?;
            let table_name = self.parse_ident_ref()?;
            self.expect_symbol("(")?;
            let column_name = self.parse_ident_ref()?;
            if self.is_symbol(",") {
                return Err(unsupported("multi-column indexes"));
            }
            self.expect_symbol(")")?;
            return Ok(Statement::CreateIndex { index_name, table_name, column_name });
        }
        if self.is_kw("UNIQUE") {
            return Err(unsupported("CREATE UNIQUE INDEX"));
        }
        if self.is_kw("VIEW") || (self.is_kw("OR") && self.is_kw_at(1, "REPLACE")) {
            return Err(unsupported("CREATE VIEW"));
        }
        self.expect_kw("TABLE")?;
        let mut if_not_exists = false;
        if self.is_kw("IF") {
            self.advance();
            self.expect_kw("NOT")?;
            self.expect_kw("EXISTS")?;
            if_not_exists = true;
        }
        let table_name = self.parse_ident()?;
        self.expect_symbol("(")?;
        let mut columns: Vec<ColumnDef> = Vec::new();
        let mut fks = Vec::new();
        let mut primary_key: Option<Vec<Ident>> = None;
        loop {
            if self.is_kw("PRIMARY") {
                self.advance();
                self.expect_kw("KEY")?;
                let cols = self.parse_ident_list()?;
                if primary_key.is_some() {
                    return Err(DbError::Syntax("multiple primary keys defined".into()));
                }
                primary_key = Some(cols);
            } else if self.is_kw("FOREIGN") {
                self.advance();
                self.expect_kw("KEY")?;
                fks.push(self.parse_foreign_key()?);
            } else if self.is_kw("UNIQUE") {
                return Err(unsupported("UNIQUE constraints"));
            } else if self.is_kw("CHECK") {
                return Err(unsupported("CHECK constraints"));
            } else if self.is_kw("CONSTRAINT") {
                return Err(unsupported("named constraints (CONSTRAINT name)"));
            } else {
                columns.push(self.parse_column_def()?);
            }
            if !self.eat_symbol(",") {
                break;
            }
        }
        self.expect_symbol(")")?;
        if columns.is_empty() {
            return Err(DbError::Syntax("at least one column required".into()));
        }
        // An inline PRIMARY KEY names the declared column itself: match it exactly.
        let inline_pk: Vec<Ident> =
            columns.iter().filter(|c| c.primary_key).map(|c| Ident::quoted(c.name.clone())).collect();
        if !inline_pk.is_empty() {
            if primary_key.is_some() {
                return Err(DbError::Syntax("multiple primary keys defined".into()));
            }
            primary_key = Some(inline_pk);
        }
        Ok(Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists })
    }

    fn parse_foreign_key(&mut self) -> DbResult<ForeignKey> {
        let columns = self.parse_ident_list()?;
        self.expect_kw("REFERENCES")?;
        let parent_table = self.parse_ident_ref()?;
        let parent_columns = self.parse_ident_list()?;
        let mut on_delete = None;
        let mut on_update = None;
        while self.is_kw("ON") {
            self.advance();
            let is_delete = if self.eat_kw("DELETE") {
                true
            } else if self.eat_kw("UPDATE") {
                false
            } else {
                return Err(self.error_here("DELETE or UPDATE"));
            };
            let action = if self.eat_kw("CASCADE") {
                Action::Cascade
            } else if self.eat_kw("RESTRICT") {
                Action::Restrict
            } else if self.is_kw("NO") {
                self.advance();
                self.expect_kw("ACTION")?;
                Action::NoAction
            } else if self.is_kw("SET") {
                return Err(unsupported("ON DELETE/UPDATE SET NULL / SET DEFAULT"));
            } else {
                return Err(self.error_here("referential action"));
            };
            if is_delete {
                on_delete = Some(action);
            } else {
                on_update = Some(action);
            }
        }
        Ok(ForeignKey { columns, parent_table, parent_columns, on_delete, on_update })
    }

    fn parse_column_def(&mut self) -> DbResult<ColumnDef> {
        let name = self.parse_ident()?;
        let col_type = self.parse_column_type()?;
        let mut not_null = false;
        let mut default_value = None;
        let mut auto_increment = false;
        let mut primary_key = false;
        loop {
            if self.is_kw("NOT") && self.is_kw_at(1, "NULL") {
                self.advance();
                self.advance();
                not_null = true;
            } else if self.eat_kw("NULL") {
                // explicit nullability, the default
            } else if self.eat_kw("DEFAULT") {
                let expr = self.parse_expr()?;
                let mut constant = true;
                expr.walk(&mut |e| {
                    if matches!(
                        e,
                        Expr::Column { .. } | Expr::Aggregate { .. } | Expr::ScalarSubquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. }
                    ) {
                        constant = false;
                    }
                });
                if !constant {
                    return Err(DbError::Syntax("DEFAULT must be a constant expression".into()));
                }
                default_value = Some(expr);
            } else if self.is_kw("PRIMARY") {
                self.advance();
                self.expect_kw("KEY")?;
                primary_key = true;
            } else if self.eat_kw("AUTO_INCREMENT") {
                auto_increment = true;
            } else if self.is_kw("UNIQUE") {
                return Err(unsupported("UNIQUE constraints"));
            } else if self.is_kw("CHECK") {
                return Err(unsupported("CHECK constraints"));
            } else if self.is_kw("REFERENCES") {
                return Err(unsupported("column REFERENCES constraints"));
            } else if self.is_kw("CONSTRAINT") {
                return Err(unsupported("named constraints (CONSTRAINT name)"));
            } else if self.is_symbol(",") || self.is_symbol(")") {
                break;
            } else {
                return Err(self.error_here("column constraint, ',' or ')'"));
            }
        }
        if auto_increment {
            let is_int = matches!(
                col_type,
                ColumnType::Integer | ColumnType::SmallInt { .. } | ColumnType::MediumInt { .. }
            );
            if !is_int {
                return Err(DbError::Syntax("AUTO_INCREMENT can only be used with integer columns".into()));
            }
            if !not_null {
                return Err(DbError::Syntax("AUTO_INCREMENT columns must be NOT NULL".into()));
            }
        }
        Ok(ColumnDef { name, col_type, not_null, default_value, auto_increment, primary_key })
    }

    /// Reads the type words of a column definition and resolves them through
    /// `ColumnType::from_str`.
    fn parse_column_type(&mut self) -> DbResult<ColumnType> {
        let start = self.peek().offset;
        let mut text = match &self.peek().kind {
            TokenKind::Ident(s) => s.clone(),
            _ => return Err(self.error_here("column type")),
        };
        self.advance();
        while self.is_kw("VARYING") || self.is_kw("PRECISION") {
            text.push(' ');
            text.push_str(&self.advance().describe());
        }
        if self.eat_symbol("(") {
            text.push('(');
            text.push_str(&self.parse_u64()?.to_string());
            if self.eat_symbol(",") {
                text.push(',');
                text.push_str(&self.parse_u64()?.to_string());
            }
            self.expect_symbol(")")?;
            text.push(')');
        }
        if self.eat_kw("UNSIGNED") {
            text.push_str(" UNSIGNED");
        }
        ColumnType::from_str(&text)
            .ok_or_else(|| DbError::Syntax(format!("unknown or invalid type {} at byte {}", text, start)))
    }

    // ---- queries ------------------------------------------------------------

    fn parse_query(&mut self) -> DbResult<Query> {
        if self.is_kw("WITH") {
            return Err(unsupported("WITH (common table expressions)"));
        }
        let body = self.parse_set_expr()?;
        let mut order_by = Vec::new();
        if self.is_kw("ORDER") {
            self.advance();
            self.expect_kw("BY")?;
            loop {
                order_by.push(self.parse_order_item()?);
                if !self.eat_symbol(",") {
                    break;
                }
            }
        }
        let mut limit = None;
        let mut offset = None;
        loop {
            if limit.is_none() && self.eat_kw("LIMIT") {
                limit = Some(self.parse_u64()?);
            } else if offset.is_none() && self.eat_kw("OFFSET") {
                offset = Some(self.parse_u64()?);
            } else {
                break;
            }
        }
        if self.is_kw("FETCH") {
            return Err(unsupported("FETCH FIRST"));
        }
        Ok(Query { body, order_by, limit, offset })
    }

    fn parse_order_item(&mut self) -> DbResult<OrderByItem> {
        let expr = self.parse_expr()?;
        let descending = if self.eat_kw("DESC") {
            true
        } else {
            self.eat_kw("ASC");
            false
        };
        let nulls_first = if self.eat_kw("NULLS") {
            if self.eat_kw("FIRST") {
                Some(true)
            } else if self.eat_kw("LAST") {
                Some(false)
            } else {
                return Err(self.error_here("FIRST or LAST"));
            }
        } else {
            None
        };
        Ok(OrderByItem { expr, descending, nulls_first })
    }

    fn parse_set_quantifier(&mut self) -> bool {
        if self.eat_kw("ALL") {
            true
        } else {
            self.eat_kw("DISTINCT");
            false
        }
    }

    fn parse_set_expr(&mut self) -> DbResult<SetExpr> {
        let mut left = self.parse_intersect_term()?;
        loop {
            let op = if self.eat_kw("UNION") {
                SetOperator::Union
            } else if self.eat_kw("EXCEPT") {
                SetOperator::Except
            } else {
                break;
            };
            let all = self.parse_set_quantifier();
            let right = self.parse_intersect_term()?;
            left = SetExpr::SetOp { op, all, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_intersect_term(&mut self) -> DbResult<SetExpr> {
        let mut left = self.parse_query_primary()?;
        while self.eat_kw("INTERSECT") {
            let all = self.parse_set_quantifier();
            let right = self.parse_query_primary()?;
            left = SetExpr::SetOp { op: SetOperator::Intersect, all, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_query_primary(&mut self) -> DbResult<SetExpr> {
        if self.eat_symbol("(") {
            let q = self.parse_query()?;
            self.expect_symbol(")")?;
            if !q.order_by.is_empty() || q.limit.is_some() || q.offset.is_some() {
                return Err(unsupported("ORDER BY/LIMIT/OFFSET inside a parenthesized set-operation operand"));
            }
            return Ok(q.body);
        }
        if self.is_kw("VALUES") {
            return Err(unsupported("VALUES as a query"));
        }
        Ok(SetExpr::Select(Box::new(self.parse_select()?)))
    }

    fn parse_select(&mut self) -> DbResult<Select> {
        self.expect_kw("SELECT")?;
        let distinct = if self.eat_kw("DISTINCT") {
            true
        } else {
            self.eat_kw("ALL");
            false
        };
        let mut projection = Vec::new();
        loop {
            projection.push(self.parse_select_item()?);
            if !self.eat_symbol(",") {
                break;
            }
        }
        let mut from = Vec::new();
        let mut joins = Vec::new();
        if self.eat_kw("FROM") {
            from.push(self.parse_table_ref()?);
            loop {
                if self.eat_symbol(",") {
                    if !joins.is_empty() {
                        return Err(unsupported("comma-separated table after a JOIN"));
                    }
                    from.push(self.parse_table_ref()?);
                    continue;
                }
                if let Some(join) = self.parse_join()? {
                    joins.push(join);
                    continue;
                }
                break;
            }
        }
        let selection = if self.eat_kw("WHERE") { Some(self.parse_expr()?) } else { None };
        let mut group_by = Vec::new();
        if self.is_kw("GROUP") {
            self.advance();
            self.expect_kw("BY")?;
            loop {
                group_by.push(self.parse_expr()?);
                if !self.eat_symbol(",") {
                    break;
                }
            }
        }
        let having = if self.eat_kw("HAVING") { Some(self.parse_expr()?) } else { None };
        if self.is_kw("WINDOW") {
            return Err(unsupported("WINDOW clause"));
        }
        Ok(Select { distinct, projection, from, joins, selection, group_by, having })
    }

    fn parse_select_item(&mut self) -> DbResult<SelectItem> {
        if self.eat_symbol("*") {
            return Ok(SelectItem::Wildcard);
        }
        if self.at_ident() && self.peek_at(1).is_symbol(".") && self.peek_at(2).is_symbol("*") {
            let q = self.parse_ident_ref()?;
            self.advance();
            self.advance();
            return Ok(SelectItem::QualifiedWildcard(q));
        }
        let expr = self.parse_expr()?;
        let alias = self.parse_optional_alias()?;
        Ok(SelectItem::Expr { expr, alias })
    }

    fn parse_optional_alias(&mut self) -> DbResult<Option<String>> {
        if self.eat_kw("AS") {
            return Ok(Some(self.parse_ident()?));
        }
        if self.at_ident() {
            return Ok(Some(self.parse_ident()?));
        }
        Ok(None)
    }

    fn parse_table_ref(&mut self) -> DbResult<TableRef> {
        if self.eat_symbol("(") {
            if !(self.is_kw("SELECT") || self.is_kw("WITH") || self.is_symbol("(")) {
                return Err(unsupported("parenthesized joins in FROM"));
            }
            let query = self.parse_query()?;
            self.expect_symbol(")")?;
            let alias = self
                .parse_optional_alias()?
                .ok_or_else(|| DbError::Syntax("subquery in FROM must have an alias".into()))?;
            return Ok(TableRef::Subquery { query: Box::new(query), alias });
        }
        let name = self.parse_ident_ref()?;
        if self.is_symbol(".") {
            return Err(unsupported("schema-qualified table names"));
        }
        let alias = self.parse_optional_alias()?;
        Ok(TableRef::Named { name, alias })
    }

    fn parse_join(&mut self) -> DbResult<Option<JoinClause>> {
        let join_type = if self.eat_kw("JOIN") {
            JoinType::Inner
        } else if self.is_kw("INNER") {
            self.advance();
            self.expect_kw("JOIN")?;
            JoinType::Inner
        } else if self.is_kw("LEFT") || self.is_kw("RIGHT") || self.is_kw("FULL") {
            let t = if self.eat_kw("LEFT") {
                JoinType::Left
            } else if self.eat_kw("RIGHT") {
                JoinType::Right
            } else {
                self.advance();
                JoinType::Full
            };
            self.eat_kw("OUTER");
            self.expect_kw("JOIN")?;
            t
        } else if self.is_kw("CROSS") {
            self.advance();
            self.expect_kw("JOIN")?;
            JoinType::Cross
        } else if self.is_kw("NATURAL") {
            return Err(unsupported("NATURAL JOIN"));
        } else {
            return Ok(None);
        };
        let table = self.parse_table_ref()?;
        let predicate = if join_type == JoinType::Cross {
            if self.is_kw("ON") {
                return Err(DbError::Syntax("CROSS JOIN does not take an ON clause".into()));
            }
            None
        } else {
            if self.is_kw("USING") {
                return Err(unsupported("JOIN ... USING"));
            }
            self.expect_kw("ON")?;
            Some(self.parse_expr()?)
        };
        Ok(Some(JoinClause { join_type, table, predicate }))
    }

    // ---- expressions --------------------------------------------------------

    fn parse_expr(&mut self) -> DbResult<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> DbResult<Expr> {
        let mut left = self.parse_and()?;
        while self.eat_kw("OR") {
            let right = self.parse_and()?;
            left = Expr::Binary { op: BinaryOp::Or, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> DbResult<Expr> {
        let mut left = self.parse_not()?;
        while self.eat_kw("AND") {
            let right = self.parse_not()?;
            left = Expr::Binary { op: BinaryOp::And, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> DbResult<Expr> {
        if self.eat_kw("NOT") {
            let inner = self.parse_not()?;
            return Ok(Expr::Unary { op: UnaryOp::Not, expr: Box::new(inner) });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> DbResult<Expr> {
        let mut left = self.parse_bitor()?;
        loop {
            let cmp = match &self.peek().kind {
                TokenKind::Symbol("=") => Some(BinaryOp::Eq),
                TokenKind::Symbol("<>") | TokenKind::Symbol("!=") => Some(BinaryOp::NotEq),
                TokenKind::Symbol("<") => Some(BinaryOp::Lt),
                TokenKind::Symbol("<=") => Some(BinaryOp::LtEq),
                TokenKind::Symbol(">") => Some(BinaryOp::Gt),
                TokenKind::Symbol(">=") => Some(BinaryOp::GtEq),
                _ => None,
            };
            if let Some(op) = cmp {
                self.advance();
                if self.is_kw("ANY") || self.is_kw("SOME") || self.is_kw("ALL") {
                    return Err(unsupported("quantified comparisons (ANY/SOME/ALL)"));
                }
                let right = self.parse_bitor()?;
                left = Expr::Binary { op, left: Box::new(left), right: Box::new(right) };
                continue;
            }
            if self.is_kw("IS") {
                self.advance();
                let negated = self.eat_kw("NOT");
                if self.eat_kw("NULL") {
                    left = Expr::IsNull { expr: Box::new(left), negated };
                } else if self.eat_kw("TRUE") {
                    left = Expr::IsTruth { expr: Box::new(left), value: Some(true), negated };
                } else if self.eat_kw("FALSE") {
                    left = Expr::IsTruth { expr: Box::new(left), value: Some(false), negated };
                } else if self.eat_kw("UNKNOWN") {
                    left = Expr::IsTruth { expr: Box::new(left), value: None, negated };
                } else if self.is_kw("DISTINCT") {
                    return Err(unsupported("IS [NOT] DISTINCT FROM"));
                } else {
                    return Err(self.error_here("NULL, TRUE, FALSE or UNKNOWN"));
                }
                continue;
            }
            if (self.is_kw("SIMILAR") && self.is_kw_at(1, "TO"))
                || (self.is_kw("NOT") && self.is_kw_at(1, "SIMILAR") && self.is_kw_at(2, "TO"))
            {
                return Err(unsupported("SIMILAR TO"));
            }
            let negated = self.is_kw("NOT")
                && (self.is_kw_at(1, "IN") || self.is_kw_at(1, "BETWEEN") || self.is_kw_at(1, "LIKE"));
            if negated {
                self.advance();
            }
            if self.eat_kw("IN") {
                self.expect_symbol("(")?;
                if self.is_kw("SELECT") || self.is_kw("WITH") {
                    let query = self.parse_query()?;
                    self.expect_symbol(")")?;
                    left = Expr::InSubquery { expr: Box::new(left), query: Box::new(query), negated };
                } else {
                    let mut list = vec![self.parse_expr()?];
                    while self.eat_symbol(",") {
                        list.push(self.parse_expr()?);
                    }
                    self.expect_symbol(")")?;
                    left = Expr::InList { expr: Box::new(left), list, negated };
                }
                continue;
            }
            if self.eat_kw("BETWEEN") {
                if self.is_kw("SYMMETRIC") || self.is_kw("ASYMMETRIC") {
                    return Err(unsupported("BETWEEN SYMMETRIC/ASYMMETRIC"));
                }
                let low = self.parse_bitor()?;
                self.expect_kw("AND")?;
                let high = self.parse_bitor()?;
                left = Expr::Between { expr: Box::new(left), low: Box::new(low), high: Box::new(high), negated };
                continue;
            }
            if self.eat_kw("LIKE") {
                let pattern = self.parse_bitor()?;
                let escape = if self.eat_kw("ESCAPE") { Some(Box::new(self.parse_bitor()?)) } else { None };
                left = Expr::Like { expr: Box::new(left), pattern: Box::new(pattern), escape, negated };
                continue;
            }
            break;
        }
        Ok(left)
    }

    fn parse_binary_level(
        &mut self,
        ops: &[(&str, BinaryOp)],
        next: fn(&mut Self) -> DbResult<Expr>,
    ) -> DbResult<Expr> {
        let mut left = next(self)?;
        'outer: loop {
            for (sym, op) in ops {
                if self.is_symbol(sym) {
                    self.advance();
                    let right = next(self)?;
                    left = Expr::Binary { op: *op, left: Box::new(left), right: Box::new(right) };
                    continue 'outer;
                }
            }
            break;
        }
        Ok(left)
    }

    fn parse_bitor(&mut self) -> DbResult<Expr> {
        self.parse_binary_level(&[("|", BinaryOp::BitOr)], Self::parse_bitxor)
    }

    fn parse_bitxor(&mut self) -> DbResult<Expr> {
        self.parse_binary_level(&[("^", BinaryOp::BitXor)], Self::parse_bitand)
    }

    fn parse_bitand(&mut self) -> DbResult<Expr> {
        self.parse_binary_level(&[("&", BinaryOp::BitAnd)], Self::parse_additive)
    }

    fn parse_additive(&mut self) -> DbResult<Expr> {
        self.parse_binary_level(
            &[("+", BinaryOp::Add), ("-", BinaryOp::Sub), ("||", BinaryOp::Concat)],
            Self::parse_multiplicative,
        )
    }

    fn parse_multiplicative(&mut self) -> DbResult<Expr> {
        self.parse_binary_level(
            &[("*", BinaryOp::Mul), ("/", BinaryOp::Div), ("%", BinaryOp::Mod)],
            Self::parse_unary,
        )
    }

    fn parse_unary(&mut self) -> DbResult<Expr> {
        if self.eat_symbol("-") {
            let inner = self.parse_unary()?;
            return Ok(Expr::Unary { op: UnaryOp::Neg, expr: Box::new(inner) });
        }
        if self.eat_symbol("+") {
            let inner = self.parse_unary()?;
            return Ok(Expr::Unary { op: UnaryOp::Plus, expr: Box::new(inner) });
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> DbResult<Expr> {
        let tok = self.peek().clone();
        match &tok.kind {
            TokenKind::Integer(s) => {
                self.advance();
                Ok(Expr::Literal(match s.parse::<i64>() {
                    Ok(i) => Value::Int(i),
                    Err(_) => Value::Decimal(Decimal::parse(s).map_err(|_| {
                        DbError::NumericOverflow(format!("integer literal {} is too large", s))
                    })?),
                }))
            }
            TokenKind::Decimal(s) => {
                self.advance();
                let d = Decimal::parse(s)
                    .map_err(|_| DbError::NumericOverflow(format!("numeric literal {} is out of range", s)))?;
                Ok(Expr::Literal(Value::Decimal(d)))
            }
            TokenKind::Float(s) => {
                self.advance();
                match s.parse::<f64>() {
                    Ok(f) if f.is_finite() => Ok(Expr::Literal(Value::Double(f))),
                    _ => Err(DbError::NumericOverflow(format!("numeric literal {} is out of range", s))),
                }
            }
            TokenKind::String(s) => {
                self.advance();
                Ok(Expr::Literal(Value::Text(s.clone())))
            }
            TokenKind::Symbol("(") => {
                self.advance();
                if self.is_kw("SELECT") || self.is_kw("WITH") {
                    let q = self.parse_query()?;
                    self.expect_symbol(")")?;
                    return Ok(Expr::ScalarSubquery(Box::new(q)));
                }
                let e = self.parse_expr()?;
                if self.is_symbol(",") {
                    return Err(unsupported("row value constructors"));
                }
                self.expect_symbol(")")?;
                Ok(e)
            }
            TokenKind::QuotedIdent(_) => self.parse_column_ref(),
            TokenKind::Ident(word) => {
                let upper = word.to_ascii_uppercase();
                match upper.as_str() {
                    "NULL" => {
                        self.advance();
                        Ok(Expr::Literal(Value::Null))
                    }
                    "TRUE" => {
                        self.advance();
                        Ok(Expr::Literal(Value::Bool(true)))
                    }
                    "FALSE" => {
                        self.advance();
                        Ok(Expr::Literal(Value::Bool(false)))
                    }
                    "CASE" => self.parse_case(),
                    "CAST" => self.parse_cast(),
                    "EXISTS" => {
                        self.advance();
                        self.expect_symbol("(")?;
                        let q = self.parse_query()?;
                        self.expect_symbol(")")?;
                        Ok(Expr::Exists { query: Box::new(q), negated: false })
                    }
                    "DATE" if matches!(self.peek_at(1).kind, TokenKind::String(_)) => {
                        self.advance();
                        let lit = match self.advance().kind {
                            TokenKind::String(s) => s,
                            _ => unreachable!("checked above"),
                        };
                        let days = parse_date(lit.trim())
                            .ok_or_else(|| DbError::InvalidCast(format!("'{}' is not a valid DATE", lit)))?;
                        Ok(Expr::Literal(Value::Date(days)))
                    }
                    "TIMESTAMP" | "TIME" if matches!(self.peek_at(1).kind, TokenKind::String(_)) => {
                        Err(unsupported("TIMESTAMP/TIME typed literals"))
                    }
                    "INTERVAL" if matches!(self.peek_at(1).kind, TokenKind::String(_)) => {
                        Err(unsupported("INTERVAL literals"))
                    }
                    "CURRENT_TIMESTAMP" if !self.peek_at(1).is_symbol("(") => {
                        self.advance();
                        Ok(Expr::Function { name: "CURRENT_TIMESTAMP".into(), args: Vec::new() })
                    }
                    _ if is_reserved_word(word) => Err(self.error_here("expression")),
                    _ if self.peek_at(1).is_symbol("(") => self.parse_function_call(),
                    _ => self.parse_column_ref(),
                }
            }
            _ => Err(self.error_here("expression")),
        }
    }

    fn parse_column_ref(&mut self) -> DbResult<Expr> {
        let first = self.parse_ident_ref()?;
        if self.eat_symbol(".") {
            let name = self.parse_ident_ref()?;
            if self.is_symbol(".") {
                return Err(unsupported("schema-qualified column references"));
            }
            return Ok(Expr::Column { table: Some(first), name });
        }
        Ok(Expr::Column { table: None, name: first })
    }

    fn parse_case(&mut self) -> DbResult<Expr> {
        self.expect_kw("CASE")?;
        let operand = if self.is_kw("WHEN") { None } else { Some(Box::new(self.parse_expr()?)) };
        let mut whens = Vec::new();
        while self.eat_kw("WHEN") {
            let cond = self.parse_expr()?;
            self.expect_kw("THEN")?;
            let result = self.parse_expr()?;
            whens.push((cond, result));
        }
        if whens.is_empty() {
            return Err(self.error_here("WHEN"));
        }
        let else_result = if self.eat_kw("ELSE") { Some(Box::new(self.parse_expr()?)) } else { None };
        self.expect_kw("END")?;
        Ok(Expr::Case { operand, whens, else_result })
    }

    fn parse_cast(&mut self) -> DbResult<Expr> {
        self.expect_kw("CAST")?;
        self.expect_symbol("(")?;
        let expr = self.parse_expr()?;
        self.expect_kw("AS")?;
        let ty = self.parse_sql_type()?;
        self.expect_symbol(")")?;
        Ok(Expr::Cast { expr: Box::new(expr), ty })
    }

    fn parse_type_args(&mut self) -> DbResult<Vec<u64>> {
        let mut args = Vec::new();
        if self.eat_symbol("(") {
            args.push(self.parse_u64()?);
            while self.eat_symbol(",") {
                args.push(self.parse_u64()?);
            }
            self.expect_symbol(")")?;
        }
        Ok(args)
    }

    fn parse_sql_type(&mut self) -> DbResult<SqlType> {
        let word = match &self.peek().kind {
            TokenKind::Ident(s) => s.to_ascii_uppercase(),
            _ => return Err(self.error_here("type name")),
        };
        self.advance();
        let bad = |w: &str| DbError::Syntax(format!("invalid type modifier for {}", w));
        let ty = match word.as_str() {
            "INTEGER" | "INT" => SqlType::Integer,
            "SMALLINT" => SqlType::SmallInt,
            "BIGINT" => SqlType::BigInt,
            "DECIMAL" | "NUMERIC" | "DEC" => {
                let args = self.parse_type_args()?;
                let (p, s) = match args.as_slice() {
                    [] => (18, 0),
                    [p] => (*p, 0),
                    [p, s] => (*p, *s),
                    _ => return Err(bad(&word)),
                };
                if p == 0 || p > 38 || s > p {
                    return Err(bad(&word));
                }
                return Ok(SqlType::Decimal { precision: p as u8, scale: s as u8 });
            }
            "DOUBLE" => {
                self.eat_kw("PRECISION");
                SqlType::Double
            }
            "REAL" | "FLOAT" => SqlType::Double,
            "TEXT" => SqlType::Text,
            "CHAR" | "CHARACTER" => {
                if self.eat_kw("VARYING") {
                    return self.parse_varchar_len(&word);
                }
                let args = self.parse_type_args()?;
                match args.as_slice() {
                    [] => SqlType::Char(1),
                    [n] if *n >= 1 => SqlType::Char(*n as usize),
                    _ => return Err(bad(&word)),
                }
            }
            "VARCHAR" => return self.parse_varchar_len(&word),
            "BOOLEAN" | "BOOL" => SqlType::Boolean,
            "DATE" => SqlType::Date,
            "TIME" => SqlType::Time,
            "TIMESTAMP" => SqlType::Timestamp,
            "DATETIME" => SqlType::DateTime,
            "YEAR" => SqlType::Year,
            _ => return Err(DbError::Syntax(format!("unknown type {}", word))),
        };
        Ok(ty)
    }

    fn parse_varchar_len(&mut self, word: &str) -> DbResult<SqlType> {
        let args = self.parse_type_args()?;
        match args.as_slice() {
            [] => Ok(SqlType::Text),
            [n] if *n >= 1 => Ok(SqlType::Varchar(*n as usize)),
            _ => Err(DbError::Syntax(format!("invalid type modifier for {}", word))),
        }
    }

    fn parse_function_call(&mut self) -> DbResult<Expr> {
        let raw = match self.advance().kind {
            TokenKind::Ident(s) => s,
            _ => unreachable!("caller checked for an identifier"),
        };
        let name = raw.to_ascii_uppercase();
        self.expect_symbol("(")?;
        let agg = match name.as_str() {
            "COUNT" => Some(AggFunc::Count),
            "SUM" => Some(AggFunc::Sum),
            "AVG" => Some(AggFunc::Avg),
            "MIN" => Some(AggFunc::Min),
            "MAX" => Some(AggFunc::Max),
            _ => None,
        };
        let mut distinct = false;
        let mut star = false;
        let mut args = Vec::new();
        if let Some(func) = agg {
            if func == AggFunc::Count && self.is_symbol("*") {
                self.advance();
                star = true;
            } else {
                if self.eat_kw("DISTINCT") {
                    distinct = true;
                } else {
                    self.eat_kw("ALL");
                }
                args.push(self.parse_expr()?);
                if self.is_symbol(",") {
                    return Err(DbError::UndefinedFunction(format!("{} with more than one argument", name)));
                }
            }
        } else if !self.is_symbol(")") {
            if self.is_kw("DISTINCT") {
                return Err(DbError::Syntax(format!("DISTINCT is not allowed in {}()", name)));
            }
            args.push(self.parse_expr()?);
            while self.eat_symbol(",") {
                args.push(self.parse_expr()?);
            }
        }
        self.expect_symbol(")")?;
        if self.is_kw("OVER") {
            return Err(unsupported("window functions (OVER)"));
        }
        if self.is_kw("FILTER") || self.is_kw("WITHIN") {
            return Err(unsupported("aggregate FILTER / WITHIN GROUP"));
        }
        if let Some(func) = agg {
            if star {
                return Ok(Expr::Aggregate { func, arg: None, distinct: false });
            }
            let arg = args.pop().expect("aggregate argument parsed above");
            if arg.contains_aggregate() {
                return Err(DbError::Syntax(format!(
                    "aggregate function calls cannot be nested ({})",
                    name
                )));
            }
            return Ok(Expr::Aggregate { func, arg: Some(Box::new(arg)), distinct });
        }
        if !KNOWN_FUNCTIONS.contains(&name.as_str()) {
            return Err(DbError::UndefinedFunction(name));
        }
        check_function_arity(&name, args.len())?;
        Ok(Expr::Function { name, args })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(sql: &str) {
        let e = parse_expression_sql(sql).unwrap();
        let again = parse_expression_sql(&e.to_sql()).unwrap();
        assert_eq!(e, again, "{} -> {}", sql, e.to_sql());
    }

    #[test]
    fn expression_text_round_trips() {
        for sql in [
            "1 + 2 * 3",
            "'it''s' || \"Col\"",
            "a.b IS NOT NULL AND NOT c",
            "x NOT IN (1, 2.50, NULL)",
            "v BETWEEN -1 AND 1e3",
            "s NOT LIKE 'a!%' ESCAPE '!'",
            "CASE WHEN a = 1 THEN 'x' ELSE 'y' END",
            "CAST(a AS DECIMAL(10,2))",
            "COALESCE(a, CURRENT_TIMESTAMP, GETDATE())",
            "COUNT(DISTINCT a) + COUNT(*)",
            "DATE '2024-02-29'",
            "a IN (SELECT b FROM t WHERE c = 1)",
            "EXISTS (SELECT 1 UNION ALL SELECT 2)",
        ] {
            roundtrip(sql);
        }
    }

    #[test]
    fn precedence_and_or_not() {
        let e = parse_expression_sql("a OR b AND NOT c").unwrap();
        assert_eq!(e.to_sql(), "(a OR (b AND (NOT c)))");
        let e = parse_expression_sql("1 + 2 * 3 - 4").unwrap();
        assert_eq!(e.to_sql(), "((1 + (2 * 3)) - 4)");
        let e = parse_expression_sql("a | b & c + 1 = 3").unwrap();
        assert_eq!(e.to_sql(), "((a | (b & (c + 1))) = 3)");
    }

    #[test]
    fn literals_by_kind() {
        assert_eq!(parse_expression_sql("12").unwrap(), Expr::Literal(Value::Int(12)));
        assert!(matches!(parse_expression_sql("1.5").unwrap(), Expr::Literal(Value::Decimal(_))));
        assert!(matches!(parse_expression_sql("1e3").unwrap(), Expr::Literal(Value::Double(_))));
        assert!(parse_expression_sql("DATE '2024-02-30'").is_err());
    }
}
