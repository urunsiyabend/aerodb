use std::io;
use thiserror::Error;

use crate::sql::value::ValueError;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("no such table: {0}")]
    TableNotFound(String),
    #[error("column '{0}' not found")]
    ColumnNotFound(String),
    #[error("duplicate primary key {0}")]
    DuplicateKey(String),
    #[error("null value in column '{0}' violates not-null constraint")]
    NullViolation(String),
    #[error("value out of range")]
    Overflow,
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("invalid value: {0}")]
    InvalidValue(String),
    #[error("foreign key violation: {0}")]
    ForeignKeyViolation(String),
    #[error("column '{0}' must appear in the GROUP BY clause or be used in an aggregate function")]
    GroupByMismatch(String),
    #[error("{0} not found")]
    NotFound(String),
    #[error("write conflict on logical key {0}")]
    WriteConflict(i32),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("syntax error: {0}")]
    Syntax(String),
    #[error("feature not supported: {0}")]
    FeatureNotSupported(String),
    #[error("function {0} does not exist")]
    UndefinedFunction(String),
    #[error("division by zero")]
    DivisionByZero,
    #[error("numeric value out of range: {0}")]
    NumericOverflow(String),
    #[error("value too long: {0}")]
    StringTooLong(String),
    #[error("invalid value for cast: {0}")]
    InvalidCast(String),
    #[error("type mismatch: {0}")]
    TypeMismatch(String),
    #[error("cardinality violation: {0}")]
    CardinalityViolation(String),
    #[error("column reference '{0}' is ambiguous")]
    AmbiguousColumn(String),
    #[error("invalid foreign key: {0}")]
    InvalidForeignKey(String),
    #[error("duplicate key value violates unique constraint {constraint}: {key}")]
    UniqueViolation { constraint: String, key: String },
    #[error("new row violates check constraint {constraint}")]
    CheckViolation { constraint: String },
    #[error("current transaction is aborted, commands ignored until end of transaction block")]
    InFailedTransaction,
    #[error("cannot drop {0} because other objects depend on it")]
    DependentObjects(String),
    #[error("relation \"{0}\" already exists")]
    DuplicateObject(String),
    #[error("column \"{0}\" already exists")]
    DuplicateColumn(String),
    /// A DROP TABLE named a view or a DROP VIEW named a table; the message
    /// is e.g. "v is not a table".
    #[error("{0}")]
    WrongObjectType(String),
    /// A constraint name given twice for one table.
    #[error("constraint \"{name}\" for relation \"{table}\" already exists")]
    DuplicateConstraint { name: String, table: String },
    /// A row image whose serialized form cannot fit in one B-tree leaf page.
    #[error("row is too large for a page: {0}")]
    RowTooLarge(String),
    /// A transaction handle whose transaction already ended (committed or
    /// aborted) or never existed, e.g. a `Database::commit` repeated on a
    /// copied `TxHandle`.
    #[error("no active transaction: {0}")]
    NoActiveTransaction(String),
}

impl DbError {
    /// The five-character SQLSTATE code classifying this error. Every
    /// variant has exactly one code:
    ///
    /// | SQLSTATE | condition | variants |
    /// |---|---|---|
    /// | 0A000 | feature not supported (incl. writes to information_schema) | `FeatureNotSupported` |
    /// | 21000 | cardinality violation | `CardinalityViolation` |
    /// | 22001 | string data, right truncation | `StringTooLong` |
    /// | 22003 | numeric value out of range | `Overflow`, `NumericOverflow` |
    /// | 22012 | division by zero | `DivisionByZero` |
    /// | 22018 | invalid character value for cast | `InvalidCast` |
    /// | 22023 | invalid parameter value | `InvalidValue` |
    /// | 23502 | not-null violation | `NullViolation` |
    /// | 23503 | foreign key violation (statement end, RESTRICT, or COMMIT recheck) | `ForeignKeyViolation` |
    /// | 23505 | unique violation (PRIMARY KEY or UNIQUE) | `DuplicateKey`, `UniqueViolation` |
    /// | 23514 | check violation | `CheckViolation` |
    /// | 25P01 | no active SQL transaction | `NoActiveTransaction` |
    /// | 25P02 | in failed SQL transaction | `InFailedTransaction` |
    /// | 2BP01 | dependent objects still exist | `DependentObjects` |
    /// | 40001 | serialization failure (write/write conflict) | `WriteConflict` |
    /// | 42601 | syntax error | `Syntax`, `ParseError` |
    /// | 42701 | duplicate column | `DuplicateColumn` |
    /// | 42702 | ambiguous column | `AmbiguousColumn` |
    /// | 42703 | undefined column | `ColumnNotFound` |
    /// | 42704 | undefined object (sequence, index, ...) | `NotFound` |
    /// | 42710 | duplicate object (constraint name) | `DuplicateConstraint` |
    /// | 42803 | grouping error | `GroupByMismatch` |
    /// | 42804 | datatype mismatch | `TypeMismatch` |
    /// | 42809 | wrong object type | `WrongObjectType` |
    /// | 42830 | invalid foreign key | `InvalidForeignKey` |
    /// | 42883 | undefined function | `UndefinedFunction` |
    /// | 42P01 | undefined table (incl. unknown schema qualifier) | `TableNotFound` |
    /// | 42P07 | duplicate table | `DuplicateObject` |
    /// | 54000 | program limit exceeded (row too large for a page) | `RowTooLarge` |
    /// | 58030 | I/O error | `Io` |
    pub fn sqlstate(&self) -> &'static str {
        match self {
            DbError::TableNotFound(_) => "42P01",
            DbError::ColumnNotFound(_) => "42703",
            DbError::DuplicateKey(_) => "23505",
            DbError::NullViolation(_) => "23502",
            DbError::Overflow => "22003",
            DbError::ParseError(_) => "42601",
            DbError::InvalidValue(_) => "22023",
            DbError::ForeignKeyViolation(_) => "23503",
            DbError::GroupByMismatch(_) => "42803",
            DbError::NotFound(_) => "42704",
            DbError::WriteConflict(_) => "40001",
            DbError::Io(_) => "58030",
            DbError::Syntax(_) => "42601",
            DbError::FeatureNotSupported(_) => "0A000",
            DbError::UndefinedFunction(_) => "42883",
            DbError::DivisionByZero => "22012",
            DbError::NumericOverflow(_) => "22003",
            DbError::StringTooLong(_) => "22001",
            DbError::InvalidCast(_) => "22018",
            DbError::TypeMismatch(_) => "42804",
            DbError::CardinalityViolation(_) => "21000",
            DbError::AmbiguousColumn(_) => "42702",
            DbError::InvalidForeignKey(_) => "42830",
            DbError::UniqueViolation { .. } => "23505",
            DbError::CheckViolation { .. } => "23514",
            DbError::InFailedTransaction => "25P02",
            DbError::DependentObjects(_) => "2BP01",
            DbError::DuplicateObject(_) => "42P07",
            DbError::DuplicateColumn(_) => "42701",
            DbError::WrongObjectType(_) => "42809",
            DbError::DuplicateConstraint { .. } => "42710",
            DbError::RowTooLarge(_) => "54000",
            DbError::NoActiveTransaction(_) => "25P01",
        }
    }
}

impl From<ValueError> for DbError {
    fn from(e: ValueError) -> Self {
        match e {
            ValueError::DivisionByZero => DbError::DivisionByZero,
            ValueError::NumericOverflow => DbError::NumericOverflow("numeric value out of range".into()),
            ValueError::InvalidCast(m) => DbError::InvalidCast(m),
            ValueError::StringTooLong(m) => DbError::StringTooLong(m),
            ValueError::TypeMismatch(m) => DbError::TypeMismatch(m),
        }
    }
}

pub type DbResult<T> = Result<T, DbError>;

#[cfg(test)]
mod tests {
    use super::*;

    /// No wildcard arm: a new variant without a row in the table below does
    /// not compile.
    fn covered(e: &DbError) {
        match e {
            DbError::TableNotFound(_)
            | DbError::ColumnNotFound(_)
            | DbError::DuplicateKey(_)
            | DbError::NullViolation(_)
            | DbError::Overflow
            | DbError::ParseError(_)
            | DbError::InvalidValue(_)
            | DbError::ForeignKeyViolation(_)
            | DbError::GroupByMismatch(_)
            | DbError::NotFound(_)
            | DbError::WriteConflict(_)
            | DbError::Io(_)
            | DbError::Syntax(_)
            | DbError::FeatureNotSupported(_)
            | DbError::UndefinedFunction(_)
            | DbError::DivisionByZero
            | DbError::NumericOverflow(_)
            | DbError::StringTooLong(_)
            | DbError::InvalidCast(_)
            | DbError::TypeMismatch(_)
            | DbError::CardinalityViolation(_)
            | DbError::AmbiguousColumn(_)
            | DbError::InvalidForeignKey(_)
            | DbError::UniqueViolation { .. }
            | DbError::CheckViolation { .. }
            | DbError::InFailedTransaction
            | DbError::DependentObjects(_)
            | DbError::DuplicateObject(_)
            | DbError::DuplicateColumn(_)
            | DbError::WrongObjectType(_)
            | DbError::DuplicateConstraint { .. }
            | DbError::RowTooLarge(_)
            | DbError::NoActiveTransaction(_) => {}
        }
    }

    /// 0.5.1 wording kept for message compatibility (the audit oracle and
    /// existing clients match "no such table"); SQLSTATE stays 42P01.
    #[test]
    fn table_not_found_message_says_no_such_table() {
        let e = DbError::TableNotFound("t".into());
        let msg = e.to_string();
        assert!(msg.to_lowercase().contains("no such table"), "{msg}");
        assert!(msg.contains('t'), "{msg}");
        assert_eq!(e.sqlstate(), "42P01");
    }

    #[test]
    fn every_variant_has_its_sqlstate() {
        let s = || String::from("x");
        let table: Vec<(DbError, &str)> = vec![
            (DbError::TableNotFound(s()), "42P01"),
            (DbError::ColumnNotFound(s()), "42703"),
            (DbError::DuplicateKey(s()), "23505"),
            (DbError::NullViolation(s()), "23502"),
            (DbError::Overflow, "22003"),
            (DbError::ParseError(s()), "42601"),
            (DbError::InvalidValue(s()), "22023"),
            (DbError::ForeignKeyViolation(s()), "23503"),
            (DbError::GroupByMismatch(s()), "42803"),
            (DbError::NotFound(s()), "42704"),
            (DbError::WriteConflict(1), "40001"),
            (DbError::Io(io::Error::other("x")), "58030"),
            (DbError::Syntax(s()), "42601"),
            (DbError::FeatureNotSupported(s()), "0A000"),
            (DbError::UndefinedFunction(s()), "42883"),
            (DbError::DivisionByZero, "22012"),
            (DbError::NumericOverflow(s()), "22003"),
            (DbError::StringTooLong(s()), "22001"),
            (DbError::InvalidCast(s()), "22018"),
            (DbError::TypeMismatch(s()), "42804"),
            (DbError::CardinalityViolation(s()), "21000"),
            (DbError::AmbiguousColumn(s()), "42702"),
            (DbError::InvalidForeignKey(s()), "42830"),
            (DbError::UniqueViolation { constraint: s(), key: s() }, "23505"),
            (DbError::CheckViolation { constraint: s() }, "23514"),
            (DbError::InFailedTransaction, "25P02"),
            (DbError::DependentObjects(s()), "2BP01"),
            (DbError::DuplicateObject(s()), "42P07"),
            (DbError::DuplicateColumn(s()), "42701"),
            (DbError::WrongObjectType(s()), "42809"),
            (DbError::DuplicateConstraint { name: s(), table: s() }, "42710"),
            (DbError::RowTooLarge(s()), "54000"),
            (DbError::NoActiveTransaction(s()), "25P01"),
        ];
        // One row per variant: the count is derived from the enum definition
        // itself, so a variant missing from the table fails here.
        let src = include_str!("error.rs");
        let body = &src[src.find("pub enum DbError {").unwrap()..];
        let body = &body[..body.find("\n}\n").unwrap()];
        let variants = body
            .lines()
            .skip(1)
            .filter(|l| l.trim_start().starts_with(|c: char| c.is_ascii_uppercase()))
            .count();
        assert_eq!(table.len(), variants, "table rows vs DbError variants");
        let mut seen = std::collections::HashSet::new();
        for (e, code) in &table {
            covered(e);
            assert!(seen.insert(std::mem::discriminant(e)), "variant listed twice: {e:?}");
            assert_eq!(e.sqlstate(), *code, "{e:?}");
            assert_eq!(code.len(), 5);
            assert!(code.chars().all(|c| c.is_ascii_digit() || c.is_ascii_uppercase()), "{code}");
        }
    }
}
