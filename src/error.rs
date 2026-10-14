use std::io;
use thiserror::Error;

use crate::sql::value::ValueError;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("table '{0}' not found")]
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
}

impl DbError {
    /// The five-character SQLSTATE code classifying this error.
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
