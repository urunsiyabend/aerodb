use thiserror::Error;
use std::io;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("table '{0}' not found")]
    TableNotFound(String),
    #[error("column '{0}' not found")]
    ColumnNotFound(String),
    #[error("duplicate primary key {0}")]
    DuplicateKey(i32),
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
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub type DbResult<T> = Result<T, DbError>;
