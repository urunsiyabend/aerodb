pub mod primary_key;
pub mod not_null;

use crate::storage::pager::Pager;
use crate::storage::row::RowData;
use crate::catalog::TableInfo;

pub use primary_key::PrimaryKeyConstraint;
pub use not_null::NotNullConstraint;

#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintType {
    PrimaryKey(PrimaryKeyConstraint),
    NotNull(NotNullConstraint),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraints {
    pub table_name: String,
    pub constraints: Vec<ConstraintType>,
}

pub trait ConstraintValidator {
    fn validate_insert(&self, row: &RowData, table_info: &TableInfo, pager: &mut Pager) -> Result<(), ConstraintError>;
    fn validate_update(&self, _old_row: &RowData, _new_row: &RowData, _table_info: &TableInfo, _pager: &mut Pager) -> Result<(), ConstraintError> {
        Ok(())
    }
    fn validate_delete(&self, _row: &RowData, _table_info: &TableInfo, _pager: &mut Pager) -> Result<(), ConstraintError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ConstraintError {
    PrimaryKeyViolation { table: String, columns: Vec<String> },
    NotNullViolation { table: String, column: String },
    UniqueViolation { table: String, columns: Vec<String> },
}
