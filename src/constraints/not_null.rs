use crate::catalog::TableInfo;
use crate::storage::row::{RowData, ColumnValue};
use crate::catalog::Catalog;
use crate::error::{DbError, DbResult};
use super::Constraint;

pub struct NotNullConstraint;

impl Constraint for NotNullConstraint {
    fn validate_insert(&self, _catalog: &mut Catalog, table: &TableInfo, row: &mut RowData) -> DbResult<()> {
        for ((val, nn), (name, _)) in row.0.iter().zip(table.not_null.iter()).zip(table.columns.iter()) {
            if *nn && matches!(val, ColumnValue::Null) {
                return Err(DbError::NullViolation(name.clone()));
            }
        }
        Ok(())
    }
}
