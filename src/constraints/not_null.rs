use super::Constraint;
use crate::catalog::Catalog;
use crate::catalog::TableInfo;
use crate::error::{DbError, DbResult};
use crate::storage::row::{ColumnValue, RowData};
use crate::transaction::Snapshot;

pub struct NotNullConstraint;

impl Constraint for NotNullConstraint {
    fn validate_insert(
        &self,
        _catalog: &mut Catalog,
        table: &TableInfo,
        row: &mut RowData,
        _snapshot: &Snapshot,
    ) -> DbResult<()> {
        for ((val, nn), (name, _)) in row
            .0
            .iter()
            .zip(table.not_null.iter())
            .zip(table.columns.iter())
        {
            if *nn && matches!(val, ColumnValue::Null) {
                return Err(DbError::NullViolation(name.clone()));
            }
        }
        Ok(())
    }
}
