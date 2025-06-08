use crate::storage::pager::Pager;
use crate::storage::row::{ColumnValue, RowData};
use crate::catalog::TableInfo;
use super::{ConstraintError, ConstraintValidator};

#[derive(Debug, Clone, PartialEq)]
pub struct NotNullConstraint {
    pub table_name: String,
    pub column_index: usize,
}

impl NotNullConstraint {
    fn column_name<'a>(&self, table_info: &'a TableInfo) -> &'a str {
        &table_info.columns[self.column_index].0
    }
}

impl ConstraintValidator for NotNullConstraint {
    fn validate_insert(&self, row: &RowData, table_info: &TableInfo, _pager: &mut Pager) -> Result<(), ConstraintError> {
        if matches!(row.0.get(self.column_index), Some(ColumnValue::Null)) {
            return Err(ConstraintError::NotNullViolation { table: self.table_name.clone(), column: self.column_name(table_info).to_string() });
        }
        Ok(())
    }
}
