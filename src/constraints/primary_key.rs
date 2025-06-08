use crate::storage::btree::BTree;
use crate::storage::pager::Pager;
use crate::storage::row::{ColumnValue, RowData};
use crate::catalog::TableInfo;
use super::{ConstraintError, ConstraintValidator};

#[derive(Debug, Clone, PartialEq)]
pub struct PrimaryKeyConstraint {
    pub table_name: String,
    pub column_index: usize,
}

impl PrimaryKeyConstraint {
    fn column_name<'a>(&self, table_info: &'a TableInfo) -> &'a str {
        &table_info.columns[self.column_index].0
    }
}

impl ConstraintValidator for PrimaryKeyConstraint {
    fn validate_insert(&self, row: &RowData, table_info: &TableInfo, pager: &mut Pager) -> Result<(), ConstraintError> {
        if matches!(row.0.get(self.column_index), Some(ColumnValue::Null)) {
            return Err(ConstraintError::NotNullViolation { table: self.table_name.clone(), column: self.column_name(table_info).to_string() });
        }
        if let Some(ColumnValue::Integer(key)) = row.0.get(self.column_index) {
            let mut tree = BTree::open_root(pager, table_info.root_page).map_err(|_| ConstraintError::PrimaryKeyViolation { table: self.table_name.clone(), columns: vec![self.column_name(table_info).to_string()] })?;
            if tree.find(*key).map_err(|_| ConstraintError::PrimaryKeyViolation { table: self.table_name.clone(), columns: vec![self.column_name(table_info).to_string()] })?.is_some() {
                return Err(ConstraintError::UniqueViolation { table: self.table_name.clone(), columns: vec![self.column_name(table_info).to_string()] });
            }
        }
        Ok(())
    }
}
