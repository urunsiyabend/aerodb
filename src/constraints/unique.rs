use crate::catalog::{Catalog, TableInfo};
use crate::storage::row::{RowData, ColumnValue};
use crate::storage::btree::BTree;
use crate::error::{DbError, DbResult};
use super::Constraint;

pub struct UniqueConstraint<'a> {
    pub columns: &'a [String],
}

impl<'a> Constraint for UniqueConstraint<'a> {
    fn validate_insert(&self, catalog: &mut Catalog, table: &TableInfo, row: &mut RowData) -> DbResult<()> {
        // Check uniqueness by scanning all existing rows
        let mut btree = BTree::open_root(&mut catalog.pager, table.root_page)?;
        let mut cursor = btree.scan_all_rows();

        while let Some(existing) = cursor.next() {
            let mut all_match = true;
            let mut has_null = false;

            for col in self.columns {
                if let Some(idx) = table.columns.iter().position(|(c, _)| c == col) {
                    // NULL values don't participate in UNIQUE constraints
                    // (two NULL values are not considered equal for UNIQUE)
                    if matches!(row.0[idx], ColumnValue::Null) {
                        has_null = true;
                        break;
                    }
                    if matches!(existing.data.0[idx], ColumnValue::Null) {
                        has_null = true;
                        break;
                    }

                    if existing.data.0[idx] != row.0[idx] {
                        all_match = false;
                        break;
                    }
                }
            }

            // If all columns match and none are NULL, we have a duplicate
            if all_match && !has_null {
                let column_names = self.columns.join(", ");
                return Err(DbError::UniqueViolation(column_names));
            }
        }

        Ok(())
    }
}
