use crate::catalog::{Catalog, TableInfo};
use crate::storage::row::{RowData, ColumnValue};
use crate::storage::btree::BTree;
use crate::error::{DbError, DbResult};
use super::Constraint;

pub struct PrimaryKeyConstraint<'a> {
    pub columns: &'a [String],
}

impl<'a> Constraint for PrimaryKeyConstraint<'a> {
    fn validate_insert(&self, catalog: &mut Catalog, table: &TableInfo, row: &mut RowData) -> DbResult<()> {
        // ensure not null
        for col in self.columns {
            if let Some(idx) = table.columns.iter().position(|(c, _)| c == col) {
                if matches!(row.0[idx], ColumnValue::Null) {
                    return Err(DbError::NullViolation(col.clone()));
                }
            }
        }
        // check uniqueness
        let mut btree = BTree::open_root(&mut catalog.pager, table.root_page)?;
        let mut cursor = btree.scan_all_rows();
        while let Some(existing) = cursor.next() {
            let mut equal = true;
            for col in self.columns {
                if let Some(idx) = table.columns.iter().position(|(c, _)| c == col) {
                    if existing.data.0[idx] != row.0[idx] {
                        equal = false;
                        break;
                    }
                }
            }
            if equal {
                if let Some(ColumnValue::Integer(i)) = row.0.get(0) {
                    return Err(DbError::DuplicateKey(*i));
                }
                return Err(DbError::DuplicateKey(0));
            }
        }
        Ok(())
    }
}
