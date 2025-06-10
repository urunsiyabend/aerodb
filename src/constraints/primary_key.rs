use std::io;
use crate::catalog::{Catalog, TableInfo};
use crate::storage::row::{RowData, ColumnValue};
use crate::storage::btree::BTree;
use super::Constraint;

pub struct PrimaryKeyConstraint<'a> {
    pub columns: &'a [String],
}

impl<'a> Constraint for PrimaryKeyConstraint<'a> {
    fn validate_insert(&self, catalog: &mut Catalog, table: &TableInfo, row: &mut RowData) -> io::Result<()> {
        // ensure not null
        for col in self.columns {
            if let Some(idx) = table.columns.iter().position(|(c, _)| c == col) {
                if matches!(row.0[idx], ColumnValue::Null) {
                    return Err(io::Error::new(io::ErrorKind::Other, "PRIMARY KEY column cannot be NULL"));
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
                return Err(io::Error::new(io::ErrorKind::Other, "duplicate primary key"));
            }
        }
        Ok(())
    }
}
