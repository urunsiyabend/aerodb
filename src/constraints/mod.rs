pub mod not_null;
pub mod default;
pub mod foreign_key;

use crate::catalog::{Catalog, TableInfo};
use crate::storage::row::RowData;
use std::io;

pub trait Constraint {
    fn validate_insert(&self, catalog: &mut Catalog, table: &TableInfo, row: &mut RowData) -> io::Result<()>;
    fn validate_delete(&self, _catalog: &mut Catalog, _table: &TableInfo, _row: &RowData) -> io::Result<()> {
        Ok(())
    }
}
