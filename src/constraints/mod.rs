pub mod default;
pub mod foreign_key;
pub mod not_null;
pub mod primary_key;

use crate::catalog::{Catalog, TableInfo};
use crate::error::DbResult;
use crate::storage::row::RowData;
use crate::transaction::Snapshot;

pub trait Constraint {
    fn validate_insert(
        &self,
        catalog: &mut Catalog,
        table: &TableInfo,
        row: &mut RowData,
        snapshot: &Snapshot,
    ) -> DbResult<()>;
    fn validate_delete(
        &self,
        _catalog: &mut Catalog,
        _table: &TableInfo,
        _row: &RowData,
        _snapshot: &Snapshot,
    ) -> DbResult<()> {
        Ok(())
    }
}
